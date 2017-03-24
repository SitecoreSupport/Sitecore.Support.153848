using Sitecore.Data;
using Sitecore.Diagnostics;
using Sitecore.EmailCampaign.Cd.EmailEvents;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Web;

namespace Sitecore.Support.EmailCampaign.Cd.EmailEvents
{
    public class WebDataProvider : IEmailEventStorage
    {
        private readonly string connectionString;
        private readonly static object obj = new object();
        public WebDataProvider(string connectionStringName)
        {
            Assert.ArgumentNotNull(connectionStringName, "connectionStringName");
            ConnectionStringSettings settings = ConfigurationManager.ConnectionStrings[connectionStringName];
            if (settings == null)
            {
                throw new ConfigurationException($"No connection string configuration was found by the name '{connectionStringName}'.");
            }
            this.connectionString = settings.ConnectionString;
        }

        private static byte[] CalculateMd5Hash(string input)
        {
            using (MD5 md = MD5.Create())
            {
                byte[] bytes = Encoding.UTF8.GetBytes(input);
                return md.ComputeHash(bytes);
            }
        }

        public DateTime? GetEmailOpenedRegistration(ID messageId, ID instanceId, ID contactId)
        {
            DateTime? nullable;
            Assert.ArgumentNotNull(messageId, "messageId");
            Assert.ArgumentNotNull(instanceId, "instanceId");
            Assert.ArgumentNotNull(contactId, "contactId");
            using (SqlConnection connection = new SqlConnection(this.connectionString))
            {
                connection.Open();
                using (SqlCommand command = connection.CreateCommand())
                {
                    command.CommandText = "SELECT [Timestamp] FROM [EmailOpenEvents] WITH (NOLOCK) WHERE [MessageId]=@MessageId AND [InstanceId]=@InstanceId AND [ContactId]=@ContactId";
                    command.Parameters.Add("@MessageId", SqlDbType.UniqueIdentifier).Value = messageId.Guid;
                    command.Parameters.Add("@InstanceId", SqlDbType.UniqueIdentifier).Value = instanceId.Guid;
                    command.Parameters.Add("@ContactId", SqlDbType.UniqueIdentifier).Value = contactId.Guid;
                    nullable = command.ExecuteScalar() as DateTime?;
                }
            }
            return nullable;
        }

        public DateTime? GetLinkClickedRegistration(ID messageId, ID instanceId, ID contactId, string link)
        {
            DateTime? nullable;
            Assert.ArgumentNotNull(messageId, "messageId");
            Assert.ArgumentNotNull(instanceId, "instanceId");
            Assert.ArgumentNotNull(contactId, "contactId");
            Assert.ArgumentNotNull(link, "link");
            using (SqlConnection connection = new SqlConnection(this.connectionString))
            {
                connection.Open();
                using (SqlCommand command = connection.CreateCommand())
                {
                    command.CommandText = "SELECT [Timestamp] FROM [LinkClickEvents] WITH (NOLOCK) WHERE [MessageId]=@MessageId AND [InstanceId]=@InstanceId AND [ContactId]=@ContactId AND [LinkHash]=@LinkHash";
                    byte[] buffer = CalculateMd5Hash(link);
                    command.Parameters.Add("@MessageId", SqlDbType.UniqueIdentifier).Value = messageId.Guid;
                    command.Parameters.Add("@InstanceId", SqlDbType.UniqueIdentifier).Value = instanceId.Guid;
                    command.Parameters.Add("@ContactId", SqlDbType.UniqueIdentifier).Value = contactId.Guid;
                    command.Parameters.Add("@LinkHash", SqlDbType.Binary).Value = buffer;
                    nullable = command.ExecuteScalar() as DateTime?;
                }
            }
            return nullable;
        }

        public RegistrationResult RegisterEmailOpened(ID messageId, ID instanceId, ID contactId, TimeSpan duplicateProtectionInterval)
        {
            Assert.ArgumentNotNull(messageId, "messageId");
            Assert.ArgumentNotNull(instanceId, "instanceId");
            Assert.ArgumentNotNull(contactId, "contactId");
            Assert.ArgumentCondition(duplicateProtectionInterval >= TimeSpan.Zero, "duplicateProtectionInterval", "The duplication protection interval must be positive.");
            using (SqlConnection connection = new SqlConnection(this.connectionString))
            {
                connection.Open();
                using (SqlCommand command = connection.CreateCommand())
                {
                    command.CommandText = "MERGE [EmailOpenEvents] WITH (HOLDLOCK) AS T USING (SELECT @MessageId,@InstanceId,@ContactId,@Timestamp) AS S ([MessageId],[InstanceId],[ContactId],[Timestamp]) ON (T.MessageId=S.MessageId AND T.InstanceId=S.InstanceId AND T.ContactId=S.ContactId) WHEN MATCHED THEN UPDATE SET [Timestamp] = CASE WHEN T.[Timestamp] < @DuplicateProtectionTime THEN S.[Timestamp] ELSE T.[Timestamp] END WHEN NOT MATCHED THEN INSERT ([MessageId],[InstanceId],[ContactId],[Timestamp]) VALUES (S.[MessageId],S.[InstanceId],S.[ContactId],S.[Timestamp]) OUTPUT INSERTED.[Timestamp] AS [Timestamp], CAST(CASE WHEN INSERTED.[Timestamp] = DELETED.[Timestamp] THEN 1 ELSE 0 END AS BIT) AS [IsDuplicate], CAST(CASE WHEN DELETED.[Timestamp] IS NULL THEN 1 ELSE 0 END AS BIT) AS [IsFirstRegistration];";
                    DateTime utcNow = DateTime.UtcNow;
                    command.Parameters.Add("@MessageId", SqlDbType.UniqueIdentifier).Value = messageId.Guid;
                    command.Parameters.Add("@InstanceId", SqlDbType.UniqueIdentifier).Value = instanceId.Guid;
                    command.Parameters.Add("@ContactId", SqlDbType.UniqueIdentifier).Value = contactId.Guid;
                    command.Parameters.Add("@Timestamp", SqlDbType.DateTime).Value = utcNow;
                    command.Parameters.Add("@DuplicateProtectionTime", SqlDbType.DateTime).Value = utcNow - duplicateProtectionInterval;
                    using (SqlDataReader reader = command.ExecuteReader(CommandBehavior.SingleRow))
                    {
                        if (reader.Read())
                        {
                            try
                            {
                                DateTime dateTime = reader.GetDateTime(0);
                                bool boolean = reader.GetBoolean(1);
                                return new RegistrationResult(dateTime, boolean, reader.GetBoolean(2));
                            }
                            catch (InvalidCastException exception)
                            {
                                throw new DataException("The result retrieved from the database when registering an email open event contained an unexpected data type.", exception);
                            }
                        }
                        throw new DataException("An empty response was retrieved from the database when registering an email open event.");
                    }
                }
            }
        }


        public new RegistrationResult RegisterLinkClicked(ID messageId, ID instanceId, ID contactId, string link, TimeSpan duplicateProtectionInterval)
        {
            Assert.ArgumentNotNull(messageId, "messageId");
            Assert.ArgumentNotNull(instanceId, "instanceId");
            Assert.ArgumentNotNull(contactId, "contactId");
            Assert.ArgumentNotNull(link, "link");
            Assert.ArgumentCondition(duplicateProtectionInterval >= TimeSpan.Zero, "duplicateProtectionInterval", "The duplication protection interval must be positive.");
            lock (obj)
            {
                using (SqlConnection connection = new SqlConnection(this.connectionString))
                {
                    connection.Open();
                    using (SqlCommand command = connection.CreateCommand())
                    {
                        command.CommandText = "MERGE [LinkClickEvents] WITH (HOLDLOCK) AS T USING (SELECT @MessageId,@InstanceId,@ContactId,@LinkHash,@Timestamp,(SELECT TOP 1 [MessageId] FROM [LinkClickEvents] WHERE [MessageId] = @MessageId AND [InstanceId] = @InstanceId AND [ContactId] = @ContactId) AS ExistingClickEvents) AS S ([MessageId],[InstanceId],[ContactId],[LinkHash],[Timestamp],[ExistingClickEvents]) ON (T.MessageId=S.MessageId AND T.InstanceId=S.InstanceId AND T.ContactId=S.ContactId AND T.LinkHash=S.LinkHash) WHEN MATCHED THEN UPDATE SET [Timestamp] = CASE WHEN T.[Timestamp] < @DuplicateProtectionTime THEN S.[Timestamp] ELSE T.[Timestamp] END WHEN NOT MATCHED THEN INSERT ([MessageId],[InstanceId],[ContactId],[LinkHash],[Timestamp]) VALUES (S.[MessageId],S.[InstanceId],S.[ContactId],S.[LinkHash],S.[Timestamp]) OUTPUT INSERTED.[Timestamp] AS [Timestamp], CAST(CASE WHEN INSERTED.[Timestamp] = DELETED.[Timestamp] THEN 1 ELSE 0 END AS BIT) AS [IsDuplicate], CAST(CASE WHEN DELETED.[Timestamp] IS NULL AND S.ExistingClickEvents IS NULL THEN 1 ELSE 0 END AS BIT) AS [IsFirstRegistration];";
                        byte[] buffer = CalculateMd5Hash(link);
                        DateTime utcNow = DateTime.UtcNow;
                        command.Parameters.Add("@MessageId", SqlDbType.UniqueIdentifier).Value = messageId.Guid;
                        command.Parameters.Add("@InstanceId", SqlDbType.UniqueIdentifier).Value = instanceId.Guid;
                        command.Parameters.Add("@ContactId", SqlDbType.UniqueIdentifier).Value = contactId.Guid;
                        command.Parameters.Add("@LinkHash", SqlDbType.Binary).Value = buffer;
                        command.Parameters.Add("@Timestamp", SqlDbType.DateTime).Value = utcNow;
                        command.Parameters.Add("@DuplicateProtectionTime", SqlDbType.DateTime).Value = utcNow - duplicateProtectionInterval;
                        using (SqlDataReader reader = command.ExecuteReader(CommandBehavior.SingleRow))
                        {
                            if (reader.Read())
                            {
                                try
                                {
                                    DateTime dateTime = reader.GetDateTime(0);
                                    bool boolean = reader.GetBoolean(1);
                                    return new RegistrationResult(dateTime, boolean, reader.GetBoolean(2));
                                }
                                catch (InvalidCastException exception)
                                {
                                    throw new DataException("The result retrieved from the database when registering a link click event contained an unexpected data type.", exception);
                                }
                            }
                            throw new DataException("An empty response was retrieved from the database when registering a link click event.");
                        }
                    }
                }
            }
        }
    }
}