using System;
using System.Threading.Tasks;
using Naveego.Sdk.Logging;
using Naveego.Sdk.Plugins;
using Newtonsoft.Json;
using PluginCassandra.API.Factory;
using PluginCassandra.DataContracts;
using PluginCassandra.Helper;
using Constants = PluginCassandra.API.Utility.Constants;

namespace PluginCassandra.API.Replication
{
    public static partial class Replication
    {
        private static readonly string GetMetaDataQuery = @"SELECT * FROM {0}.{1} WHERE {2} = '{3}'";

        public static async Task<ReplicationMetaData> GetPreviousReplicationMetaDataAsync(
            ISessionFactory sessionFactory,
            string jobId,
            ReplicationTable table)
        {

            try
            {
                ReplicationMetaData replicationMetaData = null;

                // ensure replication metadata table
                await EnsureTableAsync(sessionFactory, table);

                // check if metadata exists
                var session = sessionFactory.GetSession();

                var rows = await session.Execute(string.Format(GetMetaDataQuery,
                    Utility.Utility.GetSafeName(table.SchemaName, '"'),
                    Utility.Utility.GetSafeName(table.TableName, '"'),
                    Utility.Utility.GetSafeName(Constants.ReplicationMetaDataJobId, '"'),
                    jobId));
                

                foreach (var row in rows)
                {
                    var request = JsonConvert.DeserializeObject<PrepareWriteRequest>(row[Constants.ReplicationMetaDataRequest].ToString());
                    var shapeName = row[Constants.ReplicationMetaDataReplicatedShapeName].ToString();
                    var shapeId = row[Constants.ReplicationMetaDataReplicatedShapeId].ToString();
                    var timestamp = DateTime.Parse(row[Constants.ReplicationMetaDataTimestamp].ToString());
                    
                    replicationMetaData = new ReplicationMetaData
                    {
                        Request = request,
                        ReplicatedShapeName = shapeName,
                        ReplicatedShapeId = shapeId,
                        Timestamp = timestamp
                    };
                }
                return replicationMetaData;
            }
            catch (Exception e)
            {
                Logger.Error(e, e.Message);
                throw;
            }
        }
    }
}