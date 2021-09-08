using System;
using System.Threading.Tasks;
using Naveego.Sdk.Logging;
using Newtonsoft.Json;
using PluginCassandra.API.Factory;
using PluginCassandra.API.Utility;
using PluginCassandra.DataContracts;
using PluginCassandra.Helper;

namespace PluginCassandra.API.Replication
{
    public static partial class Replication
    {
        private static readonly string InsertMetaDataQuery = $@"INSERT INTO {{0}}.{{1}} 
(
{Utility.Utility.GetSafeName(Constants.ReplicationMetaDataJobId)}
, {Utility.Utility.GetSafeName(Constants.ReplicationMetaDataRequest)}
, {Utility.Utility.GetSafeName(Constants.ReplicationMetaDataReplicatedShapeId)}
, {Utility.Utility.GetSafeName(Constants.ReplicationMetaDataReplicatedShapeName)}
, {Utility.Utility.GetSafeName(Constants.ReplicationMetaDataTimestamp)})
VALUES (
'{{2}}'
, '{{3}}'
, '{{4}}'
, '{{5}}'
, '{{6}}'
)";

        private static readonly string UpdateMetaDataQuery = $@"UPDATE {{0}}.{{1}}
SET 
{Utility.Utility.GetSafeName(Constants.ReplicationMetaDataRequest)} = '{{2}}'
, {Utility.Utility.GetSafeName(Constants.ReplicationMetaDataReplicatedShapeId)} = '{{3}}'
, {Utility.Utility.GetSafeName(Constants.ReplicationMetaDataReplicatedShapeName)} = '{{4}}'
, {Utility.Utility.GetSafeName(Constants.ReplicationMetaDataTimestamp)} = '{{5}}'
WHERE {Utility.Utility.GetSafeName(Constants.ReplicationMetaDataJobId)} = '{{6}}'";

        public static async Task UpsertReplicationMetaDataAsync(ISessionFactory sessionFactory, ReplicationTable table,
            ReplicationMetaData metaData)
        {
            var session = sessionFactory.GetSession();
            try
            {
                await session.Execute(string.Format(InsertMetaDataQuery,
                    Utility.Utility.GetSafeName(table.SchemaName, '"'),
                    Utility.Utility.GetSafeName(table.TableName, '"'),
                    metaData.Request.DataVersions.JobId,
                    // JsonConvert.SerializeObject(metaData.Request).Replace("\\", "\\\\"),
                    JsonConvert.SerializeObject(metaData.Request),
                    metaData.ReplicatedShapeId,
                    metaData.ReplicatedShapeName,
                    metaData.Timestamp
                ));
            }
            catch (Exception e)
            {
                try
                {

                    session.Execute(string.Format(UpdateMetaDataQuery,
                        Utility.Utility.GetSafeName(table.SchemaName, '"'),
                        Utility.Utility.GetSafeName(table.TableName, '"'),
                        JsonConvert.SerializeObject(metaData.Request),
                        metaData.ReplicatedShapeId,
                        metaData.ReplicatedShapeName,
                        metaData.Timestamp,
                        metaData.Request.DataVersions.JobId
                    ));
                    // update if it failed
                    

                }
                catch (Exception exception)
                {
                    Logger.Error(e, $"Error Insert: {e.Message}");
                    Logger.Error(exception, $"Error Update: {exception.Message}");
                    throw;
                }
                
            }
            
        }
    }
}