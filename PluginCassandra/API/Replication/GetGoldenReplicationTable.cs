using Naveego.Sdk.Plugins;
using PluginCassandra.API.Utility;
using PluginCassandra.DataContracts;

namespace PluginCassandra.API.Replication
{
    public static partial class Replication
    {
        public static ReplicationTable GetGoldenReplicationTable(Schema schema, string safeSchemaName, string safeGoldenTableName)
        {
            var goldenTable = ConvertSchemaToReplicationTable(schema, safeSchemaName, safeGoldenTableName);
            goldenTable.Columns.Add(new ReplicationColumn
            {
                ColumnName = Constants.ReplicationRecordId,
                DataType = "varchar",
                PrimaryKey = true
            });
            goldenTable.Columns.Add(new ReplicationColumn
            {
                ColumnName = Constants.ReplicationVersionIds,
                DataType = "text",
                PrimaryKey = false,
                Serialize = true
            });

            return goldenTable;
        }
    }
}