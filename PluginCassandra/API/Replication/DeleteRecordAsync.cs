using System.Threading.Tasks;
using PluginCassandra.API.Factory;
using PluginCassandra.DataContracts;

namespace PluginCassandra.API.Replication
{
    public static partial class Replication
    {
        private static readonly string DeleteRecordQuery = @"DELETE FROM {0}.{1}
WHERE {2} = '{3}'";

        public static async Task DeleteRecordAsync(ISessionFactory sessionFactory, ReplicationTable table,
            string primaryKeyValue)
        {
            // var conn = connFactory.GetConnection();
            var session = sessionFactory.GetSession();
            
            // await conn.OpenAsync();
            //
            // var cmd = connFactory.GetCommand(string.Format(DeleteRecordQuery,
            //         Utility.Utility.GetSafeName(table.SchemaName, '`'),
            //         Utility.Utility.GetSafeName(table.TableName, '`'),
            //         Utility.Utility.GetSafeName(table.Columns.Find(c => c.PrimaryKey == true).ColumnName, '`'),
            //         primaryKeyValue
            //     ),
            //     conn);

            await session.Execute(string.Format(DeleteRecordQuery,
                Utility.Utility.GetSafeName(table.SchemaName, '"'),
                Utility.Utility.GetSafeName(table.TableName, '"'),
                Utility.Utility.GetSafeName(table.Columns.Find(c => c.PrimaryKey == true).ColumnName, '"')));

            // check if table exists
            // await cmd.ExecuteNonQueryAsync();
            
        }
    }
}