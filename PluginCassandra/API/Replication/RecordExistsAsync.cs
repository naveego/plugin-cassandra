using System.Linq;
using System.Threading.Tasks;
using PluginCassandra.API.Factory;
using PluginCassandra.DataContracts;
using Session = Cassandra.Session;

namespace PluginCassandra.API.Replication
{
    public static partial class Replication
    {
        private static readonly string RecordExistsQuery = @"SELECT COUNT(*) FROM {0}.{1}
            WHERE {2} = '{3}'
            ALLOW FILTERING";

        public static async Task<bool> RecordExistsAsync(ISessionFactory sessionFactory, ReplicationTable table,
            string primaryKeyValue)
        {
            var session = sessionFactory.GetConnection();
            
            // await conn.OpenAsync();
            //
            // var cmd = connFactory.GetCommand(string.Format(RecordExistsQuery,
            //         Utility.Utility.GetSafeName(table.SchemaName, '`'),
            //         Utility.Utility.GetSafeName(table.TableName, '`'),
            //         Utility.Utility.GetSafeName(table.Columns.Find(c => c.PrimaryKey == true).ColumnName, '`'),
            //         primaryKeyValue
            //     ),
            //     conn);

            // check if record exists
            // var reader = await cmd.ExecuteReaderAsync();
            // await reader.ReadAsync();
            // var count = (long) reader.GetValueById("count");

            var rows = await session.Execute(string.Format(RecordExistsQuery,
                Utility.Utility.GetSafeName(table.SchemaName, '"'),
                Utility.Utility.GetSafeName(table.TableName, '"'),
                Utility.Utility.GetSafeName(table.Columns.Find(c => c.PrimaryKey == true).ColumnName, '"'),
                primaryKeyValue));

            var count = (long) rows.First()["count"];
            
            return count != 0;
        }
    }
}