using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Naveego.Sdk.Logging;
using PluginCassandra.API.Factory;
using PluginCassandra.DataContracts;
using PluginCassandra.Helper;

namespace PluginCassandra.API.Replication
{
    public static partial class Replication
    {
        private static readonly string EnsureTableQuery = @"SELECT COUNT(*)
FROM system_schema.tables 
WHERE keyspace_name = '{0}' 
AND table_name = '{1}'";

        // private static readonly string EnsureTableQuery = @"SELECT * FROM {0}.{1}";

        public static async Task EnsureTableAsync(ISessionFactory sessionFactory, ReplicationTable table)
        {
            var session = sessionFactory.GetSession();
           // var conn = connFactory.GetConnection();

            try
            {
                //await conn.OpenAsync();

                Logger.Info($"Creating Schema... {table.SchemaName}");

                await session.Execute($"CREATE KEYSPACE IF NOT EXISTS {table.SchemaName} WITH REPLICATION = {{'class' : 'SimpleStrategy', 'replication_factor' : 1}};");
                
                var rows = await session.Execute(string.Format(EnsureTableQuery, table.SchemaName, table.TableName));
                
                
                Logger.Info($"Creating Table: {string.Format(EnsureTableQuery, table.SchemaName, table.TableName)}");

                // check if table exists
                // var reader = await cmd.ExecuteReaderAsync();
                //await reader.ReadAsync();
                var count = (long) rows.First()["count"];
                // await conn.CloseAsync();

                if (count == 0)
                {
                    // create table
                    var querySb = new StringBuilder($@"CREATE TABLE IF NOT EXISTS 
{Utility.Utility.GetSafeName(table.SchemaName, '"')}.{Utility.Utility.GetSafeName(table.TableName, '"')}(");
                    var primaryKeySb = new StringBuilder("(");
                    var hasPrimaryKey = false;
                    foreach (var column in table.Columns)
                    {
                        querySb.Append(
                            $"{Utility.Utility.GetSafeName(column.ColumnName)} {column.DataType}{(column.PrimaryKey ? " PRIMARY KEY" : "")},");
                        if (column.PrimaryKey)
                        {
                            primaryKeySb.Append($"{Utility.Utility.GetSafeName(column.ColumnName)},");
                            hasPrimaryKey = true;
                        }
                    }

                    // if (hasPrimaryKey)
                    // {
                    //     primaryKeySb.Length--;
                    //     primaryKeySb.Append(")");
                    //     querySb.Append($"{primaryKeySb});");
                    // }
                    // else
                    // {
                    querySb.Length--;
                    querySb.Append(");");
                    // }

                    var query = querySb.ToString();
                    Logger.Info($"Creating Table: {query}");

                    await session.Execute(query);
                }
            }
            finally
            {
                //noop
            }
        }
    }
}