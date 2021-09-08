using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Naveego.Sdk.Logging;
using PluginCassandra.API.Factory;
using PluginCassandra.DataContracts;
using PluginCassandra.Helper;

namespace PluginCassandra.API.Replication
{
    public static partial class Replication
    {
        private static readonly string GetRecordQuery = @"SELECT * FROM {0}.{1}
WHERE {2} = '{3}'";

        public static async Task<Dictionary<string, object>> GetRecordAsync(ISessionFactory sessionFactory,
            ReplicationTable table,
            string primaryKeyValue)
        {
            var session = sessionFactory.GetSession();

            var rows = await session.Execute(string.Format(GetRecordQuery,
                Utility.Utility.GetSafeName(table.SchemaName, '"'),
                Utility.Utility.GetSafeName(table.TableName, '"'),
                Utility.Utility.GetSafeName(table.Columns.Find(c => c.PrimaryKey == true).ColumnName, '"'),
                primaryKeyValue));

            Dictionary<string, object> recordMap = null;
            // check if record exists

            foreach (var row in rows)
            {
                recordMap = new Dictionary<string, object>();
                
                foreach (var column in table.Columns)
                {
                    try
                    {
                        recordMap[column.ColumnName] = row[column.ColumnName];
                    }
                    catch (Exception e)
                    {
                        Logger.Error(e, $"No column with column name: {column.ColumnName}");
                        Logger.Error(e, e.Message);
                        recordMap[column.ColumnName] = null;
                    }
                }
            }
            return recordMap;
            
        }
    }
}