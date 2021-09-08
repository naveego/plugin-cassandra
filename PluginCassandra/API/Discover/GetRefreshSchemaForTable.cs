using System.Collections.Generic;
using System.Threading.Tasks;
using Naveego.Sdk.Plugins;
using PluginCassandra.API.Factory;

namespace PluginCassandra.API.Discover
{
    public static partial class Discover
    {

        private const string GetTableAndColumnsQuery = @"
SELECT keyspace_name,
       table_name,
       column_name,
       type,
       kind
FROM system_schema.columns
where keyspace_name = '{0}'
and table_name = '{1}'
ALLOW FILTERING";

        public static async Task<Schema> GetRefreshSchemaForTable(ISessionFactory sessionFactory, Schema schema,
            int sampleSize = 5)
        {
            var decomposed = DecomposeSafeName(schema.Id).TrimEscape();

            var session = sessionFactory.GetSession();

            var rows = await session.Execute(string.Format(GetTableAndColumnsQuery, decomposed.Schema, decomposed.Table));
            
            var refreshProperties = new List<Property>();

            foreach (var row in rows)
            {
                var property = new Property
                {
                    Id = row[2].ToString(),
                    Name = row[2].ToString(),
                    IsKey = (row[4].ToString() == "partition_key" || row[4].ToString() == "clustering"),
                    IsNullable = !(row[4].ToString() == "partition_key" || row[4].ToString() == "clustering"),
                    Type = GetType(row[3].ToString()),
                    TypeAtSource = row[3].ToString(),
                };
                refreshProperties.Add(property);
            }
            // add properties
            
            schema.Properties.Clear();
            schema.Properties.AddRange(refreshProperties);

            // get sample and count
            return await AddSampleAndCount(sessionFactory, schema, sampleSize);
        }

        private static DecomposeResponse DecomposeSafeName(string schemaId)
        {
            var response = new DecomposeResponse
            {
                Database = "",
                Schema = "",
                Table = ""
            };
            var parts = schemaId.Split('.');

            switch (parts.Length)
            {
                case 0:
                    return response;
                case 1:
                    response.Table = parts[0];
                    return response;
                case 2:
                    response.Schema = parts[0];
                    response.Table = parts[1];
                    return response;
                case 3:
                    response.Database = parts[0];
                    response.Schema = parts[1];
                    response.Table = parts[2];
                    return response;
                default:
                    return response;
            }
        }

        private static DecomposeResponse TrimEscape(this DecomposeResponse response, char escape = '"')
        {
            response.Database = response.Database.Trim(escape);
            response.Schema = response.Schema.Trim(escape);
            response.Table = response.Table.Trim(escape);

            return response;
        }
    }

    class DecomposeResponse
    {
        public string Database { get; set; }
        public string Schema { get; set; }
        public string Table { get; set; }
    }
}