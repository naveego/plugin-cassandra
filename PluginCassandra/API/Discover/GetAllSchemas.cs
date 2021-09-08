using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Naveego.Sdk.Plugins;
using PluginCassandra.API.Factory;

namespace PluginCassandra.API.Discover
{
    public static partial class Discover
    {
        private const string TableName = "table_name";
        private const string TableSchema = "keyspace_name";
        //private const string TableType = "TABLE_TYPE";
        private const string ColumnName = "column_name";
        private const string DataType = "type";
        private const string ColumnKey = "kind";
        // private const string IsNullable = "IS_NULLABLE";
        // private const string CharacterMaxLength = "CHARACTER_MAXIMUM_LENGTH";


        
        private const string GetAllTablesAndColumnsQuery = @"
SELECT keyspace_name, 
       table_name, 
       column_name, 
       type, 
       kind
FROM system_schema.columns";

        public static async IAsyncEnumerable<Schema> GetAllSchemas(ISessionFactory sessionFactory, int sampleSize = 5)
        {

            var session = sessionFactory.GetSession();


            var rows = await session.Execute(GetAllTablesAndColumnsQuery);

            //var data = new List<string> { };

            Schema schema = null;
            var currentSchemaId = "";

            // foreach (var datum in rows.First())
            // {
            //     data.Add(datum?.ToString());
            // }

            foreach (var row in rows)
            {
                //keyspace.table
                var schemaId = $"\"{row[0]}\".\"{row[1]}\"";
                
                if (schemaId != currentSchemaId)
                {
                    //return previous schema
                    if (schema != null)
                    {
                        // get sample and count
                        yield return await AddSampleAndCount(sessionFactory, schema, sampleSize);
                    }

                    // start new schema
                    currentSchemaId = schemaId;
                    var parts = DecomposeSafeName(currentSchemaId).TrimEscape();
                    schema = new Schema
                    {
                        Id = currentSchemaId,
                        Name = $"{parts.Schema}.{parts.Table}",
                        Properties = { },
                        DataFlowDirection = Schema.Types.DataFlowDirection.Read
                    };
                }
                
                var property = new Property
                {
                    Id = row[2].ToString(),
                    Name = row[2].ToString(),
                    IsKey = (row[4].ToString() == "partition_key" || row[4].ToString() == "clustering"),
                    IsNullable = !(row[4].ToString() == "partition_key" || row[4].ToString() == "clustering"),
                    Type = GetType(row[3].ToString()),
                    TypeAtSource = row[3].ToString(),
                    //    reader.GetValueById(CharacterMaxLength))
                };
                schema?.Properties.Add(property);
                
            }
            
            if (schema != null)
            {
                // get sample and count
                yield return await AddSampleAndCount(sessionFactory, schema, sampleSize);
            }
        }
        
        private static async Task<Schema> AddSampleAndCount(ISessionFactory sessionFactory, Schema schema,
            int sampleSize)
        {
            // add sample and count
            var records = Read.Read.ReadRecords(sessionFactory, schema).Take(sampleSize);
            schema.Sample.AddRange(await records.ToListAsync());
            schema.Count = await GetCountOfRecords(sessionFactory, schema);

            return schema;
        }

        public static PropertyType GetType(string dataType)
        {
            switch (dataType.ToLower())
            {
                case "datetime":
                case "timestamp":
                    return PropertyType.Datetime;
                case "date":
                    return PropertyType.Date;
                case "time":
                    return PropertyType.Time;
                case "tinyint":
                case "smallint":
                case "mediumint":
                case "int":
                case "bigint":
                    return PropertyType.Integer;
                case "decimal":
                    return PropertyType.Decimal;
                case "float":
                case "double":
                    return PropertyType.Float;
                case "boolean":
                    return PropertyType.Bool;
                case "blob":
                case "mediumblob":
                case "longblob":
                    return PropertyType.Blob;
                case "char":
                case "varchar":
                case "tinytext":
                    return PropertyType.String;
                case "text":
                case "mediumtext":
                case "longtext":
                    return PropertyType.Text;
                default:
                    return PropertyType.String;
            }
        }

        private static string GetTypeAtSource(string dataType, object maxLength)
        {
            return maxLength != null ? $"{dataType}({maxLength})" : dataType;
        }
    }
}