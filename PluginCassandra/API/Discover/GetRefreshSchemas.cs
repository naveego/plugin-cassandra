using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using Google.Protobuf.Collections;
using Naveego.Sdk.Plugins;
using PluginCassandra.API.Factory;

namespace PluginCassandra.API.Discover
{
    public static partial class Discover
    {
        private const string ColumnInfoQuery = @"
                SELECT keyspace_name,
                       table_name,
                       column_name,
                       type,
                       kind
                FROM system_schema.columns
                where keyspace_name = '{0}'
                and table_name = '{1}'
                and column_name = '{2}'
                ALLOW FILTERING";
        
        public static async IAsyncEnumerable<Schema> GetRefreshSchemas(ISessionFactory sessionFactory,
            RepeatedField<Schema> refreshSchemas, int sampleSize = 5)
        {
            var session = sessionFactory.GetSession();
            
            foreach (var schema in refreshSchemas)
            {
                
                if (string.IsNullOrWhiteSpace(schema.Query))
                {
                    yield return await GetRefreshSchemaForTable(sessionFactory, schema, sampleSize);
                    continue;
                }

                var rows = await session.Execute(schema.Query);

                var properties = new List<Property>();
                
                foreach (var column in rows.Columns)
                {
                    //Query system_schema.columns for col info

                    var colInfo = await session.Execute(string.Format(ColumnInfoQuery, column.Keyspace, column.Table, column.Name));

                    var row = colInfo.First();
                    
                    var property = new Property
                    {
                        Id = row[2].ToString(),
                        Name = row[2].ToString(),
                        IsKey = (row[4].ToString() == "partition_key" || row[4].ToString() == "clustering"),
                        IsNullable = !(row[4].ToString() == "partition_key" || row[4].ToString() == "clustering"),
                        Type = GetType(row[3].ToString()),
                        TypeAtSource = row[3].ToString(),
                    };
                    properties.Add(property);
                    
                }
                
                schema.Properties.Clear();
                schema.Properties.AddRange(properties);
                
                // get sample and count
                yield return await AddSampleAndCount(sessionFactory, schema, sampleSize);
                

                //
                //  //var cmd = connFactory.GetCommand(schema.Query, conn);
                // //
                // // var reader = await cmd.ExecuteReaderAsync();
                //
                // var rows = await session.Execute(schema.Query);
                //
                // var schemaTable = reader.GetSchemaTable();
                //
                // var properties = new List<Property>();
                // if (schemaTable != null)
                // {
                //     var unnamedColIndex = 0;
                //
                //     // get each column and create a property for the column
                //     foreach (DataRow row in schemaTable.Rows)
                //     {
                //         // get the column name
                //         var colName = row["ColumnName"].ToString();
                //         if (string.IsNullOrWhiteSpace(colName))
                //         {
                //             colName = $"UNKNOWN_{unnamedColIndex}";
                //             unnamedColIndex++;
                //         }
                //
                //         // create property
                //         var property = new Property
                //         {
                //             Id = Utility.Utility.GetSafeName(colName, '`'),
                //             Name = colName,
                //             Description = "",
                //             Type = GetPropertyType(row),
                //             // TypeAtSource = row["DataType"].ToString(),
                //             IsKey = Boolean.Parse(row["IsKey"].ToString()),
                //             IsNullable = Boolean.Parse(row["AllowDBNull"].ToString()),
                //             IsCreateCounter = false,
                //             IsUpdateCounter = false,
                //             PublisherMetaJson = ""
                //         };
                //
                //         // add property to properties
                //         properties.Add(property);
                //     }
                // }
                //
                // // add only discovered properties to schema
                // schema.Properties.Clear();
                // schema.Properties.AddRange(properties);
                //
                // // get sample and count
                // yield return await AddSampleAndCount(sessionFactory, schema, sampleSize);
            }
            
        }
    }
}