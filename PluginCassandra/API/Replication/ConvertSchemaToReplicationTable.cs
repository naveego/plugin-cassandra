using System.Collections.Generic;
using Naveego.Sdk.Plugins;
using PluginCassandra.DataContracts;

namespace PluginCassandra.API.Replication
{
    public static partial class Replication 
    {
        public static ReplicationTable ConvertSchemaToReplicationTable(Schema schema, string schemaName,
            string tableName)
        {
            var table = new ReplicationTable
            {
                SchemaName = schemaName,
                TableName = tableName,
                Columns = new List<ReplicationColumn>()
            };
            
            foreach (var property in schema.Properties)
            {
                var column = new ReplicationColumn
                {
                    ColumnName = property.Name,
                    DataType = string.IsNullOrWhiteSpace(property.TypeAtSource)? GetType(property.Type): property.TypeAtSource,
                    PrimaryKey = false
                };
                
                table.Columns.Add(column);
            }

            return table;
        }
        
        private static string GetType(PropertyType dataType)
        {
            switch (dataType)
            {
                case PropertyType.Datetime:
                    return "varchar";
                case PropertyType.Date:
                    return "varchar";
                case PropertyType.Time:
                    return "varchar";
                case PropertyType.Integer:
                    return "int";
                case PropertyType.Decimal:
                    return "decimal";
                case PropertyType.Float:
                    return "double";
                case PropertyType.Bool:
                    return "boolean";
                case PropertyType.Blob:
                    return "blob";
                case PropertyType.String:
                    return "varchar";
                case PropertyType.Text:
                    return "string";
                default:
                    return "string";
            }
        }
    }
}