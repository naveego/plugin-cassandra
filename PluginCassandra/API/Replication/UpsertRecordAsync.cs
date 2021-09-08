using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Naveego.Sdk.Logging;
using Newtonsoft.Json;
using PluginCassandra.API.Factory;
using PluginCassandra.DataContracts;
using PluginCassandra.Helper;

namespace PluginCassandra.API.Replication
{
    public static partial class Replication
    {
        public static async Task UpsertRecordAsync(ISessionFactory sessionFactory,
            ReplicationTable table,
            Dictionary<string, object> recordMap)
        {
            var session = sessionFactory.GetSession();
            
            try
            {
                // try to insert
                var querySb =
                    new StringBuilder(
                        $"INSERT INTO {Utility.Utility.GetSafeName(table.SchemaName, '"')}.{Utility.Utility.GetSafeName(table.TableName, '"')}(");
                foreach (var column in table.Columns)
                {
                    querySb.Append($"{Utility.Utility.GetSafeName(column.ColumnName, '"')},");
                }

                querySb.Length--;
                querySb.Append(") VALUES (");

                foreach (var column in table.Columns)
                {
                    if (recordMap.ContainsKey(column.ColumnName))
                    {
                        var rawValue = recordMap[column.ColumnName];
                        if (column.Serialize)
                        {
                            rawValue = JsonConvert.SerializeObject(rawValue);
                        }

                        switch (column.DataType)
                        {
                            case("bigint"):
                            case("smallint"):
                            case("varint"):
                            case("int"):
                            case("decimal"):
                            case("float"):
                            case("counter"):
                                querySb.Append(rawValue != null
                                    ? $"{Utility.Utility.GetSafeString(rawValue.ToString())},"
                                    : $"NULL,");
                                break;
                            default:
                                querySb.Append(rawValue != null
                                    ? $"'{Utility.Utility.GetSafeString(rawValue.ToString(), "\"")}',"
                                    : $"NULL,");
                                break;
                        }
                        
                    }
                    else
                    {
                        querySb.Append($"NULL,");
                    }
                }

                querySb.Length--;
                querySb.Append(");");

                var query = querySb.ToString();

                Logger.Debug($"Insert record query: {query}");

                await session.Execute(query);


            }
            catch (Exception e)
            {
                try
                {
                    // update if it failed
                    var querySb =
                        new StringBuilder(
                            $"UPDATE {Utility.Utility.GetSafeName(table.SchemaName, '"')}.{Utility.Utility.GetSafeName(table.TableName, '"')} SET ");
                    foreach (var column in table.Columns)
                    {
                        if (!column.PrimaryKey)
                        {
                            if (recordMap.ContainsKey(column.ColumnName))
                            {
                                var rawValue = recordMap[column.ColumnName];
                                if (column.Serialize)
                                {
                                    rawValue = JsonConvert.SerializeObject(rawValue);
                                }

                                if (rawValue != null)
                                {
                                    querySb.Append(
                                        $"{Utility.Utility.GetSafeName(column.ColumnName, '"')}='{Utility.Utility.GetSafeString(rawValue.ToString(), "'", "''")}',");
                                }
                                else
                                {
                                    querySb.Append($"{Utility.Utility.GetSafeName(column.ColumnName, '"')}=NULL,");
                                }
                            }
                            else
                            {
                                querySb.Append($"{Utility.Utility.GetSafeName(column.ColumnName, '"')}=NULL,");
                            }
                        }
                    }

                    querySb.Length--;

                    var primaryKey = table.Columns.Find(c => c.PrimaryKey);
                    var primaryValue = recordMap[primaryKey.ColumnName];
                    if (primaryKey.Serialize)
                    {
                        primaryValue = JsonConvert.SerializeObject(primaryValue);
                    }

                    querySb.Append($" WHERE {primaryKey.ColumnName} = '{primaryValue}'");

                    var query = querySb.ToString();

                    await session.Execute(query);

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