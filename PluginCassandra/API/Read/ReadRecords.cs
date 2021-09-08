using System;
using System.Collections.Generic;
using System.Linq;
using Cassandra;
using Naveego.Sdk.Plugins;
using Newtonsoft.Json;
using PluginCassandra.API.Factory;
using PluginCassandra.Helper;
using Logger = Naveego.Sdk.Logging.Logger;

namespace PluginCassandra.API.Read
{
    public static partial class Read
    {
        public static async IAsyncEnumerable<Record> ReadRecords(ISessionFactory sessionFactory, Schema schema)
        {
            
            var session = sessionFactory.GetSession();
            
            try
            {
                var query = schema.Query;

                if (string.IsNullOrWhiteSpace(query))
                {
                    query = $"SELECT * FROM {schema.Id}";
                }
                
                RowSet rows;
                
                try
                {
                    rows = await session.Execute(query);
                }
                catch (Exception e)
                {
                    Logger.Error(e, e.Message);
                    yield break;
                }
                
                if (rows != null)
                {
                    foreach (var row in rows)
                    {
                        var recordMap = new Dictionary<string, object>();

                        foreach (var property in schema.Properties)
                        {
                            try
                            {
                                switch (property.Type)
                                {
                                    case PropertyType.String:
                                    case PropertyType.Text:
                                    case PropertyType.Decimal:
                                        recordMap[property.Id] = row[property.Id].ToString();
                                        break;
                                    default:
                                        recordMap[property.Id] = row[property.Id];
                                        break;
                                }
                            }
                            catch (Exception e)
                            {
                                Logger.Error(e, $"No column or no data with property Id: {property.Id}");
                                Logger.Error(e, e.Message);
                                recordMap[property.Id] = null;
                            }
                        }
                        var record = new Record
                        {
                            Action = Record.Types.Action.Upsert,
                            DataJson = JsonConvert.SerializeObject(recordMap)
                        };
                        
                        yield return record;
                    }
                    
                    // while (await reader.ReadAsync())
                    // {
                    //     var recordMap = new Dictionary<string, object>();
                    //
                    //     foreach (var property in schema.Properties)
                    //     {
                    //         try
                    //         {
                    //             switch (property.Type)
                    //             {
                    //                 case PropertyType.String:
                    //                 case PropertyType.Text:
                    //                 case PropertyType.Decimal:
                    //                     recordMap[property.Id] = reader.GetValueById(property.Id, '`').ToString();
                    //                     break;
                    //                 default:
                    //                     recordMap[property.Id] = reader.GetValueById(property.Id, '`');
                    //                     break;
                    //             }
                    //         }
                    //         catch (Exception e)
                    //         {
                    //             Logger.Error(e, $"No column with property Id: {property.Id}");
                    //             Logger.Error(e, e.Message);
                    //             recordMap[property.Id] = null;
                    //         }
                    //     }
                    //
                    //     var record = new Record
                    //     {
                    //         Action = Record.Types.Action.Upsert,
                    //         DataJson = JsonConvert.SerializeObject(recordMap)
                    //     };
                    //
                    //     yield return record;
                    // }
                }
            }
            finally
            {
                //Noop
            }
        }
    }
}