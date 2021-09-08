using System;
using System.Threading.Tasks;
using Naveego.Sdk.Plugins;
using PluginCassandra.API.Factory;

namespace PluginCassandra.API.Discover
{
    public static partial class Discover
    {
        public static async Task<Count> GetCountOfRecords(ISessionFactory sessionFactory, Schema schema)
        {
            //var conn = connFactory.GetConnection();
            var session = sessionFactory.GetSession();
            
            try
            {
                var rows = await session.Execute($"SELECT COUNT(*) FROM {schema.Id}");
                
                var count = -1;

                foreach (var row in rows)
                {
                    count = Int32.Parse(row["count"].ToString());
                }
                
                return count == -1
                    ? new Count
                    {
                        Kind = Count.Types.Kind.Unavailable,
                    }
                    : new Count
                    {
                        Kind = Count.Types.Kind.Exact,
                        Value = count
                    };
            }
            catch(Exception e)
            {
                return new Count
                {
                    Kind = Count.Types.Kind.Unavailable,
                };
                //noop
            }
        }
    }
}