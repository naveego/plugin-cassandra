using System.Threading.Tasks;
using PluginCassandra.API.Factory;
using PluginCassandra.DataContracts;

namespace PluginCassandra.API.Replication
{
    public static partial class Replication
    {
        private static readonly string DropTableQuery = @"DROP TABLE IF EXISTS {0}.{1}";

        public static async Task DropTableAsync(ISessionFactory sessionFactory, ReplicationTable table)
        {
            var session = sessionFactory.GetSession();
            var rows = await session.Execute(string.Format(DropTableQuery,
                Utility.Utility.GetSafeName(table.SchemaName, '"'),
                Utility.Utility.GetSafeName(table.TableName, '"')
            ));
    }
    }
}