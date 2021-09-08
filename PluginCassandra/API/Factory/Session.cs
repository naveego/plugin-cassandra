using System;
using System.Data;
using System.Threading.Tasks;
using PluginCassandra.Helper;
using Cassandra;

namespace PluginCassandra.API.Factory
{
    public class Session : ISession
    {
        private readonly Cassandra.ISession _session;

        public Session(Settings settings)
        {
            _session = Cluster.Builder()
                .AddContactPoint(settings.Hostname).WithPort(Int32.Parse(settings.Port))
                .WithCredentials(settings.Username, settings.Password)
                .Build()
                .Connect();
        }
        
        public Cassandra.ISession GetSession()
        {
            return _session;
        }

        public async Task<RowSet> Execute(string statement)
        {
            return await _session.ExecuteAsync(new SimpleStatement(statement));
        }
        
        public RowSet ExecuteNonAsync(string statement)
        {
            return _session.Execute(new SimpleStatement(statement));
        }
    }
}