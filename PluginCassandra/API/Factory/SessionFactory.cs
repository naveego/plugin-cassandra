using System;
using PluginCassandra.Helper;
using Cassandra;

namespace PluginCassandra.API.Factory
{
    public class SessionFactory : ISessionFactory
    {
        private Settings _settings;

        public void Initialize(Settings settings)
        {
            _settings = settings;
        }

        public ISession GetSession()
        {
            return new Session(_settings);
        }
        
        public ISession GetConnection()
        {
            return new Session(_settings);
        }

        
        
        // public IConnection GetConnection(string database)
        // {
        //     return new Connection(_settings, database);
        // }

        // public ICommand GetCommand(string commandText, ISession session)
        // {
        //     return new Command(commandText, session);
        // }
    }
}