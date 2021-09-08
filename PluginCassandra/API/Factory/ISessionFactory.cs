using PluginCassandra.Helper;

namespace PluginCassandra.API.Factory
{
    public interface ISessionFactory
    {
        void Initialize(Settings settings);
        ISession GetConnection();
        ISession GetSession();
        //IConnection GetConnection(string database);
        //ICommand GetCommand(string commandText, ISession conn);
    }
}