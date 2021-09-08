using System.Data;
using System.Threading.Tasks;
using Cassandra;

namespace PluginCassandra.API.Factory
{
    public interface ISession
    {
        Cassandra.ISession GetSession();

        Task<RowSet> Execute(string statement); 
        RowSet ExecuteNonAsync(string statement); 

    }
}