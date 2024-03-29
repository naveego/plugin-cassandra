using PluginCassandra.DataContracts;

namespace PluginCassandra.API.Utility
{
    public static partial class Utility
    {
        public static string GetSafeName(string unsafeName, char escapeChar = '"')
        {
            return $"{escapeChar}{unsafeName}{escapeChar}";
        }
    }
}