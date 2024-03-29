using System;
using Grpc.Core;

namespace PluginCassandra.Helper
{
    public class Settings
    {
        public string Hostname { get; set; }
        public string Port { get; set; }
        public string Username { get; set; }
        public string Password { get; set; }

        /// <summary>
        /// Validates the settings input object
        /// </summary>
        /// <exception cref="Exception"></exception>
        public void Validate()
        {
            
            if (String.IsNullOrEmpty(Hostname))
            {
                throw new Exception("The Hostname property must be set");
            }
            if (String.IsNullOrEmpty(Username))
            {
                throw new Exception("The Username property must be set");
            }
            if (String.IsNullOrEmpty(Port))
            {
                throw new Exception("The Port property must be set");
            }
            if (String.IsNullOrEmpty(Password))
            {
                throw new Exception("The Password property must be set");
            }
        }

        /// <summary>
        /// Gets the database connection string
        /// </summary>
        /// <returns></returns>
        public string GetConnectionString()
        {
            return $"Server={Hostname}; Port={Port}; User={Username}; Password={Password};";
        }
        
        /// <summary>
        /// Gets the database connection string
        /// </summary>
        /// <returns></returns>
        public string GetConnectionString(string database)
        {
            return $"Server={Hostname}; Port={Port}; Database={database}; User={Username}; Password={Password};";
        }
    }
}