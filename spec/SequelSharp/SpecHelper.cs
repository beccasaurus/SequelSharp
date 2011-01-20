using System;
using System.IO;

namespace SequelSharp.Specs {

    public class Spec {

        // The root project directory, assuming we're running the tests from bin\[Debug|Release]\
        public static string RootDirectory {
            get { return Path.GetFullPath(Path.Combine(Directory.GetCurrentDirectory(), Path.Combine("..", ".."))); }
        }

        public static string SpecDirectory { get { return Path.Combine(RootDirectory, "spec"); }}

        // Gets the connection string for a SQL Server to use for our specs
        public static string SqlServerConnectionString {
            get {
                var connStringFile = Path.Combine(SpecDirectory, "SqlServerConnectionString");
                
                if (! File.Exists(connStringFile))
                    throw new Exception(string.Format(@"File not found: {0}.  To run the specs, please put a SQL Server connection string in spec/SqlServerConnectionString", connStringFile));
                
                using (var reader = new StreamReader(connStringFile))
                    return reader.ReadToEnd().Trim();
            }
        }
    }
}
