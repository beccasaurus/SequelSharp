using System;
using System.Linq;
using System.Reflection;
using System.Collections.Generic;
using System.Text.RegularExpressions;

namespace SequelSharp {

    /// <summary>
    /// Provides the main DSL for connecting to databases, eg. Sequel.SqlServer(server: "localhost\sqlexpress", database: "MyDatabase");
    /// </summary>
    public class Sequel {

		/// <summary>If the type of database you want to connect to is not specified, this Type is used</summary>
		public static Type DefaultDatabaseType = typeof(SqlServerDatabase);

		/// <summary>Returns a Sequel.Database for the provided connection string, eg. "sqlite://foo.db"</summary>
		/// <remarks>
		/// If the connection string doesn't starts with something:// then we pass the full connection string 
		/// to the DefaultDatabaseType.
		///
		/// Otherwise, we use the something:// portion of the connection string to determine what Database type 
		/// we should use, eg. sqlserver:// loads the SqlServerDatabase type because we look for a type that 
		/// inherits from SequelSharp.Database and ends with *Database matching this string.
		/// </remarks>
		public static Database Connect(string connectionString) {
			var match = Regex.Match(connectionString, @"([^:]+)://(.*)");
			
			if (match == Match.Empty)
				return InstantiateDatabase(DefaultDatabaseType, connectionString);

			var databaseTypeName       = match.Groups[1].ToString();
			var nativeConnectionString = match.Groups[2].ToString();

			return InstantiateDatabase(DatabaseTypeFor(databaseTypeName), nativeConnectionString);
		}

		/// <summary>Given a string like "sqlserver" this will return the SequelSharp.SqlServerDatabase type.</summary>
		public static Type DatabaseTypeFor(string name) {
			if (name.ToLower().Trim() == "sqlserver")
				return typeof(SequelSharp.SqlServerDatabase);
			else
				throw new NotImplementedException("Haven't implemented the logic that finds database types given a name yet!");
		}

		#region Private
		static Database InstantiateDatabase(Type type, string connectionString) {
			var db = Activator.CreateInstance(type) as Database;
			db.LoadConnectionOptions(connectionString);
			return db;
		}
		#endregion
    }
}
