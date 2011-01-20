using System;
using System.Linq;
using System.Data.Common;
using System.Collections.Generic;

namespace SequelSharp {

	/// <summary>Represents some type of Database</summary>
	public abstract class Database {

		/// <summary>This database's real, native connection string</summary>
		/// <remarks>
		/// Sequel takes the options passed to Connect() and converts them into a ConnectionString 
		/// by figuring out which Database class should be used, instantiating a new instance of that class, 
		/// and then calling Database.LoadConnectionOptions() with the options passed to Connect()
		///
		/// NOTE: The way we deal with connection options will probably TOTALLY change while we implement specs ...
		/// </remarks>
		public string ConnectionString { get; set; }

		/// <summary></summary>
		public void LoadConnectionOptions(string connectionString) {
			ConnectionString = connectionString;
		}

		public abstract DbConnection Connection { get; }

		public DbCommand CreateCommand(string sql) {
			Console.WriteLine("Creating command for: '{0}'", ConnectionString);
			var command = Connection.CreateCommand();
			command.CommandText = sql;
			return command;
		}

		public DbDataReader ExecuteReader(string sql) {
			var command = CreateCommand(sql);
			command.Connection.Open();
			return command.ExecuteReader();
		}

		public int ExecuteNonQuery(string sql) {
			var command = CreateCommand(sql);
			command.Connection.Open();
			return command.ExecuteNonQuery();
		}

		public int ExecuteNonQuery(string sql, Dictionary<string, object> parameters) {
			var command = CreateCommand(sql);
			foreach (var param in parameters) {
				var dbParam           = command.CreateDbParameter();
				dbParam.ParameterName = param.Key;
				dbParam.Value         = param.Value;
				command.Parameters.Add(param);
			}
			command.Connection.Open();
			return command.ExecuteNonQuery();
		}
	}
}
