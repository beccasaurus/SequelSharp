using System;
using System.Linq;
using System.Data.Common;
using System.Collections.Generic;

using System.Data.SqlClient;

namespace SequelSharp {

	// not sure what we're gonna do with this ...
	public class Table {
		public Database Database { get; set; }
		public string Name { get; set; }
	}

	/// <summary>Represents some type of Database</summary>
	public abstract class Database {

		#region Abstract Methods/Properties that your Database class needs to implement
		public abstract DbConnection Connection { get; }
		public abstract List<Table> Tables { get; }
		#endregion

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

		public DbCommand CreateCommand(string sql) {
			var command = Connection.CreateCommand();
			command.CommandText = sql;
			return command;
		}

		public void ExecuteReader(string sql, Action<DbDataReader> action) {
			Console.WriteLine("ExecuteReader('{0}')", sql);
			var command = CreateCommand(sql);
			using (var connection = command.Connection) {
				connection.Open();
				using (var reader = command.ExecuteReader())
					action.Invoke(reader);
			}
		}

		public int ExecuteNonQuery(string sql) {
			Console.WriteLine("ExecuteNonQuery('{0}')", sql);
			var command = CreateCommand(sql);
			using (var connection = command.Connection) {
				connection.Open();
				return command.ExecuteNonQuery();
			}
		}

		public int ExecuteNonQuery(string sql, object parameters) {
			return ExecuteNonQuery(sql, Util.ObjectToDictionary(parameters));
		}

		public int ExecuteNonQuery(string sql, IDictionary<string, object> parameters) {
			Console.WriteLine("ExecuteNonQuery('{0}', [parameters])", sql);
			var command = CreateCommand(sql);
			AddCommandParameters(command, parameters);
			using (var connection = command.Connection) {
				connection.Open();
				return command.ExecuteNonQuery();
			}
		}

		public void AddCommandParameters(DbCommand command, IDictionary<string, object> parameters) {
			foreach (var param in parameters) {
				var dbParam           = command.CreateParameter();
				dbParam.ParameterName = "@" + param.Key;
				dbParam.Value         = param.Value;
				command.Parameters.Add(dbParam);
			}
		}

		public List<string> TableNames {
			get { return Tables.Select(table => table.Name).ToList(); }
		}

		// TODO CreateTable will eventually have a DSL and each adapter will need to generate the CREATE TABLE sql from our column data.
		//      But, for now, we just need to get this working, so we're just using raw SQL ...
		public bool CreateTable(string tableName, string columnSql) {
			return ExecuteNonQuery(string.Format("CREATE TABLE {0} ({1})", tableName, columnSql)) > 0;
		}
	}
}
