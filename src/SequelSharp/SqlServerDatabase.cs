using System;
using System.Linq;
using System.Data.Common;
using System.Data.SqlClient;
using System.Collections.Generic;

namespace SequelSharp {

	/// <summary>Represents a SQL Server Database</summary>
	/// <remarks>
	/// If we connect to the [master] database of a SQL Server, we can drop/create databases so, 
	/// even though this represents a single "Database", we can actually do things with this like 
	/// list, create, and drop databases on this SQL Server.
	/// </remarks>
	public class SqlServerDatabase : Database {

		#region SequelSharp.Database implementation
		DbConnection _connection;
		public override DbConnection Connection {
			get {
				if (_connection == null)
					return new SqlConnection(ConnectionString);
				return _connection;
			}
		}
		#endregion

		#region Custom SqlServerDatabase methods
		public List<string> DatabaseNames {
			get {
				var names = new List<string>();

				using (var reader = ExecuteReader("select name from sys.databases"))
					while (reader.Read())
						names.Add(reader["name"].ToString());

				return names;
			}
		}

		public bool CreateDatabase(string name) {
			return ExecuteNonQuery("create database @database_name", new { database_name = name }) > 0;
		}

		public bool DropDatabase(string name) {
			throw new NotImplementedException("not done");
		}
		#endregion
	}
}
