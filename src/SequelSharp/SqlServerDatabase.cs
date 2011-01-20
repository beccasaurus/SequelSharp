using System;
using System.Linq;
using System.Data.Common;
using System.Data.SqlClient;
using System.Collections.Generic;
using System.Text.RegularExpressions;

namespace SequelSharp {

	/// <summary>Represents a SQL Server Database</summary>
	/// <remarks>
	/// If we connect to the [master] database of a SQL Server, we can drop/create databases so, 
	/// even though this represents a single "Database", we can actually do things with this like 
	/// list, create, and drop databases on this SQL Server.
	/// </remarks>
	public class SqlServerDatabase : Database {

		#region SequelSharp.Database implementation
		public override DbConnection Connection {
			get { return new SqlConnection(ConnectionString); }
		}

		public override List<Table> Tables {
			get {
				var tables = new List<Table>();
				ExecuteReader("select name from sys.tables", reader => {
					while (reader.Read())
						tables.Add(new Table { Database = this, Name = reader["name"].ToString() });
				});
				return tables;
			}
		}
		#endregion

		#region Custom SqlServerDatabase methods
		public List<string> DatabaseNames {
			get {
				var names = new List<string>();
				ExecuteReader("select name from sys.databases", reader => {
					while (reader.Read())
						names.Add(reader["name"].ToString());
				});
				return names;
			}
		}

		public bool CreateDatabase(string name) {
			try {
				ExecuteNonQuery("create database " + name); return true;
			} catch (SqlException ex) {
				if (ex.Message.Contains("already exists. Choose a different database name.")) return false;
				throw ex;
			}
		}

		public bool DropDatabase(string name) {
			Use("master");
			SqlConnection.ClearAllPools();
			try {
				ExecuteNonQuery("drop database " + name); return true;
			} catch (SqlException ex) {
				if (ex.Message.Contains("it does not exist")) return false;
				throw ex;
			}
		}

		// Right now, this ONLY works if your ConnectionString uses this format:
		//   Initial Catalog=...
		public void Use(string databaseName){
			Console.WriteLine("Use('{0}')", databaseName);
			ConnectionString = Regex.Replace(ConnectionString, "Initial Catalog=[^;]+", "Initial Catalog=" + databaseName);
		}
		#endregion
	}
}
