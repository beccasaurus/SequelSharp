using System;
using System.Linq;
using System.Data;
using System.Data.Common;
using System.Collections.Generic;

using System.Data.SqlClient; // make this go away!

namespace SequelSharp {

	// NOTE once we have more implemented, we'll clean this up and put classes into their own files ...

	public class TableRow : Dictionary<string, object>, IDictionary<string, object> {
		public Table Table { get; set; }

		public string TableName { get { return Table.Name; } }

		public object Key {
			get { return this[Table.KeyName]; }
		}

		public Database Database {
			get { return Table.Database; }
		}

		public int Update(object columns) {
			return Update(Util.ObjectToDictionary(columns));
		}

		public int Update(IDictionary<string, object> columns) {
			var setText = string.Join(", ", columns.Select(c => string.Format("{0} = @{1}", c.Key, c.Key)).ToArray());
			var sql     = string.Format("UPDATE {0} SET {1} WHERE {2} = @updateKeyValue", TableName, setText, Table.KeyName);
			columns["updateKeyValue"] = Key;
			return Database.ExecuteNonQuery(sql, columns);
		}
	}

	public class Column {
		public Table Table { get; set; }
		public string Name { get; set; }
		public DbType DataType { get; set; }
	}

	public class Table {
		public Database Database { get; set; }
		public string Name { get; set; }

		public ColumnList Columns {
			get { return Database.GetColumns(this); }
		}

		public List<string> ColumnNames {
			get { return Columns.Select(c => c.Name).ToList(); }
		}

		public string KeyName {
			get { return string.Join(", ", KeyNames.ToArray()); }
		}

		public List<string> KeyNames {
			get { return KeyColumns.Select(c => c.Name).ToList(); }
		}

		public ColumnList KeyColumns {
			get {
				var columns = new ColumnList();
				foreach (DataRow row in Database.IndexColumnsSchema.Rows)
					if (row["table_name"].ToString() == this.Name)
						if (row["constraint_name"].ToString().StartsWith("PK_"))
							columns.Add(this.Columns[row["column_name"].ToString()]);
				return columns;
			}
		}

		public int Count {
			get { return (int) Database.ExecuteScalar(string.Format("SELECT COUNT(*) FROM {0}", this.Name)); }
		}

		public int Insert(object columns) {
			return Insert(Util.ObjectToDictionary(columns));
		}

		public int Insert(IDictionary<string, object> columns) {
			var columnNames      = string.Join(", ", columns.Keys.ToArray());
			var columnParameters = string.Join(", ", columns.Keys.Select(key => "@" + key).ToArray());
			var sql              = string.Format("INSERT INTO {0} ({1}) values ({2})", this.Name, columnNames, columnParameters);

			return Database.ExecuteNonQuery(sql, columns);
		}

		public TableRowList All {
			get { return Database.GetRows(this, "select * from " + this.Name); }
		}

		public TableRow First {
			get { return Database.GetRow(this, "select top 1 * from " + this.Name); }
		}

		public TableRow Last {
			get { return Database.GetRow(this, "select top 1 * from " + this.Name + " order by " + this.KeyName + " desc"); }
		}
	}

	public class ColumnList : List<Column>, IList<Column> {
		public Column this[string name] {
			get { return this.FirstOrDefault(c => c.Name == name); }
		}
	}

	public class TableList : List<Table>, IList<Table> {
		public Table this[string name] {
			get { return this.FirstOrDefault(t => t.Name == name); }
		}
	}

	public class TableRowList : List<TableRow>, IList<TableRow> {
		public void Add(Table table, DbDataReader reader) {
			var row = new TableRow { Table = table };
			for (int i = 0; i < reader.FieldCount; i++)
				row.Add(reader.GetName(i), reader[i]);
			base.Add(row);
		}
	}

	/// <summary>Represents some type of Database</summary>
	public abstract class Database {

		#region Abstract Methods/Properties that your Database class needs to implement
		public abstract DbConnection Connection { get; }
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
			ExecuteReader(sql, null, action);
		}

		public void ExecuteReader(string sql, object parameters, Action<DbDataReader> action) {
			ExecuteReader(sql, Util.ObjectToDictionary(parameters), action);
		}

		public void ExecuteReader(string sql, IDictionary<string, object> parameters, Action<DbDataReader> action) {
			Sequel.Log("ExecuteReader('{0}', {1})", sql, parameters == null ? "null" : string.Join(", ", parameters.Select(i => i.Key + " = " + i.Value.ToString()).ToArray()));
			var command = CreateCommand(sql);
			AddCommandParameters(command, parameters);
			using (var connection = command.Connection) {
				connection.Open();
				using (var reader = command.ExecuteReader())
					action.Invoke(reader);
			}
		}

		public int ExecuteNonQuery(string sql) {
			return ExecuteNonQuery(sql, null);
		}

		public int ExecuteNonQuery(string sql, object parameters) {
			return ExecuteNonQuery(sql, Util.ObjectToDictionary(parameters));
		}

		public int ExecuteNonQuery(string sql, IDictionary<string, object> parameters) {
			Sequel.Log("ExecuteNonQuery('{0}', {1})", sql, parameters == null ? "null" : string.Join(", ", parameters.Select(i => i.Key + " = " + i.Value.ToString()).ToArray()));
			var command = CreateCommand(sql);
			AddCommandParameters(command, parameters);
			using (var connection = command.Connection) {
				connection.Open();
				return command.ExecuteNonQuery();
			}
		}

		public object ExecuteScalar(string sql) {
			return ExecuteScalar(sql, null);
		}

		public object ExecuteScalar(string sql, object parameters) {
			return ExecuteScalar(sql, Util.ObjectToDictionary(parameters));
		}

		public object ExecuteScalar(string sql, IDictionary<string, object> parameters) {
			Sequel.Log("ExecuteScalar('{0}', {1})", sql, parameters == null ? "null" : string.Join(", ", parameters.Select(i => i.Key + " = " + i.Value.ToString()).ToArray()));
			var command = CreateCommand(sql);
			AddCommandParameters(command, parameters);
			using (var connection = command.Connection) {
				connection.Open();
				return command.ExecuteScalar();
			}
		}

		public TableRowList GetRows(Table table, string sql) {
			return GetRows(table, sql, null);
		}

		public TableRowList GetRows(Table table, string sql, object parameters) {
			return GetRows(table, sql, Util.ObjectToDictionary(parameters));
		}

		public TableRowList GetRows(Table table, string sql, IDictionary<string, object> parameters) {
			var rows = new TableRowList();
			ExecuteReader(sql, parameters, reader => {
				while (reader.Read())
					rows.Add(table, reader);
			});
			return rows;
		}

		public TableRow GetRow(Table table, string sql) {
			return GetRow(table, sql, null);
		}

		public TableRow GetRow(Table table, string sql, object parameters) {
			return GetRow(table, sql, Util.ObjectToDictionary(parameters));
		}

		public TableRow GetRow(Table table, string sql, IDictionary<string, object> parameters) {
			return GetRows(table, sql, parameters).FirstOrDefault();
		}

		public void AddCommandParameters(DbCommand command, IDictionary<string, object> parameters) {
			if (parameters == null) return;
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

		public DataTable GetSchema(string collectionName) {
			using (var connection = Connection) {
				connection.Open();
				return connection.GetSchema(collectionName);
			}
		}

		public DataTable ColumnSchema        { get { return GetSchema("Columns");      } }
		public DataTable TableSchema         { get { return GetSchema("Tables");       } }
		public DataTable IndexColumnsSchema  { get { return GetSchema("IndexColumns"); } }

		// TODO CreateTable will eventually have a DSL and each adapter will need to generate the CREATE TABLE sql from our column data.
		//      But, for now, we just need to get this working, so we're just using raw SQL ...
		public bool CreateTable(string tableName, string columnSql) {
			return ExecuteNonQuery(string.Format("CREATE TABLE {0} ({1})", tableName, columnSql)) > 0;
		}

		public virtual TableList Tables {
			get {
				var tables = new TableList();
				var schema = TableSchema;
				foreach (DataRow row in schema.Rows)
					tables.Add(new Table { Database = this, Name = row["TABLE_NAME"].ToString() });
				return tables;
			}
		}

		public virtual ColumnList GetColumns(Table table) {
			var columns = new ColumnList();
			var schema  = ColumnSchema;
			foreach (DataRow row in schema.Rows)
				if (row["TABLE_NAME"].ToString() == table.Name)
					columns.Add(new Column { Table = table, Name = row["COLUMN_NAME"].ToString(), DataType = StringToDbType(row["DATA_TYPE"].ToString()) });
			return columns;
		}

		public virtual DbType StringToDbType(string name) {
			switch (name.ToLower().Trim()) {
				case "int":              return DbType.Int32;
				case "char":             return DbType.String;
				case "nchar":            return DbType.String;
				case "varchar":          return DbType.String;
				case "nvarchar":         return DbType.String;
				case "bit":              return DbType.Boolean;
				case "datetime":         return DbType.DateTime;
				case "tinyint":          return DbType.Double;
				case "smallint":         return DbType.Int32;
				case "uniqueidentifier": return DbType.Guid;
				default:
					throw new Exception("Don't know what DbType to return for: " + name);
			}
		}

		public virtual Table this[string tableName] {
			get { return Tables[tableName]; }
		}
	}
}
