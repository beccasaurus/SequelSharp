using System;
using System.Data;
using System.Linq;
using System.Text;
using System.Collections.Generic;
using NUnit.Framework;
using SequelSharp;

namespace SequelSharp.Specs {

    // We'll rename this and reorganize it once we have some specs working ...
    [TestFixture]
    public class SequelSpec : Spec {

		[SetUp]
		public void before_each() {
			// Drop the databases that we use for testing
            var db = Sequel.Connect("sqlserver://" + SqlServerConnectionString) as SqlServerDatabase;
			db.DropDatabase("MyNewDatabase_TestingSequel");
		}

        [Test]
        public void can_connect_to_SQL_Server_using_Sequel_Connect_with_connection_string() {
            var db = Sequel.Connect("sqlserver://" + SqlServerConnectionString) as SqlServerDatabase;
			db.ConnectionString.ShouldEqual(SqlServerConnectionString);

			db.DatabaseNames.Count.Should(Be.GreaterThanOrEqualTo(4));

			new List<string> { "master", "tempdb", "model", "msdb" }.ForEach(name => {
				db.DatabaseNames.ShouldContain(name);
			});
        }

		[Test]
		public void can_list_tables_in_database() {
            var db = Sequel.Connect("sqlserver://" + SqlServerConnectionString) as SqlServerDatabase;
			db.TableNames.ShouldContain("spt_fallback_db"); // do all master databases have this?
		}

		[Test]
		public void can_Use_a_database() {
            var db = Sequel.Connect("sqlserver://" + SqlServerConnectionString) as SqlServerDatabase;
			db.TableNames.ShouldContain("spt_fallback_db"); // do all master databases have this?

			db.CreateDatabase("MyNewDatabase_TestingSequel");
			db.Use("MyNewDatabase_TestingSequel");
			db.TableNames.ShouldNotContain("spt_fallback_db");

			db.Use("master");
			db.TableNames.ShouldContain("spt_fallback_db"); // do all master databases have this?
		}

		[Test][Ignore]
		public void Sequel_Connect_defaults_to_SQL_Server_if_adapter_not_specified() {
		}

        [Test][Ignore]
        public void can_connect_to_SQL_Server_using_Sequel_SqlServer_with_connection_string() {
        }

        [Test][Ignore]
        public void can_connect_to_SQL_Server_using_Sequel_SqlServer_with_host_which_defaults_to_master_database() {
        }

        [Test][Ignore]
        public void can_connect_to_SQL_Server_using_Sequel_SqlServer_with_host_and_database() {
        }

        [Test][Ignore]
        public void connection_options_can_be_passed_as_a_Dictionary() {
        }

        [Test][Ignore]
        public void connection_options_can_be_passed_as_an_anonymous_object() {
        }

        [Test][Ignore]
        public void connection_options_can_be_passed_using_named_parameters() {
        }

        [Test]
        public void can_create_new_database() {
            var db = Sequel.Connect("sqlserver://" + SqlServerConnectionString) as SqlServerDatabase;
			db.DatabaseNames.ShouldNotContain("MyNewDatabase_TestingSequel");

			db.CreateDatabase("MyNewDatabase_TestingSequel").ShouldBeTrue(); // Returns whether or not it was successful

			db.DatabaseNames.ShouldContain("MyNewDatabase_TestingSequel");

			// If we try adding it again, it should be false
			db.CreateDatabase("MyNewDatabase_TestingSequel").ShouldBeFalse();
        }

        [Test]
        public void can_drop_database() {
            var db = Sequel.Connect("sqlserver://" + SqlServerConnectionString) as SqlServerDatabase;
			db.CreateDatabase("MyNewDatabase_TestingSequel");
			db.DatabaseNames.ShouldContain("MyNewDatabase_TestingSequel");

			db.DropDatabase("MyNewDatabase_TestingSequel").ShouldBeTrue(); // Returns whether or not it was successful

			db.DatabaseNames.ShouldNotContain("MyNewDatabase_TestingSequel");

			// If we try adding it again, it should be false
			db.DropDatabase("MyNewDatabase_TestingSequel").ShouldBeFalse();
        }

		// We need to support VERY SIMPLE create table support so we can test INSERT statements
        [Test]
        public void can_add_table_to_database() {
            var db = Sequel.Connect("sqlserver://" + SqlServerConnectionString) as SqlServerDatabase;
			db.CreateDatabase("MyNewDatabase_TestingSequel");
			db.Use("MyNewDatabase_TestingSequel");

			db.TableNames.ShouldNotContain("my_first_table");

			// crappy CreateTable implementation, but it's a start ... we don't NEED CreateTable support in SequelSharp yet.  We need Insert support more.  Fix this later!
			db.CreateTable("my_first_table", "id int not null primary key, name varchar(255)");

			db.TableNames.ShouldContain("my_first_table");

			// db.CreateTable("my_first_table", t => {
			// 	t.String("Foo");
			// 	t.String("Whatever", length: 50, nullable: false);
			// });

			// db.NewTable("my_first_table").
			// 	WithColumn().
			// 	WithColumn().
			// 	WithColumn().
			// 	Create();

			// db.CreateTable("my_first_table", Columns[]);

			/*
				CreateTable syntaxes I want to support ...

				var table = db.NewTable("foo"); // or new TableBuilder("foo", db);
				table.AddColumn();
				table.AddColumn();
				table.Create(); // or db.CreateTable(table);

				Also ... CreateTable("dogs", "id INT, name TEXT");

				Also ... CreateTable("dogs", t => {
					t.Column(typeof(String), "Name");
					t.Column<string>("Name");
					t.String("name");
				});

			 */
        }

        [Test][Ignore]
        public void can_drop_table_from_database() {
        }

        [Test][Ignore]
        public void connecting_to_non_extent_database_throws_exception_if_Sequel_ThrowExceptions_is_true() {
        }

        [Test][Ignore]
        public void connecting_to_non_extent_database_returns_null_if_Sequel_ThrowExceptions_is_false() {
        }

		[Test]
		public void can_get_columns_in_table() {
            var db = Sequel.Connect("sqlserver://" + SqlServerConnectionString) as SqlServerDatabase;
			db.CreateDatabase("MyNewDatabase_TestingSequel");
			db.Use("MyNewDatabase_TestingSequel");
			db.CreateTable("my_first_table", "id int not null primary key, name varchar(255)");

			db.Tables["my_first_table"].ColumnNames.Count.ShouldEqual(2);
			db.Tables["my_first_table"].ColumnNames.ShouldContain("id");
			db.Tables["my_first_table"].ColumnNames.ShouldContain("name");
			(db.Tables["my_first_table"].Columns["id"].DataType.CompareTo(DbType.Int32)).ShouldEqual(0);
			(db.Tables["my_first_table"].Columns["name"].DataType.CompareTo(DbType.String)).ShouldEqual(0);

			// shortcut
			db["my_first_table"].ColumnNames.Count.ShouldEqual(2);
			db["my_first_table"].ColumnNames.ShouldContain("id");
			db["my_first_table"].ColumnNames.ShouldContain("name");
		}

		[Test]
		public void can_insert_new_rows() {
            var db = Sequel.Connect("sqlserver://" + SqlServerConnectionString) as SqlServerDatabase;
			db.CreateDatabase("MyNewDatabase_TestingSequel");
			db.Use("MyNewDatabase_TestingSequel");
			db.CreateTable("my_first_table", "id int not null primary key, name varchar(255)");

			var table = db["my_first_table"];
			table.Count.ShouldEqual(0);

			table.Insert(new { name = "My Name" });

			table.Count.ShouldEqual(1);
			// check columns ... table.First ...
		}
    }
}


/*
			//var schema = conn.GetSchema("Tables");
TABLE_CATALOG: MyNewDatabase_TestingSequel
TABLE_SCHEMA: dbo
TABLE_NAME: my_first_table
TABLE_TYPE: BASE TABLE

			var schema = conn.GetSchema("Columns");

ATALOG: MyNewDatabase_TestingSequel
TABLE_SCHEMA: dbo
TABLE_NAME: my_first_table
COLUMN_NAME: id
ORDINAL_POSITION: 1
COLUMN_DEFAULT: 
IS_NULLABLE: NO
DATA_TYPE: int
CHARACTER_MAXIMUM_LENGTH: 
CHARACTER_OCTET_LENGTH: 
NUMERIC_PRECISION: 10
NUMERIC_PRECISION_RADIX: 10
NUMERIC_SCALE: 0
DATETIME_PRECISION: 
CHARACTER_SET_CATALOG: 
CHARACTER_SET_SCHEMA: 
CHARACTER_SET_NAME: 
COLLATION_CATALOG:

			foreach (DataRow row in schema.Rows) {
				foreach (DataColumn column in schema.Columns) {
					Console.WriteLine("{0}: {1}", column.ColumnName, row[column]);
				}
				Console.WriteLine("\n\n");
			}
*/
