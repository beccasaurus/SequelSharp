using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
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
    }
}
