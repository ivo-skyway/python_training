import os
from unittest import TestCase

import sqlalchemy

from Database import Localhost, LocalPG


class TestLocalhost(TestCase):
    # Requirements to run:
    # 1. A MySQL local DB called "test_dbm"
    # 2. Env variables to access the local DB: LOCAL_USER, LOCAL_PW

    def setUp(self):
        self.database_sql = "_database_setup/test_halo_database.sql"
        self.db = Localhost("test_dbm")

    def test_create_and_connect_db_with_no_existing_table(self):
        self.db.create_connection()

    def test_it_can_get_an_engine(self):
        self.db.create_connection()
        engine = self.db.get_engine()
        self.assertIs(sqlalchemy.engine.base.Engine, type(engine))

    #TODO: Not a good test, should actually talk to a DB
    def test_it_can_run_query(self):
        self.db.setupDb("../_database_setup/cycle_596_data.sql")
        self.db.create_connection()

        self.assertIsNotNone(self.db.run_query('SELECT * FROM meta_data'))

    def test_it_can_insert_and_update(self):
        self.db.create_connection()
        self.assertIsNotNone(self.db.run_statement('insert into meta_data (position,type,side,axle,sensor_number,set_point,unique_id,active) values ("I","T","R",3,23455,100,"3456_23455",1)'))
        self.assertIsNotNone(self.db.run_statement('update meta_data set active = 0 where unique_id = "3456_23455";' ))

    def test_it_can_create_schema(self):
        self.db.create_connection()
        self.db.create_schema("../" + self.database_sql)
        db_names = self.db.run_query('show databases;')
        self.assertIn(("test_dbm",), db_names)  # need to do this horrid tuple match since that's what it expects
        self.assertIsNotNone(self.db.run_query('describe meta_data;'))

    def test_it_can_create_db_with_convenient_function(self):
        self.db.setupDb("../_database_setup/small_trigger_data_mock_data.sql", tables_file_path="../_database_setup/test_halo_database.sql")
        self.assertIn((2000,), self.db.run_query('select count(id) from sensor_data;'))

    def test_it_can_autobase(self):
        self.db.create_connection()
        self.db.create_schema("../" + self.database_sql)
        base, session = self.db.create_base_with_session()

    def test_it_can_generate_production_schema(self):
        self.db.setupDb("../_database_setup/small_trigger_data_mock_data.sql", tables_file_path="../_database_setup/grace_production_schema.sql", disable_foreign_keys=True)


class TestCleanupDB(TestCase):
    def setUp(self):
        self.operational_db = Localhost('operational_db')
        self.operational_db.setupDb("../_database_setup/cycle_596_data.sql")
        self.analytics_db = Localhost('analytics_db')
        self.analytics_db.setupDb("../_database_setup/cycle_596_data.sql")

    def test_cleanupdb_doesnt_hang(self):
        a = self.operational_db.run_query("SELECT * FROM cold_inflation_pressure")

        self.assertIsNotNone(a)

    def test_cleanupdb_doesnt_hang_2(self):
        a = self.operational_db.run_query("SELECT * FROM cold_inflation_pressure")
        self.assertIsNotNone(a)

    def tearDown(self):
        self.operational_db.cleanUpDB()
        self.analytics_db.cleanUpDB()

class TestDBCreation(TestCase):
    def test_it_can_make_a_db_when_it_does_not_exist(self):
        localhost = Localhost("make_a_db")
        localhost.setupDb()
        localhost.create_connection()
        localhost.run_statement("DROP DATABASE make_a_db;")

class TestLocalPG(TestCase):
    # Requirements to run:
    # 1. A PGSQL local DB called "localpg"
    # 2. Env variables to access the local DB: PG_USER, PG_PW

    def setUp(self):
        self.db = LocalPG("localpg")

    def test_it_can_connect_to_local_pg(self):
        self.db.create_connection()

    def test_it_can_refresh_local_pg(self):
        self.db.create_connection()
        self.db.refresh_database()
        result = self.db.connection.execute("SELECT datname FROM pg_database;")
        existing_databases = result.fetchall()
        self.assertTrue((str(self.db.db_name)) in str(existing_databases))