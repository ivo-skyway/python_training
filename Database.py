import os
import re

import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError, InternalError, OperationalError


class Database:
    def create_connection(self):
        if self.username is None:
            raise EnvironmentError("database credentials are not set in ENV")
        self.engine = create_engine('mysql+pymysql://{0}:{1}@{2}:3306/{3}'.format(self.username, self.password,
                                                                                  self.host, self.db_name), echo=False)
        self.connection = self.engine.connect()
        self.session = Session(self.engine)

    def get_engine(self):
        return self.engine

    def run_query(self, raw_query):
        results = self.connection.execute(sqlalchemy.text(raw_query))
        return results.fetchall()

    def run_statement(self, raw_query):
        results = self.connection.execute(sqlalchemy.text(raw_query))
        return results.lastrowid

    def create_base_with_session(self):
        base = automap_base()
        base.prepare(self.engine, reflect=True)
        self.session = Session(self.engine)
        return base, self.session

class Localhost(Database):
    def __init__(self, db_name):

        self.host = '127.0.0.1'
        self.port = 3306
        self.username = os.getenv('LOCAL_USER')
        self.password = os.getenv('LOCAL_PW')
        self.db_name = db_name
        self.engine = None
        self.connection = None
        self.session = None

    def __del__(self):
        if self.engine is not None:
            self.cleanUpDB()

    def cleanUpDB(self):
        if self.session:
            self.session.close()
        self.connection.close()
        self.engine.dispose()

    def create_connection(self):
        if self.username is None:
            raise EnvironmentError("database credentials are not set in ENV")

        try:
            self.engine = create_engine('mysql+pymysql://{0}:{1}@{2}:3306/{3}'.format(self.username, self.password,
                                                                                      self.host, self.db_name),
                                        echo=False)
            self.connection = self.engine.connect()
        except (InternalError, OperationalError) as e:
            if re.search('Unknown database', str(e)):
                self.engine = create_engine('mysql+pymysql://{0}:{1}@{2}:3306/'.format(self.username, self.password,
                                                                                       self.host), echo=False)
                self.connection = self.engine.connect()
            else:
                raise e

    def refresh_database(self):
        result = self.connection.execute("SHOW DATABASES;")
        existing_databases = result.fetchall()
        if (str(self.db_name)) in str(existing_databases):
            self.connection.execute("DROP DATABASE {}".format(self.db_name))
        self.connection.execute("CREATE DATABASE {};".format(self.db_name))
        self.connection.execute("USE {};".format(self.db_name))

    # TODO make this contextual based on caller and abstract

    def populate_db(self, sql_data_file_path='../common/_database_setup/vehicle_and_fleet_meta_data.sql',
                    disable_foreign_keys=False):
        fd = open(sql_data_file_path, 'r')
        sql_file = fd.read()
        fd.close()

        # disable foreign keys
        if disable_foreign_keys is True:
            remove_foreign_key_constraints = 'SET FOREIGN_KEY_CHECKS = 0;'
            self.connection.execute(remove_foreign_key_constraints)

        sql_commands = sql_file.split(";")[:-1]
        for command in sql_commands:
            try:
                self.connection.execute(command)
            except IntegrityError as err:
                print("attempting to execute {0} failed with {1}".format(command, err))

        # re-enable foreign keys once data is inserted.
        if disable_foreign_keys is True:
            remove_foreign_key_constraints = 'SET FOREIGN_KEY_CHECKS =1;'
            self.connection.execute(remove_foreign_key_constraints)

    def setupDb(self, sql_data_file_path=None, tables_file_path='../_database_setup/grace_production_schema.sql',
                disable_foreign_keys=False):
        self.create_connection()
        self.create_schema(tables_file_path)
        if sql_data_file_path is not None:
            self.populate_db(sql_data_file_path, disable_foreign_keys)

class LocalPG(Localhost):
    def __init__(self, db_name):
        self.host = '127.0.0.1'
        self.port = 5432
        self.username = os.getenv('PG_USER')
        self.password = os.getenv('PG_PW')
        self.db_name = db_name
        self.search_path = 'public'
        self.engine = None
        self.connection = None
        import psycopg2
        # creates a separate connection to the postgres database (a default postgres DB) so that we can connect to that
        # and execute drop and create database statements there, since you can't drop a DB you're currently connected to
        # in PostgreSQL
        self.postgres_engine, self.postgres_connection = self.create_postgres_connection()

    def create_connection(self):
        if self.username is None:
            raise EnvironmentError("database credentials are not set in ENV")
        try:
            self.engine = create_engine('postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(self.username, self.password,
                                                                                           self.host, self.port,
                                                                                           self.db_name), echo=False,
                                        isolation_level='AUTOCOMMIT')
            self.connection = self.engine.connect()
            self.run_statement('SET search_path = {0}'.format(self.search_path))
        except (InternalError, OperationalError) as e:
            if re.search('does not exist', str(e)):
                self.engine = create_engine(
                    'postgresql+psycopg2://{0}:{1}@{2}:{3}/'.format(self.username, self.password,
                                                                    self.host, self.port), echo=False,
                    isolation_level='AUTOCOMMIT')

                self.connection = self.engine.connect()
                self.run_statement('SET search_path = {0}'.format(self.search_path))
            else:
                raise e

    def refresh_database(self):
        """
        This method closes the original connection, then connects to the postgres database (a default psql DB) and
        runs the drop and create commands from there, then recreates a connection to the localpg DB.
        """

        self.connection.autocommit = True
        self.connection.execute("commit")
        result = self.connection.execute("SELECT datname FROM pg_database;")
        existing_databases = result.fetchall()
        if (str(self.db_name)) in str(existing_databases):
            self.postgres_connection.execute("revoke connect on database {} from public".format(self.db_name))
            self.postgres_connection.execute("SELECT pg_terminate_backend(pg_stat_activity.pid) FROM pg_stat_activity "
                                              "WHERE pg_stat_activity.datname = '{}'"
                                              "and pid <> pg_backend_pid();".format(self.db_name))
            self.postgres_connection.execute("drop database {}".format(self.db_name))
        self.postgres_connection.execute("CREATE DATABASE {};".format(self.db_name))
        self.connection = self.engine.connect()


    def create_postgres_connection(self):
        postgres_engine = create_engine('postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(self.username, self.password,
                                                                                       self.host, self.port,
                                                                                       'postgres'), echo=False,
                                                                                        isolation_level='AUTOCOMMIT')
        postgres_connection = postgres_engine.connect()
        return postgres_engine, postgres_connection

    def reset_primary_key_sequence(self, table_name, primary_key):
        """
        Sometimes, life gives you lemons in the form of your Postgres DB's primary key sequence not staying up to date
        if you inserted data via a CSV or something like that. It'll give a duplicate key error because it's trying to
        insert data at id=1 but there's already a row with id=1. This resets it to the max value.
        :param table_name: (str) the name of the table you want to reset the primary key sequence on
        :param primary_key: (str) the name of the primary key column
        """

        self.connection.execute("SELECT pg_catalog.setval(pg_get_serial_sequence('{0}', '{1}'), MAX({1})) FROM {0};".format(table_name, primary_key))

    def set_search_path(self, search_path='halo_connect_customer_data'):
        self.search_path = search_path