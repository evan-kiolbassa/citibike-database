import psycopg2
import csv
import pandas as pd
import multiprocessing as mp
from psycopg2.extras import execute_batch
import os, zipfile
import glob
import random
from sqlalchemy import create_engine


class PostgresPython:
    def __init__(self, root_dbname = 'postgres', user = None, password = None, localhost = None, child_db = None):
        self.root_dbname = root_dbname
        self.user = user
        self.password = password
        self.host = localhost
        self.dbname = child_db

    def sqlAlchemyConn(self):
        alchemyEngine = create_engine('postgresql+psycopg2://{0}:{1}!@localhost:{2}/{3}'.format(
            self.dbname, 
            self.password, 
            self.host, 
            self.dbname)
            )
        dbConnection = alchemyEngine.connect()
        return dbConnection

    def _cursor_instantiation(self):
        if self.dbname is not None:
            conn = psycopg2.connect('dbname = {} user = {} password = {}'.format(
                self.db_name,
                self.user,
                self.password
            ))
        else:
            conn = psycopg2.connect('dbname = {} user = {} password = {}'.format(
                self.root_dbname,
                self.user,
                self.password
            ))
        cur = conn.cursor()
        return cur, conn

    def _commitandClose(self, conn, cur):
        conn.commit()
        cur.close()
        print('Connection Closed')

    def database_creation(self, dbname):
        cur, conn = self._cursor_instantiation()
        conn.autocommit = True
        cur.execute(f"""
            DROP DATABASE IF EXISTS {dbname};
            CREATE DATABASE {dbname};
            """)
        conn.autocommit = False
        conn.close()

    def execute_transaction(self, statement):
        cur, conn = self._cursor_instantiation()
        cur.execute(statement)
        self._commitandClose(conn, cur)

