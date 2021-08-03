from dbutils.steady_db import SteadyDBConnection
import pymysql
import globals

from dbutils.persistent_db import PersistentDB

class DBPool(object):
    def __init__(self, database, **kwargs) -> None:
        self.persistent_db = PersistentDB(
            creator = pymysql, # the rest keyword arguments belong to pymysql
            host=globals.data_configure.db_host,
            port=globals.data_configure.db_port,
            user=globals.data_configure.db_user,
            password=globals.data_configure.db_pwd,
            database=database,
            charset='utf8mb4',
            **kwargs)
        return
    
    def get_conn(self):
        return self.persistent_db.connection()

    def get_cursor(self, conn:SteadyDBConnection=None):
        if conn is None:
            conn = self.get_conn()
        conn.ping(reconnect=True)
        cursor = conn.cursor()
        cursor.execute('set names utf8mb4')
        cursor.execute("SET CHARACTER SET utf8mb4")
        cursor.execute("SET character_set_connection=utf8mb4")
        conn.commit()
        return cursor

    def get_conn_and_cursor(self):
        conn = self.get_conn()
        return conn, self.get_cursor(conn)
