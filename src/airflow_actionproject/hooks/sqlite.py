# -*- coding: utf-8 -*-
# ----------------------------------------------------------------------
# Copyright (c) 2021
#
# See the LICENSE file for details
# see the AUTHORS file for authors
# ----------------------------------------------------------------------

#--------------------
# System wide imports
# -------------------

from contextlib import closing

# ---------------
# Airflow imports
# ---------------

from airflow.providers.sqlite.hooks.sqlite import SqliteHook as BaseSqliteHook

#--------------
# local imports
# -------------

from airflow_actionproject import __version__

# -----------------------
# Module global variables
# -----------------------

# ----------------
# Module constants
# ----------------

class SqliteHook(BaseSqliteHook):
    
    # ----------
    # Public API
    # ----------

    @staticmethod
    def _generate_insert_sql(table, values, target_fields, replace, **kwargs):
        """
        Static helper method that generate the INSERT OR REPLACE SQL statement.

        :param table: Name of the target table
        :type table: str
        :param values: The row to insert into the table
        :type values: tuple of cell values
        :param target_fields: The names of the columns to fill in the table
        :type target_fields: iterable of strings
        :param replace: Whether to replace instead of insert
        :type replace: bool
        :return: The generated INSERT or INSERT OR REPLACE SQL statement
        :rtype: str
        """
        placeholders = [
            "?",
        ] * len(values)

        if target_fields:
            target_fields = ", ".join(target_fields)
            target_fields = f"({target_fields})"
        else:
            target_fields = ''

        if not replace:
            sql = "INSERT INTO "
        else:
            sql = "INSERT OR REPLACE INTO "
        sql += f"{table} {target_fields} VALUES ({','.join(placeholders)})"
        return sql


    @staticmethod
    def _generate_insert_sql2(table, target_fields, replace,  ignore=False, **kwargs):
        """
        Static helper method that generate the INSERT OR REPLACE/IGNORE SQL statement.

        :param table: Name of the target table
        :type table: str
        :param values: The row to insert into the table
        :type values: tuple of cell values
        :param target_fields: The names of the columns to fill in the table
        :type target_fields: iterable of strings
        :param replace: Whether to replace instead of insert
        :type replace: bool
        :return: The generated INSERT or INSERT OR REPLACE SQL statement
        :rtype: str
        """
        columns = ",".join(target_fields)
        values  = ",".join([f":{column}" for column in target_fields])
        
        if replace:
            sql = "INSERT OR REPLACE INTO "
        elif ignore:
            sql = "INSERT OR IGNORE INTO "
        else:
            sql = "INSERT INTO "
        
        sql += f"{table} ({columns}) VALUES ({values})"
        return sql

    # ----------
    # Public API
    # ----------
    
    def __enter__(self):
        '''Support for hook context manager'''
        self.get_conn()
        return self

    def __exit__(self, type, value, traceback):
        '''Support for hook context manager'''
        pass

  
    def get_conn(self):
        self.log.info(f"{self.__class__.__name__} version {__version__}")
        return super().get_conn()


    def insert_rows(self, table, rows, target_fields=None, commit_every=1000, replace=False, **kwargs):
        """
        A generic way to insert a set of tuples into a table,
        a new transaction is created every commit_every rows

        :param table: Name of the target table
        :type table: str
        :param rows: The rows to insert into the table
        :type rows: iterable of tuples
        :param target_fields: The names of the columns to fill in the table
        :type target_fields: iterable of strings
        :param commit_every: The maximum number of rows to insert in one
            transaction. Set to 0 to insert all rows in one transaction.
        :type commit_every: int
        :param replace: Whether to replace instead of insert
        :type replace: bool
        """
        i = 0
        with closing(self.get_conn()) as conn:
            if self.supports_autocommit:
                self.set_autocommit(conn, False)

            conn.commit()

            with closing(conn.cursor()) as cur:
                bucket_of_rows = []
                for i, row in enumerate(rows, 1):
                    values = tuple(self._serialize_cell(cell, conn) for cell in row)
                    sql = self._generate_insert_sql(table, values, target_fields, replace, **kwargs)
                    bucket_of_rows.append(values)
                    self.log.debug("Generated sql: %s", sql)
                    if commit_every and (i % commit_every) == 0:
                        cur.executemany(sql, bucket_of_rows) # The sql code is always the same, it does not depend on values
                        conn.commit()
                        bucket_of_rows = []
                        self.log.info("Saved %s rows into %s so far", i, table)
                if len(bucket_of_rows) > 0:
                    cur.executemany(sql, bucket_of_rows) # The sql code is always the same, it does not depend on values
            conn.commit()
        self.log.info("Done loading. Saved a total of %s rows", i)




    def insert_many(self, table, rows, commit_every=1000, replace=False, **kwargs):
        """
        A generic way to insert a set of tuples into a table,
        a new transaction is created every commit_every rows

        :param table: Name of the target table
        :type table: str
        :param rows: The rows to insert into the table
        :type rows: iterable of dictionaries with each column name and a value
        :param target_fields: The names of the columns to fill in the table
        :type target_fields: iterable of strings
        :param commit_every: The maximum number of rows to insert in one
            transaction. Set to 0 to insert all rows in one transaction.
        :type commit_every: int
        :param replace: Whether to replace instead of insert
        :type replace: bool
        """
        method = getattr(rows,"keys", None)
        if callable(method):
            rows = [rows]
        N = len(rows)
        if N == 0:
            self.log.info("Empty parameters list. Not writting to SQLite")
            return
        sql = self._generate_insert_sql2(table, rows[0].keys(), replace, **kwargs)
        self._run_many(sql, rows, commit_every)


    def run_many(self, sql, parameters, commit_every=1000, **kwargs):
        method = getattr(parameters,"keys", None)
        if callable(method):
            rows = [rows]
        N = len(parameters)
        if N == 0:
            self.log.info("Empty parameters list. Not writting to SQLite")
            return
        self._run_many(sql, parameters, commit_every)


    def _run_many(self, sql, rows, commit_every, **kwargs):
        N = len(rows)
        slices     = N // commit_every
        slices_rem = N %  commit_every
        if slices_rem:
            self.log.info(f"Will execute sql statement below with {N} rows looping {slices} times,  {commit_every} rows at a time time and a final reminder of of {slices_rem} rows")
        else:
            self.log.info(f"Will execute sql statement below with {N} rows looping {slices} times, {commit_every} rows at a time")
        self.log.info(f"{sql}")
        with closing(self.get_conn()) as conn:
            if self.supports_autocommit:
                self.set_autocommit(conn, False)
            conn.commit()
            with closing(conn.cursor()) as cur:
                rows = tuple({key: self._serialize_cell(value, conn) for key,value in row.items()} for row in rows)     
                for i in range(N):
                    cur.executemany(sql, rows[:commit_every])
                    conn.commit()
                    rows = rows[commit_every:]
                if slices_rem: 
                    cur.executemany(sql, rows)
                    conn.commit()
