# py_pg_exporter - A PostgreSQL Metrics Exporter for Prometheus
#
# Copyright (C) 2023 Sergey Tuchkin
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

from __future__ import annotations
import os
import sys
import psycopg2
import psycopg2.extras
import time
from prometheus_client import Gauge, Summary, start_http_server
import datetime
from urllib.parse import urlparse


def get_queries() -> dict[str,str]:
    return {
        "pg_stat_database_query": """
            SELECT datname,
                numbackends,
                xact_commit,
                xact_rollback,
                blks_read,
                blks_hit,
                tup_returned,
                tup_fetched,
                tup_inserted,
                tup_updated,
                tup_deleted,
                conflicts,
                temp_files,
                temp_bytes,
                deadlocks,
                blk_read_time,
                blk_write_time,
                stats_reset,
                pg_database_size(datname)
            FROM pg_stat_database
        """,
        "pg_stat_user_tables_query": """
            SELECT
                current_database() datname,
                schemaname,
                relname,
                seq_scan,
                seq_tup_read,
                idx_scan,
                idx_tup_fetch,
                n_tup_ins,
                n_tup_upd,
                n_tup_del,
                n_tup_hot_upd,
                n_live_tup,
                n_dead_tup,
                n_mod_since_analyze,
                COALESCE(last_vacuum, '1970-01-01Z') as last_vacuum,
                COALESCE(last_autovacuum, '1970-01-01Z') as last_autovacuum,
                COALESCE(last_analyze, '1970-01-01Z') as last_analyze,
                COALESCE(last_autoanalyze, '1970-01-01Z') as last_autoanalyze,
                vacuum_count,
                autovacuum_count,
                analyze_count,
                autoanalyze_count
            FROM
                pg_stat_user_tables
        """,
        "pg_class_query": """
            SELECT
                current_database() datname,
                relname,
                reltuples,
                relpages,
                reltoastrelid,
                reltablespace
            FROM pg_class
            WHERE relkind = 'r'
        """
    }

def get_gauges() -> dict[str,dict]:
    gauges_dict = {}

    # Initialize pg_stat_database Gauges
    gauge_labels = ['datname']
    gauges_dict['pg_stat_database_gauges'] = {
        'numbackends': Gauge('pg_stat_database_numbackends', 'Number of backends currently connected to this database', gauge_labels),
        'xact_commit': Gauge('pg_stat_database_xact_commit', 'Number of transactions in this database that have been committed', gauge_labels),
        'xact_rollback': Gauge('pg_stat_database_xact_rollback', 'Number of transactions in this database that have been rolled back', gauge_labels),
        'blks_read': Gauge('pg_stat_database_blks_read', 'Number of disk blocks read in this database', gauge_labels),
        'blks_hit': Gauge('pg_stat_database_blks_hit', 'Number of times disk blocks were found already in the buffer cache, so that a read was not necessary (this only includes hits in the PostgreSQL buffer cache, not the operating system’s file system cache)', gauge_labels),
        'tup_returned': Gauge('pg_stat_database_tup_returned', 'Number of rows returned by queries in this database', gauge_labels),
        'tup_fetched': Gauge('pg_stat_database_tup_fetched', 'Number of rows fetched by queries in this database', gauge_labels),
        'tup_inserted': Gauge('pg_stat_database_tup_inserted', 'Number of rows inserted by queries in this database', gauge_labels),
        'tup_updated': Gauge('pg_stat_database_tup_updated', 'Number of rows updated by queries in this database', gauge_labels),
        'tup_deleted': Gauge('pg_stat_database_tup_deleted', 'Number of rows deleted by queries in this database', gauge_labels),
        'conflicts': Gauge('pg_stat_database_conflicts', 'Number of queries canceled due to conflicts with recovery in this database', gauge_labels),
        'temp_files': Gauge('pg_stat_database_temp_files', 'Number of temporary files created by queries in this database', gauge_labels),
        'temp_bytes': Gauge('pg_stat_database_temp_bytes', 'Total amount of data written to temporary files by queries in this database', gauge_labels),
        'deadlocks': Gauge('pg_stat_database_deadlocks', 'Number of deadlocks detected in this database', gauge_labels),
        'blk_read_time': Gauge('pg_stat_database_blk_read_time', 'Time spent reading data file blocks by backends in this database, in milliseconds', gauge_labels),
        'blk_write_time': Gauge('pg_stat_database_blk_write_time', 'Time spent writing data file blocks by backends in this database, in milliseconds', gauge_labels),
        'stats_reset': Gauge('pg_stat_database_stats_reset', 'Time at which these statistics were last reset', gauge_labels),
        'pg_database_size': Gauge('pg_database_size_bytes', 'Size of the database in bytes', gauge_labels),
    }

    # Initialize pg_settings Gauges
    gauge_labels = ['datname', 'relname']
    gauges_dict['per_db_gauges'] = {
        'seq_scan': Gauge('pg_stat_user_tables_seq_scan', 'Number of sequential scans initiated on this table', gauge_labels),
        'seq_tup_read': Gauge('pg_stat_user_tables_seq_tup_read', 'Number of live rows fetched by sequential scans', gauge_labels),
        'idx_scan': Gauge('pg_stat_user_tables_idx_scan', 'Number of index scans initiated on this table', gauge_labels),
        'idx_tup_fetch': Gauge('pg_stat_user_tables_idx_tup_fetch', 'Number of live rows fetched by index scans', gauge_labels),
        'n_tup_ins': Gauge('pg_stat_user_tables_n_tup_ins', 'Number of rows inserted', gauge_labels),
        'n_tup_upd': Gauge('pg_stat_user_tables_n_tup_upd', 'Number of rows updated', gauge_labels),
        'n_tup_del': Gauge('pg_stat_user_tables_n_tup_del', 'Number of rows deleted', gauge_labels),
        'n_tup_hot_upd': Gauge('pg_stat_user_tables_n_tup_hot_upd', 'Number of rows HOT updated', gauge_labels),
        'n_live_tup': Gauge('pg_stat_user_tables_n_live_tup', 'Estimated number of live rows', gauge_labels),
        'n_dead_tup': Gauge('pg_stat_user_tables_n_dead_tup', 'Estimated number of dead rows', gauge_labels),
        'n_mod_since_analyze': Gauge('pg_stat_user_tables_n_mod_since_analyze', 'Estimated number of rows modified since last analyzed', gauge_labels),
        'last_vacuum': Gauge('pg_stat_user_tables_last_vacuum', 'Last time at which this table was manually vacuumed', gauge_labels),
        'last_autovacuum': Gauge('pg_stat_user_tables_last_autovacuum', 'Last time at which this table was vacuumed by the autovacuum daemon', gauge_labels),
        'last_analyze': Gauge('pg_stat_user_tables_last_analyze', 'Last time at which this table was manually analyzed', gauge_labels),
        'last_autoanalyze': Gauge('pg_stat_user_tables_last_autoanalyze', 'Last time at which this table was analyzed by the autovacuum daemon', gauge_labels),
        'vacuum_count': Gauge('pg_stat_user_tables_vacuum_count', 'Number of times this table has been manually vacuumed', gauge_labels),
        'autovacuum_count': Gauge('pg_stat_user_tables_autovacuum_count', 'Number of times this table has been vacuumed by the autovacuum daemon', gauge_labels),
        'analyze_count': Gauge('pg_stat_user_tables_analyze_count', 'Number of times this table has been manually analyzed', gauge_labels),
        'autoanalyze_count': Gauge('pg_stat_user_tables_autoanalyze_count', 'Number of times this table has been analyzed by the autovacuum daemon', gauge_labels),
        'reltuples': Gauge('pg_class_reltuples', 'Number of tuples in the table', gauge_labels),
        'relpages': Gauge('pg_class_relpages', 'Number of pages in the table', gauge_labels),
        'reltoastrelid': Gauge('pg_class_reltoastrelid', "Toast table's OID", gauge_labels),
        'reltablespace': Gauge('pg_class_reltablespace', 'Tablespace OID', gauge_labels),
    }

    # set up pg_up gauge
    gauges_dict['pg_up'] = {
        'pg_up': Gauge('pg_up', 'PostgreSQL server status (1 for up, 0 for down)')
    }

    return gauges_dict


def parse_datasource(url: str) -> dict[str, str | int]:
    """
    parse url in the format:
    `postgres://postgres:password@localhost:5432/postgres`
    into the arguments of psycopg2.connect() function, e.g.
    psycopg2.connect(**parse_datasource(url))

    :param url: database URL in the format: postgres://postgres:password@localhost:5432/postgres
    :return: dictionary with host, port, user, password and dbname keys
    """

    parsed_url = urlparse(url)

    # Extract connection components
    dbname = parsed_url.path[1:]  # Remove the leading '/'
    if not dbname:
        dbname = "postgres"
    user = parsed_url.username
    password = parsed_url.password
    host = parsed_url.hostname
    port = parsed_url.port

    # check if url path is correct
    if parsed_url.scheme not in ('postgres', 'postgresql'):
        raise ValueError(f"The {url} URL scheme must be 'postgres' or 'postgresql'.")
    if not host or not port or not user or not password or not dbname:
        raise ValueError(f"The {url} URL is not valid. It must include hostname, port, username, password, and database.")

    # return the dictionary
    return dict(
        dbname=dbname,
        user=user,
        password=password,
        host=host,
        port=port
    )

def update_common_metrics(queries: dict[str,str], gauges: dict[str,dict], dblist: list | None = None) -> None:
    """
    update metrics (gauges) that correspond to instance-wide metrics

    :param queries: queries to execute
    :param gauges: gauges to update
    :param dblist: list of databases, defaults to None
    """

    query_args = None
    pg_stat_database_query = queries['pg_stat_database_query']
    if dblist:
        print(f"{dblist=}", file=sys.stderr)
        pg_stat_database_query += " WHERE datname = ANY(%s);"
        query_args = (dblist,)
    try:
        with  psycopg2.connect(**datasource) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
                cursor.execute(pg_stat_database_query, query_args)
                rows = cursor.fetchall()
    except IOError:
        gauges['pg_up']['pg_up'].set(0)
        return None
    gauges['pg_up']['pg_up'].set(1)
    for row in rows:
        datname = row['datname']
        for column in gauges['pg_stat_database_gauges'].keys():
            value = row[column]
            if isinstance(value, datetime.datetime):
                value = value.timestamp()
            gauges['pg_stat_database_gauges'][column].labels(datname=datname).set(value)

def update_per_db_metrics(queries: dict[str,str], gauges: dict[str,dict], dblist: list | None = None) -> None:
    """
    update metrics (gauges) that correspond to database-specific metrics

    :param queries: queries to execute
    :param gauges: gauges to update
    :param dblist: list of databases, defaults to None
    """

    if not dblist:
        return
    for db in dblist:
        print("connecting to ", db, file=sys.stderr)
        try:
            with psycopg2.connect(
                host=datasource['host'],
                port=datasource['port'],
                dbname=db,
                user=datasource['user'],
                password=datasource['password']
            ) as per_db_conn:
                with per_db_conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
                    cursor.execute(queries['pg_stat_user_tables_query'])
                    rows = cursor.fetchall()
                    cursor.execute(queries['pg_class_query'])
                    rows += cursor.fetchall()
        except IOError:
            gauges['pg_up']['pg_up'].set(0)
            return None
        for row in rows:
            datname = row['datname']
            relname = row['relname']
            for column in gauges['per_db_gauges'].keys():
                if column in row.keys():
                    value = row[column]
                    if isinstance(value, datetime.datetime):
                        value = value.timestamp()
                    if not value is None:
                        gauges['per_db_gauges'][column].labels(datname=datname, relname=relname).set(value)


# set up metrics_collection_time Summary metric
metrics_collection_time = Summary('metrics_collection_time_seconds', 'Time taken to collect all metrics')

@metrics_collection_time.time()
def update_metrics(queries: dict[str,str], gauges: dict[str,dict], dblist: list | None = None) -> None:
    """
    update metrics (gauges) that correspond to both instance-wide and database-specific metrics

    :param queries: queries to execute
    :param gauges: gauges to update
    :param dblist: list of databases, defaults to None
    """

    update_common_metrics(queries, gauges, dblist)
    update_per_db_metrics(queries, gauges, dblist)

if __name__ == '__main__':
    start_http_server(8000)

    dblist = list(filter(lambda x: x.strip(), os.environ.get('INCLUDE_DATABASES', '').split()))
    time_int_seconds = int(os.environ.get('PG_PYEXPORTER_TIMEINT', "60"))
    datasource_url = os.environ.get('DATA_SOURCE_NAME')
    if datasource_url:
        datasource = parse_datasource(datasource_url)
    else:
        raise ValueError(f"DATA_SOURCE_NAME environment variable is not set or empty.")

    dblist = list(filter(lambda x: x.strip(), os.environ.get('INCLUDE_DATABASES', '').split(',')))
    queries = get_queries()
    gauges = get_gauges()
    start_time = time.perf_counter()
    while True:
        t1 = time.perf_counter()
        update_metrics(queries, gauges, dblist)
        t2 = time.perf_counter()
        print(f"time spent: {t2-t1:.2f}s", file=sys.stderr)
        time_elapsed = t2 - start_time
        seconds_to_sleep = time_int_seconds - time_elapsed % time_int_seconds
        time.sleep(seconds_to_sleep)
