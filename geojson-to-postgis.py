#!/usr/bin/env python
import operator
import psycopg2
import psycopg2.extras
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import io
import json
import sys
import logging
import configparser


config = configparser.ConfigParser()
config.read('config.ini')


'''
CREATE EXTENSION postgis;
CREATE EXTENSION pgrouting;
CREATE TABLE table_name (
    id integer not null,
    geom geometry(geometry, 4326),
    properties hstore
);
'''


def connect_to_dbms():
    return psycopg2.connect(
                            dbname=config['PostGIS.LogIn']['RoutingDB'],
                            user=config['PostGIS.LogIn']['User'],
                            password=config['PostGIS.LogIn']['Pass'],
                            host=config['PostGIS.LogIn']['Host'],
                            port=int(config['PostGIS.LogIn']['Port']))


# def check_db_exists(cursor, drop_db=False):
#     db_name = config['PostGIS.LogIn']['RoutingDB']
#     if drop_db:
#         cursor.execute(f'DROP DATABASE IF EXISTS {db_name}')
#     cursor.execute(f"SELECT 1 FROM pg_catalog.pg_database WHERE datname = '{db_name}'")
#     exists = cursor.fetchone()
#     return exists
#
#
# def create_db(cursor):
#     db_name = config['PostGIS.LogIn']['RoutingDB']
#     cursor.execute(f'CREATE DATABASE {db_name}')
#     cursor.execute(f'CREATE EXTENSION postgis')
#     cursor.execute(f'CREATE EXTENSION pgrouting;')


class RoutingManager:

    def __init__(self):

        self.table_name = config['PostGIS.Tables']['Routes']

        self.TABLE_CHECK_EXISTS = f"SELECT EXISTS(SELECT * FROM information_schema.tables " \
                                  f"WHERE table_name='{self.table_name}')"

        self.CREATE_EDGES_TABLE = f'CREATE TABLE {self.table_name} (id integer not null, ' \
                                  f'source integer, target integer, ' \
                                  f'cost double precision, reverse_cost double precision, ' \
                                  f'geom geometry(geometry, 4326));'

        self.ROUTING_INSERT_STATEMENT = f'INSERT INTO {self.table_name} (id, geom) ' \
                                        f'VALUES (%s, ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326));'

        self.UPDATE_COST = f'UPDATE {self.table_name} ' \
                           f'SET cost = ST_length(geom) * 2, reverse_cost = ST_length(geom) * 2;'

        self.CREATE_TOPOLOGY = f"SELECT pgr_createTopology('{self.table_name}', 0.00001, 'geom');"

    def truncate_routing_edges_tbl_if_exists(self, con):
        with con:
            with con.cursor() as cursor:
                cursor.execute(self.TABLE_CHECK_EXISTS)
                tbl_exists = cursor.fetchone()[0]
                # if tbl exists truncate table
                if tbl_exists:
                    cursor.execute(f'DROP TABLE {self.table_name};')
            con.commit()

    def create_routing_edges_tbl(self, con):
        with con:
            with con.cursor() as cursor:
                cursor.execute(self.CREATE_EDGES_TABLE)
            con.commit()

    def _import_routing_feature_from_json(self, cursor, feature_data):
        if feature_data.get('type') == 'FeatureCollection':
            for feature in feature_data['features']:
                self._import_routing_feature_from_json(cursor, feature)
        elif feature_data.get('type') == 'Feature':
            geojson = json.dumps(feature_data['geometry'])
            # separate field to each property
            linestring_id = int(feature_data['properties']["linestring_id"])
            cursor.execute(self.ROUTING_INSERT_STATEMENT, (linestring_id, geojson))

    def import_routing_geometry_into_edges_table(self, con, feature_data):
        with con:
            with con.cursor() as cursor:
                self._import_routing_feature_from_json(cursor, feature_data)
            con.commit()

    def set_cost_into_edges_table(self, con):
        with con:
            with con.cursor() as cursor:
                cursor.execute(self.UPDATE_COST)
            con.commit()

    def create_topology(self, con):
        with con:
            with con.cursor() as cursor:
                cursor.execute(self.CREATE_TOPOLOGY)
            con.commit()

    def run_flow(self):
        con = connect_to_dbms()
        try:
            self.truncate_routing_edges_tbl_if_exists(con)
            self.create_routing_edges_tbl(con)
            handle = io.open(config['GeoJSON.MaritimeRoutes']['Source'], 'r')
            with handle:
                data = json.load(handle)
                self.import_routing_geometry_into_edges_table(con, data)
            self.set_cost_into_edges_table(con)
            self.create_topology(con)

        except Exception as e:
            print(e)
        finally:
            con.close()


# def create_ports_tbl():
#     pass
#
# def create_eca_tbl():
#     pass
#
# def create_hra_tbl():
#     pass
#
# def create_jwc_tbl():
#     pass


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    routing_mngr = RoutingManager()
    routing_mngr.run_flow()

