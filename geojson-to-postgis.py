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
from tzwhere import tzwhere


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


class RoutingBuilder:

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

        # use a spheroid model(ellipsoid) for greater distance accuracy, use_spheroid = false
        self.UPDATE_COST = f'UPDATE {self.table_name} ' \
                           f'SET cost = ST_length(geom::geography, false), ' \
                           f'reverse_cost = ST_length(geom::geography, false);'

        self.CREATE_TOPOLOGY = f"SELECT pgr_createTopology('{self.table_name}', 0.00001, 'geom');"

        self.COUNT_CONNECTIVITY_COMPONENTS = f"SELECT COUNT(DISTINCT component) FROM pgr_connectedComponents(" \
                                             f"'SELECT id, source, target, cost, reverse_cost FROM {self.table_name}')"

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

    def check_graph_connectivity(self, con):
        with con:
            with con.cursor() as cursor:
                cursor.execute(self.COUNT_CONNECTIVITY_COMPONENTS)
                connected = cursor.fetchone()[0]
                if 1 == connected:
                    print("ALL EDGES CONNECTED IN 1 COMPONENT")
                else:
                    print("EDGES ARE CONNECTED IN MANY COMPONENTS")
                return connected

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
            self.check_graph_connectivity(con)

        except Exception as e:
            print(e)
        finally:
            con.close()


def normalize_coordinate(lon, lat):
    lon = lon % 360
    lat = lat % 360
    lon = -180 + (lon - 180) if lon > 180 else 180 + (lon + 180) if lon < -180 else lon
    lat = -180 + (lat - 180) if lat > 180 else 180 + (lat + 180) if lat < -180 else lat
    return lon, lat


class RoutingFinder:
    def __init__(self):
        self.FIND_NEAREST_COORDINATE = "SELECT id, ST_X(the_geom), ST_Y(the_geom), " \
                                       "ST_Distance(the_geom, 'SRID=4326;POINT(%s %s)'::geometry) as d " \
                                       "FROM maritime_routes_edges_vertices_pgr ORDER BY d limit 1;"
        self.ROUTING_ONE_TO_ONE = "SELECT * FROM pgr_Dijkstra('select id, source, target, cost, reverse_cost " \
                                  "FROM maritime_routes_edges', %s, %s, false);"

    def find_nearest_coordinate(self, con, lon, lat):
        with con:
            with con.cursor() as cursor:
                n_lon, n_lat = normalize_coordinate(lon, lat)
                cursor.execute(self.FIND_NEAREST_COORDINATE, (n_lon, n_lat))
                v_id, v_lon, v_lat, v_distance = cursor.fetchone()[0:2]
                tz = tzwhere.tzwhere()
                timezone_str = tz.tzNameAt(v_lat, v_lon)  # Seville coordinates

                data = {'is_valid': v_distance == 0,
                        'suggestion': [v_lon, v_lat],
                        'normalized': [n_lon, n_lat],
                        'moved_by': v_distance,
                        'validatedLatlng': {"lat": v_lat, "lng": v_lon},
                        # real ECA should be retrieved
                        'eca': False, 'eca_name': 'No ECA',
                        'timezone_of_validated': timezone_str}
                return json.dumps(data)

    def do_one_to_one_routing(self, con, v1, v2):
        with con:
            with con.cursor() as cursor:
                cursor.execute(self.ROUTING_ONE_TO_ONE, (v1, v2))

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
    routing_bldr = RoutingBuilder()
    routing_bldr.run_flow()

