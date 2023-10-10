import csv
import sys
from geojson import Feature, FeatureCollection, Point, LineString


def convert_csv_to_geojson(csv_source, geojson_dest):
    features = []
    with open(f'{csv_source}', newline='') as csvfile:
        reader = csv.reader(csvfile, delimiter=',')
        next(reader)
        for linestring_id, long1, lat1, long2, lat2, attr1, attr2, attr3, attr4, attr5, attr6 in reader:
            long1, lat1 = map(float, (long1, lat1))
            long2, lat2 = map(float, (long2, lat2))
            features.append(
                Feature(
                    geometry=LineString([(long1, lat1), (long2, lat2)]),
                    properties={
                        'linestring_id': int(linestring_id)
                    }
                )
            )

    collection = FeatureCollection(features)
    with open(f'{geojson_dest}', "w") as f:
        f.write('%s' % collection)


if __name__ == '__main__':
    convert_csv_to_geojson(sys.argv[1], sys.argv[2])
