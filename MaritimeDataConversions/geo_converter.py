import csv
import sys
import pandas as pd
from geojson import Feature, FeatureCollection, Point, LineString


class GeoConverter:
    def __init__(self, source_folder, file_name):
        self.source_folder = source_folder
        self.file_name = file_name

    def convert_xlsx_to_csv(self, sheet_name):
        xlsx_src = f"{self.source_folder}\\{self.file_name}.xlsx"
        csv_dest = f"{self.source_folder}\\{self.file_name}.csv"
        df = pd.read_excel(xlsx_src, sheet_name=sheet_name, header=0)
        df.to_csv(csv_dest, index=False)

    def convert_csv_to_geojson(self):
        geojson_dest = f"{self.source_folder}\\{self.file_name}.geojson"
        csv_src = f"{self.source_folder}\\{self.file_name}.csv"

        features = []
        with open(f'{csv_src}', newline='') as csvfile:
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


# sys.argv[1], sheet_name=sys.argv[2]

if __name__ == '__main__':
    gc = GeoConverter(sys.argv[1], sys.argv[2])
    gc.convert_xlsx_to_csv(sys.argv[3])
    gc.convert_csv_to_geojson()
