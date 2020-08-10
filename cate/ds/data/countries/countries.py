import json
import os.path

import fiona

DOWNLOAD_DIR = 'C:\\Users\\Norman\\Downloads\\'


def convert_all():
    # Downloded shapefiles from https://www.naturalearthdata.com/downloads/
    convert(DOWNLOAD_DIR + 'ne_110m_land')
    convert(DOWNLOAD_DIR + 'ne_50m_land')
    convert(DOWNLOAD_DIR + 'ne_10m_land')
    convert(DOWNLOAD_DIR + 'ne_110m_admin_0_countries')
    convert(DOWNLOAD_DIR + 'ne_50m_admin_0_countries')
    convert(DOWNLOAD_DIR + 'ne_10m_admin_0_countries')


def convert(input_path: str):
    filename = os.path.basename(input_path)
    fc = fiona.open(input_path)
    output_path = filename + '.geojson'
    print(f'Converting {input_path} to {output_path}')
    with open(output_path, 'w') as fp:
        fc_geojson = json.dumps([f for f in fc])
        geojson = '{"type": "FeatureCollection", "features": ' + fc_geojson + '}'
        fp.write(geojson)


if __name__ == '__main__':
    convert_all()
