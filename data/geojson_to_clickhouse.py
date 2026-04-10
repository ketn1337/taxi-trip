import geopandas as gpd
import pandas as pd
import json
from clickhouse_connect import get_client
from shapely.ops import transform

# Функция для перестановки x и y (долготы и широты)
def reverse_coords(geom):
    if geom.is_empty:
        return geom
    # В shapely координаты обычно (x, y), мы меняем их на (y, x)
    return transform(lambda x, y, z=None: (y, x), geom)

def to_simple_list(geom):
    # 1. Берем первый полигон, если это MultiPolygon
    poly = geom.geoms[0] if geom.geom_type == 'MultiPolygon' else geom
    # 2. Берем только внешнюю границу (без дырок)
    # 3. Меняем (lon, lat) на (lat, lon) и превращаем в простой список списков
    return [[p[1], p[0]] for p in poly.exterior.coords]

client = get_client(host='localhost', port=8123, username='default', password='')

# Загрузка GeoJSON
file_path = 'data/NYC_taxi_zones.geojson'
gdf = gpd.read_file(file_path)

# 1. Сначала фильтруем пустые значения или Unknown зоны, 
# чтобы избежать ошибок "list index out of range" в будущем
gdf = gdf[~gdf['borough'].isin(['Unknown', ''])]
gdf = gdf[gdf['geometry'].notnull()]

# 2. Меняем широту и долготу местами в объектах геометрии
gdf['geometry'] = gdf['geometry'].apply(reverse_coords)

# Подготовка данных для ClickHouse
df = pd.DataFrame()
df['location_id'] = gdf['location_id'].astype(int)
df['zone_name'] = gdf['zone']
df['borough'] = gdf['borough']

# 3. Превращаем в JSON. Теперь там [latitude, longitude]
# Мы берем только массив координат из __geo_interface__
df['geometry_json'] = gdf['geometry'].apply(lambda x: json.dumps(to_simple_list(x)))

client.command('''
CREATE TABLE IF NOT EXISTS taxi_zones_geometry (
    location_id UInt16,
    zone_name String,
    borough String,
    geometry_json String
) ENGINE = MergeTree()
ORDER BY location_id
''')

client.command('''
    TRUNCATE TABLE taxi_zones_geometry
''')

client.insert('taxi_zones_geometry', df)