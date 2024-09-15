# mwhs-obis-species

## Site shapes data preparation

```bash
python world_h3.py
ogr2ogr -wrapdateline -makevalid data/world_h3_wrap_valid.gpkg data/world_h3.gpkg
ogr2ogr -explodecollections data/world_h3_wrap_valid_explode.gpkg data/world_h3_wrap_valid.gpkg
```

```bash
ogr2ogr data/marine_world_heritage_buffer.shp -dialect SQLITE -sql "select name as site, st_buffer(geometry, 1) as geometry from marine_world_heritage" data/marine_world_heritage.shp
ogr2ogr data/marine_world_heritage_wrap.shp -wrapdateline data/marine_world_heritage_buffer.shp
ogr2ogr data/marine_world_heritage_union.shp -dialect SQLITE -sql "select site, st_union(geometry) as geometry from marine_world_heritage_wrap group by site" data/marine_world_heritage_wrap.shp
```

### Deprecated

```bash
ogr2ogr -f GeoJSON data/marine_world_heritage_union.geojson data/marine_world_heritage_union.shp
```

```bash
ogr2ogr data/output.shp layers.vrt -dialect sqlite -sql "select marine_world_heritage_union.site, world_h3.h3, world_h3.geom from world_h3, marine_world_heritage_union where st_intersects(marine_world_heritage_union.geometry, world_h3.geom)" -nln sites
```

```bash
ogr2ogr cells.csv data/output.shp -dialect sqlite -sql "select site, h3 from output group by site, h3"
tail -n +2 cells.csv > temp.txt && mv temp.txt cells.csv
```

## Create lists

Run `lists.ipynb`

## Upload

TO DO