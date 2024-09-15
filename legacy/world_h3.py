import h3
import pandas as pd
import h3pandas


resolution = 5
res0_cells = h3.get_res0_indexes()

cells = set()

for cell in res0_cells:
    print(cell)
    children = h3.h3_to_children(cell, res=resolution)
    cells.update(children)


df = pd.DataFrame({"h3": list(cells)}).set_index("h3", drop=False)
gdf = df.h3.h3_to_geo_boundary()
gdf.to_file("data/world_h3.gpkg", driver="GPKG", index=False)
