import geopandas as gpd
import folium
import matplotlib.pyplot as plt
from DaskDB.Context import Context, DASK_SCHEDULER_IP, DASK_SCHEDULER_PORT
import os

c = Context()
c.setup_configuration(daskSchedulerIP=DASK_SCHEDULER_IP, daskSchedulerPort=DASK_SCHEDULER_PORT)

# Register the dataset first
shapefile_path = "../data/geonb_pan-ncb_shp/geonb_pan_ncb.shp"
c.register_table("property_assessment_map", shapefile_path)

# THEN init the schema
c.initSchema()

# ------------------------------
# Run Query
# ------------------------------
sql = "SELECT ST_BOUNDARY(geometry) FROM property_assessment_map WHERE Descript = 'RESIDENCE & LOT'"

print("Running Query...")
res = c.query(sql)

# Convert to GeoDataFrame
print("Converting to GeoDataFrame...")
gdf = gpd.GeoDataFrame(geometry=res)

# Static Plot
print("Plotting...")
gdf.plot()
plt.title("Spatial Query Result")
plt.xlabel("Longitude")
plt.ylabel("Latitude")
plt.savefig("query_result.png", dpi=300)
plt.show()

# Export as GeoJSON
print("Exporting to GeoJSON...")
gdf.to_file("output.geojson", driver="GeoJSON")

# Interactive Map with Folium
print("Generating Interactive Map...")
centroid = gdf.unary_union.centroid
m = folium.Map(location=[centroid.y, centroid.x], zoom_start=10)
folium.GeoJson(gdf).add_to(m)
m.save("query_result_map.html")

print("All done! Map saved as 'query_result_map.html'")
