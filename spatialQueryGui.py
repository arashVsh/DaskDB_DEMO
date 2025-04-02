# # spatialQueryGui.py - with overlay and SQL generator

# import geopandas as gpd
# import folium
# from folium import plugins
# import json
# from shapely.geometry import shape, box
# import matplotlib.pyplot as plt
# from DaskDB.Context import Context, DASK_SCHEDULER_IP, DASK_SCHEDULER_PORT

# # Initialize Context
# c = Context()
# c.setup_configuration(daskSchedulerIP=DASK_SCHEDULER_IP, daskSchedulerPort=DASK_SCHEDULER_PORT)
# c.register_table("property_assessment_map", "../data/geonb_pan-ncb_shp/geonb_pan_ncb.shp")
# c.initSchema()

# # Query all data to get bounds
# df = c.query("SELECT * FROM property_assessment_map")
# gdf = gpd.GeoDataFrame(df, geometry='geometry')
# gdf.set_crs("EPSG:2953", inplace=True)  # Replace with your actual CRS

# # Get bounding box of the dataset
# bounds = gdf.total_bounds  # [minx, miny, maxx, maxy]
# center = [(bounds[1] + bounds[3]) / 2, (bounds[0] + bounds[2]) / 2]  # [lat, lon]

# # Create folium map focused on dataset
# m = folium.Map(location=center, zoom_start=10, max_bounds=True)
# m.fit_bounds([[bounds[1], bounds[0]], [bounds[3], bounds[2]]])

# # Draw the dataset boundary visibly
# bbox_geom = box(*bounds)
# bbox_gdf = gpd.GeoDataFrame(geometry=[bbox_geom], crs=gdf.crs).to_crs(epsg=4326)
# folium.GeoJson(bbox_gdf, name="Dataset Boundary").add_to(m)

# # Add drawing tool
# draw = plugins.Draw(export=True)
# draw.add_to(m)

# # Save the map
# m.save("draw_polygon_map.html")
# print("✅ Map saved as 'draw_polygon_map.html'. Please draw inside the dataset boundary and export the polygon.")

# # Automatically open the map in the default browser
# import webbrowser
# webbrowser.open("draw_polygon_map.html")

# # Load exported polygon
# try:
#     with open("user_polygon.geojson", 'r') as f:
#         geojson = json.load(f)
#         user_polygon = shape(geojson['features'][0]['geometry'])
# except Exception as e:
#     print("⚠️ Please export and save the drawn polygon as 'user_polygon.geojson'")
#     exit()

# # Check if polygon is inside dataset boundary
# bbox_geom_gdf = gpd.GeoDataFrame(geometry=[bbox_geom], crs=gdf.crs).to_crs(epsg=4326)
# bbox_geom = bbox_geom_gdf.iloc[0].geometry

# if not user_polygon.intersects(bbox_geom):
#     raise ValueError("🚫 The drawn polygon is outside the dataset boundary.")

# # Reproject the polygon to dataset CRS
# user_polygon_gdf = gpd.GeoDataFrame(geometry=[user_polygon], crs="EPSG:4326").to_crs(gdf.crs)

# # Spatial Query
# filtered = gdf[gdf.geometry.within(user_polygon_gdf.iloc[0].geometry)]

# # Generate SQL equivalent (for report/demo)
# print("\nSuggested SQL Query:")
# print("SELECT * FROM property_assessment_map WHERE ST_WITHIN(geometry, user_polygon);")

# # Plot with overlay
# if filtered.empty:
#     print("⚠️ No geometries found within the selected region.")
# else:
#     ax = filtered.plot(color="lightblue", edgecolor="k")
#     user_polygon_gdf.boundary.plot(ax=ax, color="red", linewidth=2)
#     plt.title("Spatial Query Result with Query Area Overlay")
#     plt.savefig("query_result_overlay.png", dpi=300)
#     plt.show()

#     # Export results
#     filtered.to_file("query_result_selected_region.geojson", driver="GeoJSON")
#     print("✅ Query done! Plot and GeoJSON saved.")



# spatialQueryGui_pure.py - Fully in Python with interactive polygon drawing

import sys
import os
import json
import geopandas as gpd
import matplotlib.pyplot as plt
from shapely.geometry import Polygon, box
from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
from PyQt5.QtWidgets import QApplication, QWidget, QVBoxLayout, QPushButton, QMessageBox

from DaskDB.Context import Context

# ----------------------------
# Load Dataset
# ----------------------------
c = Context()
c.register_table("property_assessment_map", "../data/geonb_pan-ncb_shp/geonb_pan_ncb.shp")
c.initSchema()
df = c.query("SELECT * FROM property_assessment_map")
gdf = gpd.GeoDataFrame(df, geometry='geometry')
gdf.set_crs("EPSG:2953", inplace=True)
bbox_geom = box(*gdf.total_bounds)
bbox_gdf = gpd.GeoDataFrame(geometry=[bbox_geom], crs=gdf.crs)

# ----------------------------
# GUI
# ----------------------------
class SpatialQueryApp(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Spatial Query GUI - Pure Python")
        self.setGeometry(100, 100, 800, 600)

        self.polygon_points = []

        layout = QVBoxLayout()

        self.fig, self.ax = plt.subplots()
        self.canvas = FigureCanvas(self.fig)
        layout.addWidget(self.canvas)

        self.btn_run = QPushButton("Run Query with Drawn Polygon")
        self.btn_run.clicked.connect(self.run_query)
        layout.addWidget(self.btn_run)

        self.setLayout(layout)

        self.plot_dataset()

        self.cid = self.canvas.mpl_connect('button_press_event', self.onclick)

    def plot_dataset(self):
        self.ax.clear()
        gdf.plot(ax=self.ax, color="lightgrey", edgecolor="k")
        bbox_gdf.boundary.plot(ax=self.ax, color="red")
        self.ax.set_title("Left-click to define polygon, Right-click to finish")
        self.canvas.draw()

    def onclick(self, event):
        if event.inaxes != self.ax:
            return
        if event.button == 1:  # Left click
            self.polygon_points.append((event.xdata, event.ydata))
            self.ax.plot(event.xdata, event.ydata, 'bo')
            self.canvas.draw()
        elif event.button == 3:  # Right click to close polygon
            if len(self.polygon_points) < 3:
                QMessageBox.warning(self, "Invalid", "Polygon needs at least 3 points.")
                return
            self.ax.plot(*zip(*self.polygon_points, self.polygon_points[0]), color="blue")
            self.canvas.draw()

    def run_query(self):
        if len(self.polygon_points) < 3:
            QMessageBox.warning(self, "Invalid", "Draw a polygon first.")
            return

        user_polygon = Polygon(self.polygon_points)
        user_polygon_gdf = gpd.GeoDataFrame(geometry=[user_polygon], crs=gdf.crs)

        if not user_polygon.intersects(bbox_geom):
            QMessageBox.warning(self, "Invalid", "Polygon is outside dataset boundary.")
            return

        filtered = gdf[gdf.geometry.within(user_polygon)]

        if filtered.empty:
            QMessageBox.information(self, "Result", "No geometries found in selected region.")
            return

        ax = filtered.plot(color="lightblue", edgecolor="k")
        user_polygon_gdf.boundary.plot(ax=ax, color="red", linewidth=2)
        plt.title("Spatial Query Result")
        plt.savefig("query_result_overlay.png", dpi=300)
        plt.show()

        filtered.to_file("query_result_selected_region.geojson", driver="GeoJSON")
        with open("query_area.geojson", "w") as f:
            json.dump(json.loads(user_polygon_gdf.to_json()), f)

        QMessageBox.information(self, "Done", "Query done. Results and polygon saved.")

# ----------------------------
# Launch
# ----------------------------
if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = SpatialQueryApp()
    window.show()
    sys.exit(app.exec_())