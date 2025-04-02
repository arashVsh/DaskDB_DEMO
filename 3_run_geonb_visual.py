# spatialQueryGui_3run.py

import sys
import os
import json
import geopandas as gpd
import matplotlib.pyplot as plt
from shapely.geometry import Polygon, box
from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
from PyQt5.QtWidgets import (
    QApplication, QWidget, QVBoxLayout, QPushButton, QMessageBox,
    QTextEdit, QDialog, QVBoxLayout as QVLayout
)

from DaskDB.Context import Context, DASK_SCHEDULER_IP, DASK_SCHEDULER_PORT

# -----------------------------------
# Load Dataset (3_run_geonb.py based)
# -----------------------------------
c = Context()
c.setup_configuration(daskSchedulerIP=DASK_SCHEDULER_IP, daskSchedulerPort=DASK_SCHEDULER_PORT)

anb_addresses_path = os.path.join('../data', 'geonb_anb_shp', 'geonb_anb_addresses.shp')
c.register_table('address_new_brunswick', anb_addresses_path)

floodriskareas_path = os.path.join('../data', 'geonb_floodriskareas_shp/Shapefiles', 'Flood_Hazard_Areas.shp')
c.register_table('flood_risk_areas', floodriskareas_path)

c.initSchema()

# Query for visualization bounds
df = c.query("SELECT * FROM flood_risk_areas")
gdf = gpd.GeoDataFrame(df, geometry='geometry')
gdf.set_crs("EPSG:2953", inplace=True)

bbox_geom = box(*gdf.total_bounds)
bbox_gdf = gpd.GeoDataFrame(geometry=[bbox_geom], crs=gdf.crs)

# -----------------------------------
# GUI Class
# -----------------------------------
class SpatialQueryApp(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Spatial Query GUI - Route 105 Intersections")
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
        if event.button == 1:
            self.polygon_points.append((event.xdata, event.ydata))
            self.ax.plot(event.xdata, event.ydata, 'bo')
            self.canvas.draw()
        elif event.button == 3:
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

        if not user_polygon.intersects(bbox_geom):
            QMessageBox.warning(self, "Invalid", "Polygon is outside dataset boundary.")
            return

        # Query using original logic
        sql = """
        SELECT ADDR_DESC
        FROM address_new_brunswick AS anb, flood_risk_areas AS fra
        WHERE ST_Intersects(fra.geometry, anb.geometry) AND STREET = 'ROUTE 105'
        """
        df = c.query(sql)

        if df.empty:
            QMessageBox.information(self, "Result", "No intersections found.")
            return

        gdf_result = gpd.GeoDataFrame(df, geometry='geometry')
        gdf_result.set_crs("EPSG:2953", inplace=True)
        filtered = gdf_result[gdf_result.geometry.within(user_polygon)]

        if filtered.empty:
            QMessageBox.information(self, "Filtered", "No geometries found inside selected polygon.")
            return

        # Show SQL
        user_wkt = user_polygon.wkt
        sql_text = (
            "SELECT ADDR_DESC\n"
            "FROM address_new_brunswick AS anb, flood_risk_areas AS fra\n"
            "WHERE ST_Intersects(fra.geometry, anb.geometry)\n"
            "AND STREET = 'ROUTE 105'\n"
            f"-- Filtered with user polygon: ST_Within(anb.geometry, ST_GeomFromText('{user_wkt}'))"
        )

        sql_dialog = QDialog(self)
        sql_dialog.setWindowTitle("Generated SQL Query")
        sql_dialog.resize(800, 300)
        dialog_layout = QVLayout()
        text_edit = QTextEdit()
        text_edit.setPlainText(sql_text)
        text_edit.setReadOnly(True)
        text_edit.setWordWrapMode(False)
        dialog_layout.addWidget(text_edit)
        sql_dialog.setLayout(dialog_layout)
        sql_dialog.exec_()

        # Plot
        ax = filtered.plot(color="lightblue", edgecolor="k")
        gpd.GeoSeries([user_polygon]).boundary.plot(ax=ax, color="red", linewidth=2)
        plt.title("Flood Risk + Address Intersection")
        plt.savefig("query_result_3_overlay.png", dpi=300)
        plt.show()

        filtered.to_file("query_result_3_selected.geojson", driver="GeoJSON")
        QMessageBox.information(self, "Done", "Query done. Results and SQL saved.")

# -----------------------------------
# Launch
# -----------------------------------
if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = SpatialQueryApp()
    window.show()
    sys.exit(app.exec_())
