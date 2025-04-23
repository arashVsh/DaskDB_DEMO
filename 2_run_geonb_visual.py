# 2_run_geonb_gui.py - Spatial Query Interface for multiple layers (Buildings + Flood Risk Areas)

import sys
import geopandas as gpd
import matplotlib.pyplot as plt
from shapely.geometry import Polygon, box
from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
from PyQt5.QtWidgets import (
    QApplication,
    QWidget,
    QVBoxLayout,
    QPushButton,
    QMessageBox,
    QTextEdit,
    QDialog,
    QVBoxLayout as QVLayout,
)

from DaskDB.Context import Context

buildings_path = "../data/geonb_buildings_shp/geonb_buildings_shp.shp"
flood_path = "../data/geonb_floodriskareas_shp/Shapefiles/Flood_Hazard_Areas.shp"

c = Context()
c.register_table("buildings", buildings_path)
c.register_table("flood_risk_areas", flood_path)
c.initSchema()

buildings_df = c.query("SELECT * FROM buildings")
flood_df = c.query("SELECT * FROM flood_risk_areas")

buildings_gdf = gpd.GeoDataFrame(buildings_df, geometry="geometry")
flood_gdf = gpd.GeoDataFrame(flood_df, geometry="geometry")

buildings_gdf.set_crs("EPSG:2953", inplace=True)
flood_gdf.set_crs("EPSG:2953", inplace=True)

bbox_geom = box(*buildings_gdf.total_bounds)
bbox_gdf = gpd.GeoDataFrame(geometry=[bbox_geom], crs=buildings_gdf.crs)


# ----------------------------
# GUI
# ----------------------------
class SpatialQueryApp(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Spatial Query GUI for Buildings and Flood Risk Areas")
        self.setGeometry(100, 100, 800, 600)

        self.polygon_points = []

        layout = QVBoxLayout()

        self.fig, self.ax = plt.subplots()
        self.canvas = FigureCanvas(self.fig)
        layout.addWidget(self.canvas)

        self.btn_run = QPushButton("Run Spatial Query")
        self.btn_run.clicked.connect(self.run_query)
        layout.addWidget(self.btn_run)

        self.setLayout(layout)

        self.plot_dataset()

        self.cid = self.canvas.mpl_connect("button_press_event", self.onclick)

    def plot_dataset(self):
        self.ax.clear()
        flood_gdf.plot(
            ax=self.ax, color="lightgrey", edgecolor="k", label="Flood Risk Areas"
        )
        buildings_gdf.plot(ax=self.ax, color="orange", edgecolor="k", label="Buildings")
        bbox_gdf.boundary.plot(ax=self.ax, color="red")
        self.ax.set_title("Left-click to define polygon, Right-click to finish")
        self.canvas.draw()

    def onclick(self, event):
        if event.inaxes != self.ax:
            return
        if event.button == 1:
            self.polygon_points.append((event.xdata, event.ydata))
            self.ax.plot(event.xdata, event.ydata, "bo")
            self.canvas.draw()
        elif event.button == 3:
            if len(self.polygon_points) < 3:
                QMessageBox.warning(self, "Invalid", "Polygon needs at least 3 points.")
                return
            self.ax.plot(
                *zip(*self.polygon_points, self.polygon_points[0]), color="blue"
            )
            self.canvas.draw()

    def run_query(self):
        if len(self.polygon_points) < 3:
            QMessageBox.warning(self, "Invalid", "Draw a polygon first.")
            return

        user_polygon = Polygon(self.polygon_points)
        user_polygon_gdf = gpd.GeoDataFrame(
            geometry=[user_polygon], crs=buildings_gdf.crs
        )

        if not user_polygon.intersects(bbox_geom):
            QMessageBox.warning(self, "Invalid", "Polygon is outside dataset boundary.")
            return

        # -------- Spatial Query Logic --------
        buildings_filtered = buildings_gdf[buildings_gdf.geometry.within(user_polygon)]
        flood_filtered = flood_gdf[flood_gdf.geometry.within(user_polygon)]

        if buildings_filtered.empty and flood_filtered.empty:
            QMessageBox.information(
                self, "Result", "No geometries found in the selected region."
            )
            return

        # ----------- SQL Text Generation --------
        polygon_wkt = user_polygon.wkt
        sql_text = f"""
SELECT BuildingID FROM buildings, flood_risk_areas
WHERE ST_Within(buildings.geometry, ST_GeomFromText('{polygon_wkt}'))
AND ST_Within(flood_risk_areas.geometry, ST_GeomFromText('{polygon_wkt}'))
AND ST_Contains(flood_risk_areas.geometry, buildings.geometry);
"""

        # ----------- Show Query in Dialog --------
        sql_dialog = QDialog(self)
        sql_dialog.setWindowTitle("Generated SQL Query")
        sql_dialog.resize(800, 300)
        dialog_layout = QVLayout()
        text_edit = QTextEdit()
        text_edit.setPlainText(sql_text.strip())
        text_edit.setReadOnly(True)
        dialog_layout.addWidget(text_edit)
        sql_dialog.setLayout(dialog_layout)
        sql_dialog.exec_()

        # ----------- Plot Result --------
        ax = buildings_filtered.plot(color="orange", edgecolor="k")
        flood_filtered.boundary.plot(ax=ax, color="grey")
        user_polygon_gdf.boundary.plot(ax=ax, color="red", linewidth=2)
        plt.title("Buildings & Flood Areas within Selection")
        plt.savefig("query_result_overlay.png", dpi=300)
        plt.show()

        buildings_filtered.to_file(
            "query_result_selected_buildings.geojson", driver="GeoJSON"
        )
        flood_filtered.to_file(
            "query_result_selected_flood_areas.geojson", driver="GeoJSON"
        )

        QMessageBox.information(self, "Done", "Results and query saved.")


# ----------------------------
# Launch
# ----------------------------
if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = SpatialQueryApp()
    window.show()
    sys.exit(app.exec_())
