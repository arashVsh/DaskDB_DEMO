import sys
import geopandas as gpd
import matplotlib.pyplot as plt
from shapely.geometry import Polygon, box
from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
from PyQt5.QtWidgets import (
    QApplication, QWidget, QVBoxLayout, QPushButton, QMessageBox,
    QTextEdit, QDialog, QVBoxLayout as QVLayout
)

from DaskDB.Context import Context
from Util import plotBarGraph

property_path = "../data/geonb_pan-ncb_shp/geonb_pan_ncb.shp"
flood_path = "../data/geonb_floodriskareas_shp/Shapefiles/Flood_Hazard_Areas.shp"

c = Context()
c.register_table("property_assessment_map", property_path)
c.register_table("flood_risk_areas", flood_path)
c.register_udf(plotBarGraph, [2])
c.initSchema()

property_df = c.query("SELECT * FROM property_assessment_map")
flood_df = c.query("SELECT * FROM flood_risk_areas")

property_gdf = gpd.GeoDataFrame(property_df, geometry="geometry")
flood_gdf = gpd.GeoDataFrame(flood_df, geometry="geometry")

property_gdf.set_crs("EPSG:2953", inplace=True)
flood_gdf.set_crs("EPSG:2953", inplace=True)

bbox_geom = box(*flood_gdf.total_bounds)
bbox_gdf = gpd.GeoDataFrame(geometry=[bbox_geom], crs=flood_gdf.crs)


class SpatialQueryApp(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Spatial Query + UDF Bar Chart")
        self.setGeometry(100, 100, 1200, 600)

        self.polygon_points_1 = []
        self.polygon_points_2 = []

        layout = QVBoxLayout()

        self.fig, (self.ax1, self.ax2) = plt.subplots(1, 2, figsize=(12, 6))
        self.canvas = FigureCanvas(self.fig)
        layout.addWidget(self.canvas)

        self.btn_run = QPushButton("Run Query with UDF on Selection")
        self.btn_run.clicked.connect(self.run_query)
        layout.addWidget(self.btn_run)

        self.setLayout(layout)
        self.plot_datasets()
        self.canvas.mpl_connect('button_press_event', self.onclick)

    def plot_datasets(self):
        self.ax1.clear()
        self.ax2.clear()
        property_gdf.plot(ax=self.ax1, color="orange", edgecolor="k", label="Properties")
        flood_gdf.plot(ax=self.ax2, color="lightgrey", edgecolor="k", label="Flood Areas")
        self.ax1.set_title("Select Region on Property Map")
        self.ax2.set_title("Select Region on Flood Map")
        self.canvas.draw()

    def onclick(self, event):
        if event.inaxes == self.ax1:
            if event.button == 1:
                self.polygon_points_1.append((event.xdata, event.ydata))
                self.ax1.plot(event.xdata, event.ydata, "bo")
            elif event.button == 3 and len(self.polygon_points_1) >= 3:
                self.ax1.plot(*zip(*self.polygon_points_1, self.polygon_points_1[0]), color="blue")

        elif event.inaxes == self.ax2:
            if event.button == 1:
                self.polygon_points_2.append((event.xdata, event.ydata))
                self.ax2.plot(event.xdata, event.ydata, "go")
            elif event.button == 3 and len(self.polygon_points_2) >= 3:
                self.ax2.plot(*zip(*self.polygon_points_2, self.polygon_points_2[0]), color="green")

        self.canvas.draw()

    def run_query(self):
        if len(self.polygon_points_1) < 3 or len(self.polygon_points_2) < 3:
            QMessageBox.warning(self, "Invalid", "Please draw valid polygons on both maps.")
            return

        poly1 = Polygon(self.polygon_points_1)
        poly2 = Polygon(self.polygon_points_2)
        intersection = poly1.intersection(poly2)

        if intersection.is_empty:
            QMessageBox.information(self, "No Overlap", "Selected regions do not overlap.")
            return

        # Execute base join query and filter in Pandas after
        sql = """
        SELECT *
        FROM property_assessment_map AS pam, flood_risk_areas AS fra
        WHERE ST_Within(pam.geometry, fra.geometry) AND Sale_Val > 0
        """
        df = c.query(sql)

        # Convert and filter manually with geopandas
        gdf_result = gpd.GeoDataFrame(df, geometry="geometry")
        gdf_result.set_crs("EPSG:2953", inplace=True)
        filtered = gdf_result[gdf_result["geometry"].within(intersection)]

        if filtered.empty:
            QMessageBox.information(self, "Result", "No results to show for this selection.")
            return

        # Manual grouping and aggregation to simulate SQL aggregation
        grouped = filtered.groupby("Location")["Sale_Val"].mean().reset_index().rename(columns={"Sale_Val": "avg_val"})
        grouped = grouped.sort_values(by="avg_val", ascending=False).head(10)

        # Visualize selected region
        user_poly_gdf = gpd.GeoDataFrame(geometry=[intersection], crs=property_gdf.crs)
        ax = filtered.plot(color="orange", edgecolor="k")
        flood_gdf[flood_gdf.geometry.within(intersection)].boundary.plot(ax=ax, color="blue")
        user_poly_gdf.boundary.plot(ax=ax, color="red", linewidth=2)
        plt.title("Intersected Query Region")
        plt.savefig("query_udf_region_overlay.png", dpi=300)
        plt.show()

        # Plot using UDF
        c.executeUDF("plotBarGraph", grouped)
        QMessageBox.information(self, "Done", "Bar chart generated via UDF.")


if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = SpatialQueryApp()
    window.show()
    sys.exit(app.exec_())
