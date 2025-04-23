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
from Util import myKMeans

anb_path = "../data/geonb_anb_shp/geonb_anb_addresses.shp"
flood_path = "../data/geonb_floodriskareas_shp/Shapefiles/Flood_Hazard_Areas.shp"

c = Context()
c.register_table("address_new_brunswick", anb_path)
c.register_table("flood_risk_areas", flood_path)
c.register_udf(myKMeans, [2])
c.initSchema()

anb_df = c.query("SELECT * FROM address_new_brunswick")
flood_df = c.query("SELECT * FROM flood_risk_areas")

anb_gdf = gpd.GeoDataFrame(anb_df, geometry="geometry")
flood_gdf = gpd.GeoDataFrame(flood_df, geometry="geometry")

anb_gdf.set_crs("EPSG:2953", inplace=True)
flood_gdf.set_crs("EPSG:2953", inplace=True)


class SpatialQueryApp(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Spatial KMeans Clustering")
        self.setGeometry(100, 100, 1200, 600)

        self.polygon_points_1 = []
        self.polygon_points_2 = []

        layout = QVBoxLayout()
        self.fig, (self.ax1, self.ax2) = plt.subplots(1, 2, figsize=(12, 6))
        self.canvas = FigureCanvas(self.fig)
        layout.addWidget(self.canvas)

        self.btn_run = QPushButton("Run KMeans Clustering on Selected Area")
        self.btn_run.clicked.connect(self.run_query)
        layout.addWidget(self.btn_run)

        self.setLayout(layout)
        self.plot_datasets()
        self.canvas.mpl_connect("button_press_event", self.onclick)

    def plot_datasets(self):
        self.ax1.clear()
        self.ax2.clear()
        anb_gdf.plot(ax=self.ax1, color="orange", edgecolor="k")
        flood_gdf.plot(ax=self.ax2, color="lightblue", edgecolor="k")
        self.ax1.set_title("Select Region on Address Map")
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

        sql = """
        SELECT *
        FROM address_new_brunswick AS anb, flood_risk_areas AS fra
        WHERE ST_Contains(fra.geometry, anb.geometry)
        """
        df = c.query(sql)

        gdf_result = gpd.GeoDataFrame(df, geometry="geometry")
        gdf_result.set_crs("EPSG:2953", inplace=True)
        filtered = gdf_result[gdf_result["geometry"].within(intersection)]

        if filtered.empty:
            QMessageBox.information(self, "Result", "No addresses in the selected intersection region.")
            return

        if "LATITUDE" not in filtered.columns or "LONGITUDE" not in filtered.columns:
            QMessageBox.warning(self, "Missing Coordinates", "Filtered data must include LATITUDE and LONGITUDE columns.")
            return

        # Run KMeans clustering using UDF
        clustered = c.executeUDF("myKMeans", filtered[["LATITUDE", "LONGITUDE"]])
        QMessageBox.information(self, "Done", "KMeans clustering completed and result plotted.")


if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = SpatialQueryApp()
    window.show()
    sys.exit(app.exec_())
