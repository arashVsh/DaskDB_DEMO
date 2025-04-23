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

anb_addresses_path = '../data/geonb_anb_shp/geonb_anb_addresses.shp'
floodriskareas_path = '../data/geonb_floodriskareas_shp/Shapefiles/Flood_Hazard_Areas.shp'

c = Context()
c.register_table('address_new_brunswick', anb_addresses_path)
c.register_table('flood_risk_areas', floodriskareas_path)
c.initSchema()

anb_df = c.query("SELECT * FROM address_new_brunswick")
flood_df = c.query("SELECT * FROM flood_risk_areas")

anb_gdf = gpd.GeoDataFrame(anb_df, geometry='geometry')
flood_gdf = gpd.GeoDataFrame(flood_df, geometry='geometry')

anb_gdf.set_crs("EPSG:2953", inplace=True)
flood_gdf.set_crs("EPSG:2953", inplace=True)

bbox_geom = box(*flood_gdf.total_bounds)
bbox_gdf = gpd.GeoDataFrame(geometry=[bbox_geom], crs=flood_gdf.crs)


class SpatialQueryApp(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Dual-Map Spatial Query: Address + Flood Overlap")
        self.setGeometry(100, 100, 1200, 600)

        self.polygon_points_1 = []
        self.polygon_points_2 = []

        layout = QVBoxLayout()

        self.fig, (self.ax1, self.ax2) = plt.subplots(1, 2, figsize=(12, 6))
        self.canvas = FigureCanvas(self.fig)
        layout.addWidget(self.canvas)

        self.btn_run = QPushButton("Run Join Query on Selected Regions")
        self.btn_run.clicked.connect(self.run_query)
        layout.addWidget(self.btn_run)

        self.setLayout(layout)
        self.plot_datasets()
        self.canvas.mpl_connect('button_press_event', self.onclick)

    def plot_datasets(self):
        self.ax1.clear()
        self.ax2.clear()
        anb_gdf.plot(ax=self.ax1, color="orange", edgecolor="k", label="Addresses")
        flood_gdf.plot(ax=self.ax2, color="lightgrey", edgecolor="k", label="Flood Risk")
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

        user_poly_gdf = gpd.GeoDataFrame(geometry=[intersection], crs=anb_gdf.crs)

        filtered_anb = anb_gdf[anb_gdf.geometry.within(intersection)]
        filtered_flood = flood_gdf[flood_gdf.geometry.within(intersection)]

        if filtered_anb.empty and filtered_flood.empty:
            QMessageBox.information(self, "Result", "No geometries found in the intersection.")
            return

        user_wkt = intersection.wkt
        sql_text = (
            "SELECT ADDR_DESC\n"
            "FROM address_new_brunswick AS anb, flood_risk_areas AS fra\n"
            "WHERE ST_Intersects(fra.geometry, anb.geometry) AND STREET = 'ROUTE 105'\n"
            f"-- Filtered with polygon intersection: ST_Within(anb.geometry, ST_GeomFromText('{user_wkt}'))"
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

        ax = filtered_anb.plot(color="orange", edgecolor="k")
        filtered_flood.boundary.plot(ax=ax, color="blue")
        user_poly_gdf.boundary.plot(ax=ax, color="red", linewidth=2)
        plt.title("Intersected Region: Route 105 Addresses & Flood Risk")
        plt.savefig("query_result_dual_map_3run.png", dpi=300)
        plt.show()

        filtered_anb.to_file("query_result_3run_addresses.geojson", driver="GeoJSON")
        filtered_flood.to_file("query_result_3run_flood.geojson", driver="GeoJSON")
        QMessageBox.information(self, "Done", "Results and SQL query saved.")


if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = SpatialQueryApp()
    window.show()
    sys.exit(app.exec_())
