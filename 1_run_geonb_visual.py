import sys
import json
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

from DaskDB.Context import Context, DASK_SCHEDULER_IP, DASK_SCHEDULER_PORT

# ----------------------------
# Load Dataset
# ----------------------------
c = Context()
c.setup_configuration(daskSchedulerIP=DASK_SCHEDULER_IP, daskSchedulerPort=DASK_SCHEDULER_PORT)
c.register_table(
    "property_assessment_map", "../data/geonb_pan-ncb_shp/geonb_pan_ncb.shp"
)
c.initSchema()
df = c.query("SELECT * FROM property_assessment_map")
gdf = gpd.GeoDataFrame(df, geometry="geometry")
gdf.set_crs("EPSG:2953", inplace=True)
bbox_geom = box(*gdf.total_bounds)
bbox_gdf = gpd.GeoDataFrame(geometry=[bbox_geom], crs=gdf.crs)


# ----------------------------
# GUI
# ----------------------------
class SpatialQueryApp(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Spatial Query GUI")
        self.setGeometry(200, 200, 1600, 1200)

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

        self.cid = self.canvas.mpl_connect("button_press_event", self.onclick)

    def plot_dataset(self):
        self.ax.clear()
        gdf.plot(ax=self.ax, color="lightgrey", edgecolor="k")
        bbox_gdf.boundary.plot(ax=self.ax, color="red")
        self.ax.set_title("Left-click to select a point, Right-click to finish")
        self.canvas.draw()

    def onclick(self, event):
        if event.inaxes != self.ax:
            return
        if event.button == 1:  # Left click
            self.polygon_points.append((event.xdata, event.ydata))
            self.ax.plot(event.xdata, event.ydata, "bo")
            self.canvas.draw()
        elif event.button == 3:  # Right click to close polygon
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
        user_polygon_gdf = gpd.GeoDataFrame(geometry=[user_polygon], crs=gdf.crs)

        if not user_polygon.intersects(bbox_geom):
            QMessageBox.warning(self, "Invalid", "Polygon is outside dataset boundary.")
            return

        filtered = gdf[gdf.geometry.within(user_polygon)]

        if filtered.empty:
            QMessageBox.information(
                self, "Result", "No geometries found in selected region."
            )
            return

        # Show SQL equivalent in a scrollable popup
        polygon_wkt = user_polygon.wkt
        sql_text = f"SELECT * FROM property_assessment_map WHERE ST_Within(geometry, ST_GeomFromText('{polygon_wkt}'))"

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
