import sys
import json
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
from shapely.geometry import Polygon, box
from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
from PyQt5.QtWidgets import (
    QApplication,
    QWidget,
    QVBoxLayout,
    QHBoxLayout,
    QScrollArea,
    QPushButton,
    QMessageBox,
    QTextEdit,
    QDialog,
    QVBoxLayout as QVLayout,
    QLabel,
    QSpinBox,
    QLineEdit,
    QFormLayout,
    QGroupBox,
    QCheckBox,
    QGridLayout
)
from PyQt5.QtCore import Qt

from DaskDB.Context import Context

property_path = "../data/geonb_pan-ncb_shp/geonb_pan_ncb.shp"

c = Context()
c.register_table("property_assessment_map", property_path)
c.initSchema()

df = c.query("SELECT * FROM property_assessment_map")
gdf = gpd.GeoDataFrame(df, geometry="geometry")
gdf.set_crs("EPSG:2953", inplace=True)
bbox_geom = box(*gdf.total_bounds)
bbox_gdf = gpd.GeoDataFrame(geometry=[bbox_geom], crs=gdf.crs)


class SpatialQueryApp(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Spatial Query GUI with Attribute Filters")
        self.setGeometry(200, 200, 1600, 1000)

        self.polygon_points = []
        self._background = None

        main_layout = QHBoxLayout()

        # Left (Map View)
        map_layout = QVBoxLayout()
        self.fig, self.ax = plt.subplots(figsize=(6, 8))
        self.canvas = FigureCanvas(self.fig)
        self.canvas.setMinimumWidth(800)
        self.canvas.setMinimumHeight(900)
        map_layout.addWidget(self.canvas)
        map_layout.addWidget(QLabel("Tip: Right-click to finish polygon. Or skip drawing to select the entire map."))

        reset_btn = QPushButton("Reset")
        reset_btn.clicked.connect(self.reset_ui)
        map_layout.addWidget(reset_btn)

        map_container = QWidget()
        map_container.setLayout(map_layout)
        main_layout.addWidget(map_container, stretch=6)

        # Right (Filter Panel)
        self.feature_controls = {}
        self.slider_group = QGroupBox("Feature Filters")
        slider_layout = QGridLayout()

        numeric_cols = gdf.select_dtypes(include='number').columns
        category_cols = gdf.select_dtypes(include='object').columns

        row = 0
        for col in numeric_cols:
            if col == "Shape_Area":
                continue
            col_min = gdf[col].min()
            col_max = gdf[col].max()
            if pd.isna(col_min) or pd.isna(col_max) or col_min >= col_max:
                continue
            checkbox = QCheckBox(col)
            min_box = QSpinBox()
            max_box = QSpinBox()
            min_box.setRange(int(max(col_min, -2_147_483_648)), int(min(col_max, 2_147_483_647)))
            max_box.setRange(int(max(col_min, -2_147_483_648)), int(min(col_max, 2_147_483_647)))
            min_box.setValue(int(max(col_min, -2_147_483_648)))
            max_box.setValue(int(min(col_max, 2_147_483_647)))
            min_box.setEnabled(False)
            max_box.setEnabled(False)

            def toggle_spinbox(state, min_box=min_box, max_box=max_box):
                min_box.setEnabled(state == Qt.Checked)
                max_box.setEnabled(state == Qt.Checked)

            checkbox.stateChanged.connect(toggle_spinbox)
            self.feature_controls[col] = (checkbox, min_box, max_box)

            slider_layout.addWidget(checkbox, row, 0)
            slider_layout.addWidget(QLabel("min"), row, 1)
            slider_layout.addWidget(min_box, row, 2)
            slider_layout.addWidget(QLabel("max"), row, 3)
            slider_layout.addWidget(max_box, row, 4)
            row += 1

        for col in category_cols:
            checkbox = QCheckBox(col)
            line_edit = QLineEdit()
            line_edit.setPlaceholderText("comma-separated values")
            line_edit.setEnabled(False)
            checkbox.stateChanged.connect(lambda state, le=line_edit: le.setEnabled(state == Qt.Checked))
            self.feature_controls[col] = (checkbox, line_edit)
            slider_layout.addWidget(checkbox, row, 0)
            slider_layout.addWidget(line_edit, row, 1, 1, 4)
            row += 1

        self.slider_group.setLayout(slider_layout)

        scroll_area = QScrollArea()
        scroll_area.setWidgetResizable(True)
        scroll_area.setWidget(self.slider_group)

        filter_layout = QVBoxLayout()
        filter_layout.addWidget(scroll_area)

        self.btn_run = QPushButton("Run Query with Drawn Polygon and Filters")
        self.btn_run.clicked.connect(self.run_query)
        filter_layout.addWidget(self.btn_run)

        filter_container = QWidget()
        filter_container.setLayout(filter_layout)
        main_layout.addWidget(filter_container, stretch=6)

        self.setLayout(main_layout)
        self.plot_dataset()
        self.cid = self.canvas.mpl_connect("button_press_event", self.onclick)

    def plot_dataset(self):
        self.ax.clear()
        gdf.plot(ax=self.ax, color="lightgrey", edgecolor="k")
        bbox_gdf.boundary.plot(ax=self.ax, color="red")
        self.ax.set_title("Left-click to select a point, Right-click to finish")
        self.canvas.draw()
        self._background = self.fig.canvas.copy_from_bbox(self.fig.bbox)

    def reset_ui(self):
        self.polygon_points = []
        self.ax.clear()
        gdf.plot(ax=self.ax, color="lightgrey", edgecolor="k")
        bbox_gdf.boundary.plot(ax=self.ax, color="red")
        self.ax.set_title("Left-click to select a point, Right-click to finish")
        self.canvas.draw()
        for ctrl in self.feature_controls.values():
            checkbox = ctrl[0]
            checkbox.setChecked(False)
            if len(ctrl) == 3:
                ctrl[1].setValue(ctrl[1].minimum())
                ctrl[2].setValue(ctrl[2].maximum())
                ctrl[1].setEnabled(False)
                ctrl[2].setEnabled(False)
            elif len(ctrl) == 2:
                ctrl[1].clear()
                ctrl[1].setEnabled(False)

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
            self.ax.plot(*zip(*self.polygon_points, self.polygon_points[0]), color="blue")
            self.canvas.draw()

    def run_query(self):
        if len(self.polygon_points) >= 3:
            user_polygon = Polygon(self.polygon_points)
        else:
            user_polygon = bbox_geom

        user_polygon_gdf = gpd.GeoDataFrame(geometry=[user_polygon], crs=gdf.crs)
        filtered = gdf[gdf.geometry.within(user_polygon)]
        where_clauses = [f"ST_Within(geometry, ST_GeomFromText('{user_polygon.wkt}'))"]

        for col, ctrl in self.feature_controls.items():
            checkbox = ctrl[0]
            if not checkbox.isChecked():
                continue
            if len(ctrl) == 3:
                min_val = ctrl[1].value()
                max_val = ctrl[2].value()
                filtered = filtered[(filtered[col] >= min_val) & (filtered[col] <= max_val)]
                where_clauses.append(f"{col} BETWEEN {min_val} AND {max_val}")
            elif len(ctrl) == 2:
                values = [v.strip() for v in ctrl[1].text().split(",") if v.strip()]
                if values:
                    filtered = filtered[filtered[col].isin(values)]
                    val_string = ", ".join(f"'{v}'" for v in values)
                    where_clauses.append(f"{col} IN ({val_string})")

        if filtered.empty:
            QMessageBox.information(self, "Result", "No geometries found with selected filters.")
            return

        where_clause = " AND\n    ".join(where_clauses)
        sql_text = f"""
SELECT *
FROM property_assessment_map
WHERE
    {where_clause}
"""

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


if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = SpatialQueryApp()
    window.show()
    sys.exit(app.exec_())
