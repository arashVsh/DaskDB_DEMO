import sys
import json
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
from shapely.geometry import Polygon, box
from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
from PyQt5.QtWidgets import (
    QApplication, QWidget, QVBoxLayout, QHBoxLayout, QScrollArea,
    QPushButton, QMessageBox, QTextEdit, QDialog, QVBoxLayout as QVLayout,
    QLabel, QSpinBox, QLineEdit, QGroupBox, QCheckBox, QGridLayout
)
from PyQt5.QtCore import Qt

from DaskDB.Context import Context

property_path = "../data/geonb_pan-ncb_shp/geonb_pan_ncb.shp"
flood_path = "../data/geonb_floodriskareas_shp/Shapefiles/Flood_Hazard_Areas.shp"

c = Context()
c.register_table("property_assessment_map", property_path)
c.register_table("flood_risk_areas", flood_path)
c.initSchema()

property_df = c.query("SELECT * FROM property_assessment_map")
flood_df = c.query("SELECT * FROM flood_risk_areas")

property_gdf = gpd.GeoDataFrame(property_df, geometry="geometry")
flood_gdf = gpd.GeoDataFrame(flood_df, geometry="geometry")

property_gdf.set_crs("EPSG:2953", inplace=True)
flood_gdf.set_crs("EPSG:2953", inplace=True)

bbox_geom = box(*property_gdf.total_bounds)
bbox_gdf = gpd.GeoDataFrame(geometry=[bbox_geom], crs=property_gdf.crs)


class SpatialQueryApp(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Dual-Map Property Assessment & Flood Risk")
        self.setGeometry(100, 100, 1600, 900)

        self.polygon_points_1 = []
        self.polygon_points_2 = []
        self.feature_controls = {}

        main_layout = QHBoxLayout()

        self.fig, (self.ax1, self.ax2) = plt.subplots(2, 1, figsize=(8, 10))
        self.canvas = FigureCanvas(self.fig)

        map_layout = QVBoxLayout()
        map_layout.addWidget(self.canvas)
        map_layout.addWidget(QLabel("Tip: Right-click to finish polygon. Or skip drawing to select entire map."))

        reset_btn = QPushButton("Reset")
        reset_btn.clicked.connect(self.reset_ui)
        map_layout.addWidget(reset_btn)

        map_container = QWidget()
        map_container.setLayout(map_layout)
        main_layout.addWidget(map_container, stretch=6)

        slider_layout = QGridLayout()
        row = 0

        all_cols = pd.concat([property_gdf, flood_gdf], axis=0)
        numeric_cols = all_cols.select_dtypes(include='number').columns
        category_cols = all_cols.select_dtypes(include='object').columns

        for col in numeric_cols:
            col_min = all_cols[col].min()
            col_max = all_cols[col].max()
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

            def toggle(state, min_box=min_box, max_box=max_box):
                min_box.setEnabled(state == Qt.Checked)
                max_box.setEnabled(state == Qt.Checked)

            checkbox.stateChanged.connect(toggle)
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

        self.slider_group = QGroupBox("Attribute Filters")
        self.slider_group.setLayout(slider_layout)

        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setWidget(self.slider_group)

        filter_layout = QVBoxLayout()
        filter_layout.addWidget(scroll)

        run_btn = QPushButton("Run Spatial Query on Intersection")
        run_btn.clicked.connect(self.run_query)
        filter_layout.addWidget(run_btn)

        filter_container = QWidget()
        filter_container.setLayout(filter_layout)
        main_layout.addWidget(filter_container, stretch=6)

        self.setLayout(main_layout)
        self.plot_datasets()
        self.canvas.mpl_connect("button_press_event", self.onclick)

    def plot_datasets(self):
        self.ax1.clear()
        self.ax2.clear()
        property_gdf.plot(ax=self.ax1, color="orange", edgecolor="k")
        flood_gdf.plot(ax=self.ax2, color="lightgrey", edgecolor="k")
        self.ax1.set_title("Property Assessment Map")
        self.ax2.set_title("Flood Risk Map")
        self.canvas.draw()

    def reset_ui(self):
        self.polygon_points_1 = []
        self.polygon_points_2 = []
        self.plot_datasets()
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
        from shapely.geometry import Polygon
        if len(self.polygon_points_1) >= 3 and len(self.polygon_points_2) >= 3:
            poly1 = Polygon(self.polygon_points_1)
            poly2 = Polygon(self.polygon_points_2)
            intersection = poly1.intersection(poly2)
        else:
            intersection = bbox_geom

        if intersection.is_empty:
            QMessageBox.information(self, "No Overlap", "Selected regions do not overlap.")
            return

        filtered_property = property_gdf[property_gdf.geometry.within(intersection) & (property_gdf["Sale_Val"] > 0)]
        filtered_flood = flood_gdf[flood_gdf.geometry.within(intersection)]

        where_clauses = [
            f"ST_Within(pam.geometry, ST_GeomFromText('{intersection.wkt}'))",
            "ST_Within(pam.geometry, fra.geometry)",
            "sale_val > 0"
        ]

        for col, ctrl in self.feature_controls.items():
            if not ctrl[0].isChecked():
                continue
            if len(ctrl) == 3:
                min_val, max_val = ctrl[1].value(), ctrl[2].value()
                where_clauses.append(f"{col} BETWEEN {min_val} AND {max_val}")
            elif len(ctrl) == 2:
                values = [v.strip() for v in ctrl[1].text().split(',') if v.strip()]
                if values:
                    val_string = ", ".join(f"'{v}'" for v in values)
                    where_clauses.append(f"{col} IN ({val_string})")

        sql_text = f"""
SELECT Location, avg(sale_val) as avg_val
FROM property_assessment_map AS pam, flood_risk_areas AS fra
WHERE
    {' AND\n    '.join(where_clauses)}
GROUP BY Location
ORDER BY avg_val DESC
LIMIT 10
"""

        dialog = QDialog(self)
        dialog.setWindowTitle("Generated SQL Query")
        dialog.resize(800, 300)
        dlg_layout = QVLayout()
        text_edit = QTextEdit()
        text_edit.setPlainText(sql_text.strip())
        text_edit.setReadOnly(True)
        dlg_layout.addWidget(text_edit)
        dialog.setLayout(dlg_layout)
        dialog.exec_()

        if filtered_property.empty and filtered_flood.empty:
            QMessageBox.information(self, "Result", "No geometries found in the intersection.")
            return

        user_poly_gdf = gpd.GeoDataFrame(geometry=[intersection], crs=property_gdf.crs)
        ax = filtered_property.plot(color="orange", edgecolor="k")
        filtered_flood.boundary.plot(ax=ax, color="blue")
        user_poly_gdf.boundary.plot(ax=ax, color="red", linewidth=2)
        plt.title("Average Sale Values in Flood Risk Area")
        plt.savefig("query_result_property_flood.png", dpi=300)
        plt.show()

        filtered_property.to_file("query_result_property.geojson", driver="GeoJSON")
        filtered_flood.to_file("query_result_flood.geojson", driver="GeoJSON")
        QMessageBox.information(self, "Done", "Results and SQL query saved.")


if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = SpatialQueryApp()
    window.show()
    sys.exit(app.exec_())
