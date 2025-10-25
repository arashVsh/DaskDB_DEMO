# Visual Spatial Query Processing Interface on DaskDB

A graphical interface for **spatial query processing** built on top of **DaskDB**, enabling users to visually draw polygons, apply attribute filters, and automatically generate SQL queries for distributed geospatial analytics.  
The system integrates **PyQt5**, **GeoPandas**, **Matplotlib**, and **Apache Calcite (via JPype)** to support scalable, real-time query execution.

---

## Features
- Interactive polygon drawing and region selection  
- Attribute filtering (numeric and categorical) via checkboxes  
- Real-time SQL query generation and execution on **DaskDB**  
- Dual-map visualization for dataset intersection (e.g., buildings vs. flood zones)  
- Visual overlay of spatial query results and GeoJSON export  
- Runs on **Dask’s distributed scheduler** for scalable processing  
- Compatible with **Python 3.10+** and cross-platform (Windows/macOS/Linux)

---

## Project Structure
```
DaskDB_Spatial_Interface/
│
├── task1.py               # Single-map query (NB property dataset)
├── task2.py               # Dual-map query (buildings & flood zones)
├── task3.py               # Dual-map query (addresses & flood areas)
├── task4.py               # Dual-map query (property & flood risk, top 10 avg sale value)
│
├── yaml_creator.py        # Generates YAML environment with dependencies
├── CalcitePlanner.py      # SQL-to-logical-plan translator using Apache Calcite (via JPype)
├── CodeGenerator.py       # Converts logical plans to executable Python code
├── Context.py             # Manages table registration, Dask execution, and schema setup
├── supported_func.py      # Maps SQL functions to Python/GeoPandas equivalents
├── table_information.py   # Tracks table metadata, partitions, and runtime schema
└── README.md              # Project documentation
```

---

## 1. Run Locally

### Setup
```bash
git clone https://github.com/arashVsh/DaskDB_DEMO.git
cd DaskDB_DEMO
conda env create -f DaskDB-requirements.yaml
conda activate DaskDB
```

---

### Start Dask Cluster
**Terminal 1 – Scheduler:**
```bash
dask-scheduler
```

**Terminal 2 – Worker:**
```bash
dask-worker tcp://<scheduler-ip>:8786
```
Replace `<scheduler-ip>` with the actual IP address displayed by the scheduler.

---

### Run the Application
**Example (Task 1):**
```bash
python task1.py
```

**Other available tasks:**
```bash
python task2.py
python task3.py
python task4.py
```

Each task corresponds to a different spatial workflow — from single-map queries to multi-layer intersections.

---

## 2. Example Workflows

| Task | Description |
|------|--------------|
| **Task 1** | Query property assessment data via polygon drawing and numeric/categorical filters. |
| **Task 2** | Identify building footprints overlapping flood zones. |
| **Task 3** | Intersect civic addresses with flood-prone areas. |
| **Task 4** | Compute top 10 locations by average property sale value within flood zones. |

---

## 3. Example Output
Generated SQL (auto-constructed from user actions):
```sql
SELECT *
FROM property_assessment_map
WHERE ST_Within(geometry, ST_GeomFromText('POLYGON((...))'))
  AND Sale_Val BETWEEN 200000 AND 400000;
```

Visual Output:
- Polygon overlay shown in blue/red on the map  
- Results exported automatically as:  
  - `query_result_overlay.png`  
  - `query_result_selected_region.geojson`

---

## 4. Dependencies
Automatically generated with:
```bash
python yaml_creator.py
```

Core libraries:
- **Dask**, **Dask-GeoPandas**, **GeoPandas**, **Shapely**
- **PyQt5**, **Matplotlib**, **Pandas**
- **JPype**, **Apache Calcite**
- **sql_metadata**, **yaml**

---

## 5. Troubleshooting

| Issue | Possible Cause | Solution |
|-------|----------------|-----------|
| `JVM failed to start` | Java not installed or `JAVA_HOME` not set | Install JDK 8+ and set `JAVA_HOME` |
| `ModuleNotFoundError: jpype` | Missing dependency | Install via `conda install -c conda-forge jpype1` |
| GUI not displaying | Qt backend issue | Update Matplotlib and PyQt5 |
| `Worker refused connection` | Wrong IP/port for Dask worker | Use correct scheduler IP from terminal output |

---

## 6. Data Sources
Download shapefiles and datasets from:  
🔗 [UNB Cloud Share](https://unbcloud-my.sharepoint.com/:f:/r/personal/sdas6_unb_ca/Documents/Code/DaskDB_DEMO/data)

Ensure data paths in `task1.py`–`task4.py` match your local environment.

---

## 7. Evaluation
- Tested on **Windows 11**, **Python 3.10**  
- Connected **Dask Scheduler** and **Worker** for distributed execution  
- Verified correctness of spatial joins and attribute filtering  
- All visual queries successfully generated valid SQL and GeoJSON outputs

---

## 8. License
This project is provided for educational and research purposes.  
You may fork, extend, or adapt for your own DaskDB or GIS workflows.

---

## Author
Developed by **Arash Vashagh** — Graduate Research Assistant, UNB  
Focused on distributed data systems, spatial analytics, and intelligent interfaces.  
📧 [arash.vashagh@gmail.com](mailto:arash.vashagh@gmail.com)  
🔗 [LinkedIn](https://www.linkedin.com/in/arash-vashagh-23084923a/)
