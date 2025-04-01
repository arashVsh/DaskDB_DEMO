from DaskDB.Context import Context, DASK_SCHEDULER_IP, DASK_SCHEDULER_PORT
import os

c = Context()
c.setup_configuration(daskSchedulerIP=DASK_SCHEDULER_IP, daskSchedulerPort=DASK_SCHEDULER_PORT)

building_path = os.path.join('../data', 'geonb_buildings_shp', 'geonb_buildings_shp.shp')
c.register_table('buildings', building_path)

floodriskareas_path = os.path.join('../data', 'geonb_floodriskareas_shp/Shapefiles', 'Flood_Hazard_Areas.shp')
c.register_table('flood_risk_areas', floodriskareas_path)

c.initSchema()

sql = """select BuildingID
        from buildings as b, flood_risk_areas as fra 
        where ST_Contains(fra.geometry, b.geometry)"""

res = c.query(sql)
print(res)
