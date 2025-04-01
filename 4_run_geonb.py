from DaskDB.Context import Context, DASK_SCHEDULER_IP, DASK_SCHEDULER_PORT
import os

c = Context()
c.setup_configuration(daskSchedulerIP=DASK_SCHEDULER_IP, daskSchedulerPort=DASK_SCHEDULER_PORT)

property_assessment_path = os.path.join('../data', 'geonb_pan-ncb_shp', 'geonb_pan_ncb.shp')
c.register_table('property_assessment_map', property_assessment_path)

floodriskareas_path = os.path.join('../data', 'geonb_floodriskareas_shp/Shapefiles', 'Flood_Hazard_Areas.shp')
c.register_table('flood_risk_areas', floodriskareas_path)

c.initSchema()

sql = """select Location, avg(sale_val) as avg_val
        from property_assessment_map as pam, flood_risk_areas as fra 
        where ST_Within(pam.geometry,fra.geometry) and sale_val > 0
        group by Location
        order by avg_val desc
        limit 10"""

res = c.query(sql)
print(res)
