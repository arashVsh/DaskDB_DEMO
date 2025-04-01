from DaskDB.Context import Context, DASK_SCHEDULER_IP, DASK_SCHEDULER_PORT
from Util import plotBarGraph
import os

c = Context()
c.setup_configuration(daskSchedulerIP=DASK_SCHEDULER_IP, daskSchedulerPort=DASK_SCHEDULER_PORT)

property_assessment_path = os.path.join('../data', 'geonb_pan-ncb_shp', 'geonb_pan_ncb.shp')
c.register_table('property_assessment_map', property_assessment_path)

floodriskareas_path = os.path.join('../data', 'geonb_floodriskareas_shp/Shapefiles', 'Flood_Hazard_Areas.shp')
c.register_table('flood_risk_areas', floodriskareas_path)

building_path = os.path.join('../data', 'geonb_buildings_shp', 'geonb_buildings_shp.shp')
c.register_table('buildings', building_path)

anb_addresses_path = os.path.join('../data', 'geonb_anb_shp', 'geonb_anb_addresses.shp')
c.register_table('address_new_brunswick', anb_addresses_path)

c.register_udf(plotBarGraph, [2])
c.initSchema()

sql = """select plotBarGraph(Location, avg_val)
        from (select Location, avg(sale_val) as avg_val
        from property_assessment_map as pam, flood_risk_areas as fra 
        where ST_Within(pam.geometry,fra.geometry) and sale_val > 0
        group by Location
        order by avg_val desc
        limit 10)"""

res = c.query(sql)
print(res)
