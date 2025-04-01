from DaskDB.Context import Context, DASK_SCHEDULER_IP, DASK_SCHEDULER_PORT
from Util import myKMeans
import os

c = Context()
c.setup_configuration(daskSchedulerIP=DASK_SCHEDULER_IP, daskSchedulerPort=DASK_SCHEDULER_PORT)

anb_addresses_path = os.path.join('../data', 'geonb_anb_shp', 'geonb_anb_addresses.shp')
c.register_table('address_new_brunswick', anb_addresses_path)

floodriskareas_path = os.path.join('../data', 'geonb_floodriskareas_shp/Shapefiles', 'Flood_Hazard_Areas.shp')
c.register_table('flood_risk_areas', floodriskareas_path)

c.register_udf(myKMeans, [2])
c.initSchema()

sql = """select myKMeans(Latitude,Longitude)
        from address_new_brunswick as anb, flood_risk_areas as fra
        where ST_Contains(fra.geometry, anb.geometry)"""

res = c.query(sql)
print(res)
