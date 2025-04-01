from DaskDB.Context import Context, DASK_SCHEDULER_IP, DASK_SCHEDULER_PORT
from Util import myLinearFit
import os

c = Context()
c.setup_configuration(daskSchedulerIP=DASK_SCHEDULER_IP, daskSchedulerPort=DASK_SCHEDULER_PORT)

geonb_pan_ncb_shp_path = os.path.join("../data", "geonb_pan-ncb_shp", "geonb_pan_ncb.shp")
c.register_table("property_assessment_map", geonb_pan_ncb_shp_path)

buildings_path = os.path.join("../data", "geonb_buildings_shp", "geonb_buildings_shp.shp")
c.register_table("buildings", buildings_path)

c.register_udf(myLinearFit, [1, 1])
c.initSchema()

sql = """select myLinearFit(min_g_elev, Sale_Val)
        from property_assessment_map as pam, buildings as b 
        where ST_Contains(pam.geometry,b.geometry)"""

res = c.query(sql)
print(res)
