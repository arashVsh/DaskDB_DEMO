from DaskDB.Context import Context, DASK_SCHEDULER_IP, DASK_SCHEDULER_PORT
from Util import myLinearFit
import os

c = Context()
c.setup_configuration(daskSchedulerIP=DASK_SCHEDULER_IP, daskSchedulerPort=DASK_SCHEDULER_PORT)

naturalearth_lowres_path = os.path.join("../data", "naturalearth_lowres", "naturalearth_lowres.shp")
c.register_table('naturalearth_lowres', naturalearth_lowres_path)

naturalearth_cities_path = os.path.join("../data", "naturalearth_cities", "naturalearth_cities.shp")
c.register_table('naturalearth_cities', naturalearth_cities_path)

c.register_udf(myLinearFit, [1,1])
c.initSchema()

sql = """select myLinearFit(pop_est, gdp_md_est)
        from naturalearth_lowres as l, naturalearth_cities as c 
        where ST_Contains(l.geometry,c.geometry) and continent = 'Asia'"""

res = c.query(sql)
print(res)
