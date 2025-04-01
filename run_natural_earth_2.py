from DaskDB.Context import Context
from Util import plotBarGraph

c = Context()
c.setup_configuration(daskSchedulerIP='localhost', daskSchedulerPort=8786)
c.register_table('naturalearth_lowres', '/home/suvam/eclipse-workspace/SpatialDaskDB_SSTD_DEMO_2/data/naturalearth_lowres/naturalearth_lowres.shp')
c.register_table('naturalearth_cities', '/home/suvam/eclipse-workspace/SpatialDaskDB_SSTD_DEMO_2/data/naturalearth_cities/naturalearth_cities.shp')
c.register_udf(plotBarGraph, [2])
c.initSchema()

#This query calculates the total population for each continent and creates a bar chart
sql = """select l.name, pop_est
    from naturalearth_lowres as l, naturalearth_cities as c 
    where ST_Contains(l.geometry,c.geometry)
    limit 10"""

res = c.query(sql)
print(res)
