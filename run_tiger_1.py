from DaskDB.Context import Context
import matplotlib.pyplot as plt

def myPlot(df):
    df.plot()
    plt.show()

c = Context()
c.setup_configuration(daskSchedulerIP='localhost', daskSchedulerPort=8786)

c.register_table('arealm', '/home/suvam/eclipse-workspace/SpatialDaskDB_SSTD_DEMO_2/data/arealm/arealm.shp')
c.register_table('edges', '/home/suvam/eclipse-workspace/SpatialDaskDB_SSTD_DEMO_2/data/edges/edges.shp')
c.register_udf(myPlot, [2])

c.initSchema()

sql = """select 
    ST_CONVEXHULL(a.geometry)
from  
    arealm as a, edges as e
where 
    ST_Contains(a.geometry,e.geometry)"""

res = c.query(sql)
print(res)
