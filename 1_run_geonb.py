from DaskDB.Context import Context, DASK_SCHEDULER_IP, DASK_SCHEDULER_PORT
import os

c = Context()
c.setup_configuration(daskSchedulerIP=DASK_SCHEDULER_IP, daskSchedulerPort=DASK_SCHEDULER_PORT)

property_assessment_path = os.path.join('../data', 'geonb_pan-ncb_shp', 'geonb_pan_ncb.shp')
c.register_table('property_assessment_map', property_assessment_path)

c.initSchema()

sql = """select ST_Boundary(geometry)
        from property_assessment_map 
        where Descript = 'RESIDENCE & LOT'"""

res = c.query(sql)
print(res)
