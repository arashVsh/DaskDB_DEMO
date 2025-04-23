from DaskDB.Context import Context
from Util import myLinearFit

geonb_pan_ncb_shp_path = "../data/geonb_pan-ncb_shp/geonb_pan_ncb.shp"
buildings_path = "../data/geonb_buildings_shp/geonb_buildings_shp.shp"

c = Context()
c.register_table("property_assessment_map", geonb_pan_ncb_shp_path)
c.register_table("buildings", buildings_path)
c.register_udf(myLinearFit, [1, 1])
c.initSchema()

sql = """select myLinearFit(min_g_elev, Sale_Val)
        from property_assessment_map as pam, buildings as b 
        where ST_Contains(pam.geometry,b.geometry)"""

res = c.query(sql)
print(res)
