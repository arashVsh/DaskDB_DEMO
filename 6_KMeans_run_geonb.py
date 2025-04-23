from DaskDB.Context import Context
from Util import myKMeans

anb_addresses_path = "../data/geonb_anb_shp/geonb_anb_addresses.shp"
floodriskareas_path = (
    "../data/geonb_floodriskareas_shp/Shapefiles/Flood_Hazard_Areas.shp"
)

c = Context()
c.register_table("address_new_brunswick", anb_addresses_path)
c.register_table("flood_risk_areas", floodriskareas_path)
c.register_udf(myKMeans, [2])
c.initSchema()

sql = """select myKMeans(Latitude,Longitude)
        from address_new_brunswick as anb, flood_risk_areas as fra
        where ST_Contains(fra.geometry, anb.geometry)"""

res = c.query(sql)
print(res)
