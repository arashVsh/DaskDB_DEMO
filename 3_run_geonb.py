from DaskDB.Context import Context

anb_addresses_path = '../data/geonb_anb_shp/geonb_anb_addresses.shp'
floodriskareas_path = '../data/geonb_floodriskareas_shp/Shapefiles/Flood_Hazard_Areas.shp'

c = Context()
c.register_table('address_new_brunswick', anb_addresses_path)
c.register_table('flood_risk_areas', floodriskareas_path)
c.initSchema()

sql = """select ADDR_DESC
        from address_new_brunswick as anb, flood_risk_areas as fra
        where ST_Intersects(fra.geometry, anb.geometry) and STREET = 'ROUTE 105'"""

res = c.query(sql)
print(res)
