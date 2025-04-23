from DaskDB.Context import Context

building_path = '../data/geonb_buildings_shp/geonb_buildings_shp.shp'
floodriskareas_path = '../data/geonb_floodriskareas_shp/Shapefiles/Flood_Hazard_Areas.shp'

c = Context()
c.register_table('buildings', building_path)
c.register_table('flood_risk_areas', floodriskareas_path)
c.initSchema()

sql = """select BuildingID
        from buildings as b, flood_risk_areas as fra 
        where ST_Contains(fra.geometry, b.geometry)"""

res = c.query(sql)
print(res)
