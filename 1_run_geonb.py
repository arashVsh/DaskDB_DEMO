from DaskDB.Context import Context

property_assessment_path = '../data/geonb_pan-ncb_shp/geonb_pan_ncb.shp'
c = Context()
c.register_table('property_assessment_map', property_assessment_path)
c.initSchema()

sql = """select ST_Boundary(geometry)
        from property_assessment_map 
        where Descript = 'RESIDENCE & LOT'"""

res = c.query(sql)
print(res)
