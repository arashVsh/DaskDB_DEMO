from DaskDB.Context import Context

property_assessment_path = "../data/geonb_pan-ncb_shp/geonb_pan_ncb.shp"
floodriskareas_path = (
    "../data/geonb_floodriskareas_shp/Shapefiles/Flood_Hazard_Areas.shp"
)

c = Context()
c.register_table("property_assessment_map", property_assessment_path)
c.register_table("flood_risk_areas", floodriskareas_path)
c.initSchema()

sql = """select Location, avg(sale_val) as avg_val
        from property_assessment_map as pam, flood_risk_areas as fra 
        where ST_Within(pam.geometry,fra.geometry) and sale_val > 0
        group by Location
        order by avg_val desc
        limit 10"""

res = c.query(sql)
print(res)
