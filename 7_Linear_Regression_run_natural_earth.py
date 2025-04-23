from DaskDB.Context import Context
from Util import myLinearFit

naturalearth_lowres_path = "../data/naturalearth_lowres/naturalearth_lowres.shp"
naturalearth_cities_path = "../data/naturalearth_cities/naturalearth_cities.shp"

c = Context()
c.register_table('naturalearth_lowres', naturalearth_lowres_path)
c.register_table('naturalearth_cities', naturalearth_cities_path)
c.register_udf(myLinearFit, [1,1])
c.initSchema()

sql = """select myLinearFit(pop_est, gdp_md_est)
        from naturalearth_lowres as l, naturalearth_cities as c 
        where ST_Contains(l.geometry,c.geometry) and continent = 'Asia'"""

res = c.query(sql)
print(res)
