spatial_func_dict = {
    'ST_CONVEXHULL':'convex_hull', 
    'ST_CONTAINS':'contains',
    'ST_CONTAINSPROPERLY':'contains_properly',
    'ST_COVEREDBY' : 'covered_by',
    'ST_COVERS' : 'covers',
    'ST_CROSSES' : 'crosses',
    'ST_INTERSECTS' : 'intersects',
    'ST_OVERLAPS' : 'overlaps',
    'ST_TOUCHES' : 'touches',
    'ST_WITHIN' : 'within',
    'ST_AREA' : 'area',
    'ST_BOUNDARY' : 'boundary',
    'ST_EXTENT' : 'bounds',
    'ST_LENGTH' : 'length'
    }

agg_func_dict = {
    'SUM' : 'sum',
    'AVG' : 'mean',
    'MAX' : 'max',
    'MIN' : 'min'
    }

udf_dict = {}

def get_spatial_func_name(udf_name):
    udf_name = udf_name.upper()
    return spatial_func_dict[udf_name]

    
def register_udf(func, param_count_list):
    d={}
    d['func'] = func
    d['param_count_list'] = param_count_list
    udf_dict[func.__name__] = d
    
def unregister_all_udf():
    udf_dict.clear()    
    
def get_udf(udf_name):
    try:
        d = udf_dict[udf_name]
        return d['func'], d['param_count_list']
    except KeyError:    
        raise ValueError("UDF " + udf_name + "is not registered")
    
def get_udf_dict():
    return udf_dict

def isValidUDF(udfName):
    return (udfName in udf_dict.keys())

def getSupportedAggFunc(funcName):
    return agg_func_dict[funcName.upper()]
    
