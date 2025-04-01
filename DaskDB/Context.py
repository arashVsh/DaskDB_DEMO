import geopandas as gpd
from dask.distributed import Client
import dask.dataframe as dd
import dask_geopandas as dgp
from DaskDB.table_information import get_dataframe_info, set_dataframe_info, get_all_table_Names
from DaskDB.supported_func import register_udf, unregister_all_udf, get_udf
from DaskDB.CalcitePlanner import CalcitePlanner as Planner
from DaskDB.CodeGenerator import CodeGenerator
from sql_metadata import Parser

DASK_SCHEDULER_IP='localhost'
DASK_SCHEDULER_PORT=8786


def executeUDF(func, tableName):
    if callable(func):
        udfName = func.__name__
    else:
        udfName = func   

    udf_func, paramList = get_udf(udfName)
    param_list = []
    start = 0
    for val in paramList:
        end = start + val
        df = tableName.iloc[:,start:end]
        param_list.append(df)
        start = val
    params = tuple(param_list)
    m = udf_func(*params)
    return m

def test():
    naturalearth_lowres = get_dataframe_info('naturalearth_lowres')
    l = naturalearth_lowres.compute()
    naturalearth_lowres = naturalearth_lowres[naturalearth_lowres['continent']=='Asia']
    naturalearth_cities = get_dataframe_info('naturalearth_cities')
    merged=naturalearth_lowres.sjoin(naturalearth_cities,predicate='contains')
    merged=merged[['pop_est', 'gdp_md_est']]
    merged=merged.compute()
    merged = executeUDF('myLinearFit',merged)

class Context:

    def setup_configuration(self, hdfsMasterIP=None, hdfsPort=None, daskSchedulerIP=None, daskSchedulerPort=None):
        self.hdfsMasterIP = hdfsMasterIP
        self.hdfsPort = hdfsPort
        self.daskSchedulerIP = daskSchedulerIP
        self.daskSchedulerPort = daskSchedulerPort
        
        if (daskSchedulerIP is not None and daskSchedulerPort is not None):
            scheduler_ip_port = daskSchedulerIP + ':' + str(daskSchedulerPort)
            self.DaskClient = Client(scheduler_ip_port)
            self.DaskClient.restart()
            self.DaskClient.get_versions(check=True)

    def register_table(self, table_name, data_path, delimiter=',', col_names=None):    
        if data_path.startswith('hdfs://'):
            if data_path.endswith('.shp'):
                df = gpd.read_file(data_path, storage_options={'host': self.hdfsMasterIP, 'port': self.hdfsPort})
                ddf = dgp.from_geopandas(df, chunksize=65536)
            elif data_path.endswith('.csv'):
                if col_names is None:
                    ddf = dd.read_csv(data_path, delimiter=delimiter, storage_options={'host': self.hdfsMasterIP, 'port': self.hdfsPort})
                else:    
                    ddf = dd.read_csv(data_path, delimiter=delimiter, names=col_names, storage_options={'host': self.hdfsMasterIP, 'port': self.hdfsPort})
            else:
                raise ValueError(f"Unsupported file type for: {data_path}")
                return None
        
        else:
            if data_path.endswith('.shp'):
                # df = gpd.read_file(data_path)
                # ddf = dgp.from_geopandas(df, chunksize=65536)
                ddf = dgp.read_file(data_path, chunksize=65536)
            elif data_path.endswith('.csv'):
                if col_names is None:
                    ddf = dd.read_csv(data_path, delimiter=delimiter)
                else:    
                    ddf = dd.read_csv(data_path, delimiter=delimiter, names=col_names)
            else:
                raise ValueError(f"Unsupported file type for: {data_path}")
                return None
        
        set_dataframe_info(table_name, ddf)


    def register_udf(self, func, param_count_list):
        register_udf(func, param_count_list)
        
    def unregister_all_udf(self):
        unregister_all_udf()    
        

    def getPlan(self, sql):
        tables_used = Parser(sql).tables
        phyPlan = self.planner.getDaskPlan(sql, tables_used)
        return phyPlan
    
    def initSchema(self):
        self.planner = Planner()
        self.planner.initSchema()
        
    def query(self, sql):
        plan = self.getPlan(sql)
        codeGen = CodeGenerator(plan)
        final_df, stmt = codeGen.generateCode()
        print("\nGenerated Code:\n", stmt)
        print()
        local_vars = {}   # <<< Make a clean local scope
        exec(stmt, globals(), local_vars)  # <<< Safe controlled exec
        x = local_vars[final_df]   # <<< Now you get the dataframe safely
        return x


