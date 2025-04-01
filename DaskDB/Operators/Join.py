from DaskDB.Operators.Operator import Operator
from DaskDB.table_information import update_runtime_table_info, get_runtime_column_names, get_runtime_table_info
from DaskDB.supported_func import get_spatial_func_name

class JoinOperator(Operator):
    def __init__(self, joinColPos1, joinColPos2, operator, allColNamesList):
        super().__init__("Join", allColNamesList)
        self.joinColPos1 = joinColPos1
        self.joinColPos2 = joinColPos2
        self.operator = operator
        
    def emitCode(self, tableNameList, inputColNameList):
        outputTableName = 'merged'
        run_time_table_info = get_runtime_table_info()
        if self.joinColPos1 < len(run_time_table_info[tableNameList[0]]):
            joinTable1 = tableNameList[0]
            joinTable2 = tableNameList[1]
        else:
            joinTable1 = tableNameList[1]
            joinTable2 = tableNameList[0]
                
        joinCol1 = inputColNameList[self.joinColPos1]
        joinCol2 = inputColNameList[self.joinColPos2]

        if 'ST_' in self.operator:
            # run_time_table_info = get_runtime_table_info()
            dask_geopanda_spatial_func_name = get_spatial_func_name(self.operator)
            # joinTable1 = ""
            # joinTable2 = ""
            # for tabName, columnList in run_time_table_info.items():
            #     if joinCol1 in columnList:
            #         joinTable1 = tabName
            #     elif joinCol2 in columnList:
            #         joinTable2 = tabName
            #
            #     if(joinTable1 != "" and joinTable2 != ""):
            #         break
            
            stmt = outputTableName + '=' + joinTable1 + ".sjoin(" + joinTable2 + ',predicate=\'' + dask_geopanda_spatial_func_name +'\')\n'
        
        else:
            # joinTable1 = tableNameList[0]
            # joinTable2 = tableNameList[1]
            
            stmt = outputTableName + '=' + joinTable1 + '.merge(' + joinTable2 + ',left_on = \'' + joinCol1 + '\',right_on = \'' + joinCol2 +'\')\n'
        update_runtime_table_info(outputTableName, self.allColumnNamesList)
        return outputTableName,stmt, self.allColumnNamesList            
            