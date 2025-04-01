from DaskDB.Operators.Operator import Operator
from DaskDB.table_information import update_runtime_table_info
from DaskDB.supported_func import get_spatial_func_name, isValidUDF

class ProjectOperator(Operator):
    def __init__(self, operatorName, projectedColPosList, allColumnNameList):
        super().__init__("Project", allColumnNameList)
        self.operatorName = operatorName
        self.projectedColPosList = projectedColPosList    

    def emitCode(self, tabNameList, inputColNameList):
        tableName = tabNameList[0]
        projectedColNameList = [inputColNameList[pos] for pos in self.projectedColPosList]
        stmt = tableName + '=' + tableName + '[' + str(projectedColNameList) + ']\n'

        if ('ST_' in self.operatorName):
            spatialOperator = get_spatial_func_name(self.operatorName)
            stmt += tableName + '=' + tableName + "." + spatialOperator + "\n"

        if(self.isRoot):
            stmt += self.getComputeStatement(tableName)

        if(self.operatorName != "" and 'ST_' not in self.operatorName):
            if isValidUDF(self.operatorName):
                stmt += tableName + " = executeUDF('" + self.operatorName + "'," + tableName + ")\n"
            else:
                raise ValueError(self.operatorName + " is not a registered UDF")    

        update_runtime_table_info(tableName, self.allColumnNamesList)
        return tableName,stmt,self.allColumnNamesList                