from DaskDB.Operators.Operator import Operator
from DaskDB.table_information import update_runtime_table_info

class LimitOperator(Operator):
    def __init__(self, numRowsToFetch, allColNamesList):
        super().__init__("Limit", allColNamesList)
        self.numRowsToFetch = numRowsToFetch
        
    def emitCode(self, tabNameList, inputColNameList):
        tableName = tabNameList[0]
        stmt = tableName + '=' + tableName + '.head(' + self.numRowsToFetch + ', compute=False)\n'

        if(self.isRoot):
            stmt += self.getComputeStatement(tableName)

        update_runtime_table_info(tableName, self.allColumnNamesList)
        return tableName,stmt, self.allColumnNamesList                   