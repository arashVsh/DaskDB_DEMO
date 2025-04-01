from DaskDB.Operators.Operator import Operator
from DaskDB.table_information import update_runtime_table_info

class FilterOperator(Operator):
    def __init__(self, opType, colPos, value, allColNamesList):
        super().__init__("Filter", allColNamesList)
        self.operator = opType
        self.colPos = colPos
        self.value = value
        
    def emitCode(self, tabNameList, inputColNameList):
        colName = inputColNameList[self.colPos]
        tableName = tabNameList[0]
        if(self.operator == '='):
            opType = '=='
        else:
            opType = self.operator    
        stmt = tableName + " = " + tableName + "[" + tableName + "['" + colName + "']" + opType + self.value +"]\n"
        update_runtime_table_info(tableName, self.allColumnNamesList)
        return tableName,stmt,self.allColumnNamesList