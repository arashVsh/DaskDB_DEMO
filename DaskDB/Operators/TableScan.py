from DaskDB.Operators.Operator import Operator
from DaskDB.table_information import update_runtime_table_info

class TableScanOperator(Operator):
    def __init__(self,tableName, allColNamesList):
        super().__init__("TableScan", allColNamesList)
        self.tableName = tableName
        
    def emitCode(self, tabNameList, inputColNameList):
        stmt = self.tableName + " = get_dataframe_info('" + self.tableName + "')\n"
        update_runtime_table_info(self.tableName, self.allColumnNamesList)
        return self.tableName, stmt, self.allColumnNamesList     