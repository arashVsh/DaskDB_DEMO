from DaskDB.Operators.Operator import Operator
from DaskDB.table_information import update_runtime_table_info

class OrderByOperator(Operator):
    def __init__(self, colPosList, ascendingList, allColNamesList):
        super().__init__("OrderBy", allColNamesList)
        self.colPosList = colPosList
        self.ascendingList = ascendingList
        
    def emitCode(self, tabNameList, inputColNameList):
        tableName = tabNameList[0]
        sortColNameList = [inputColNameList[pos] for pos in self.colPosList]
        stmt = tableName + '=' + tableName + '.sort_values(by=' + str(sortColNameList) + ', ascending = ' + str(self.ascendingList) + ')\n'
        if(self.isRoot):
            stmt += self.getComputeStatement(tableName)
        return tableName,stmt, self.allColumnNamesList            