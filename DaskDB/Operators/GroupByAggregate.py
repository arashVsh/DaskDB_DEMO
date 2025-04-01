from DaskDB.Operators.Operator import Operator
from DaskDB.table_information import update_runtime_table_info

class GroupByAggOperator(Operator):
    def __init__(self, GroupByColPosList, aggCallInfo, allColNamesList):
        super().__init__("GroupByAgg", allColNamesList)
        self.GroupByColPosList = GroupByColPosList
        self.aggCallInfo = aggCallInfo
        

    def emitCode(self, tabNameList, inputColNameList):
        tableName = tabNameList[0]
        groupByColumns = [inputColNameList[pos] for pos in self.GroupByColPosList]
        stmt = tableName + " = " + tableName + '.groupby(' + str(groupByColumns) + ')\n'
        
        aggDict = {}
        namedColDict = {}
        for namedCol, aggDetails in self.aggCallInfo.items():
            aggFunc = list(aggDetails.keys())[0]
            argColPos = aggDetails[aggFunc]
            colName = inputColNameList[argColPos]
            aggDict[colName]=aggFunc
            namedColDict[colName] = namedCol
        
        if len(aggDict) > 0:
            stmt += tableName + " = " + tableName + '.agg(' + str(aggDict) + ')\n'
            stmt += tableName + " = " + tableName + '.rename(columns=' + str(namedColDict) + ')\n'
            stmt += tableName + " = " + tableName + '.reset_index()\n'
        
        update_runtime_table_info(tableName, self.allColumnNamesList)
        return tableName,stmt,self.allColumnNamesList            