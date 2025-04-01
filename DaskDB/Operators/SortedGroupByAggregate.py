from DaskDB.Operators.Operator import Operator

class SortedGroupByAggOperator(Operator):
    def __init__(self, GroupByColNameList, AggFuncName=""):
        super().__init__("SortedGroupByAgg")
        self.GroupByColNameList = GroupByColNameList
        self.AggFuncName = AggFuncName

    def emitCode(self, tableName):
        return "",""            