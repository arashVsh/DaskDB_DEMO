class CodeGenerator:
    def __init__(self, daskPlan):
        self.daskPlan = daskPlan
        
    def generateCode(self):
        daskPlan = self.daskPlan
        final_df,exeCode,_ = self.visit(daskPlan)
        return final_df, exeCode
            
    def visit(self, operator):
        stmt = ""
        tabNameList = []
        inputColNameList = []
        for operands in operator.operands_list:
            tabName, line, colNameList = self.visit(operands)
            stmt += line
            tabNameList.append(tabName)
            inputColNameList += colNameList
        tableName, s, outputColNameList = operator.emitCode(tabNameList, inputColNameList)
        return tableName, stmt+s, outputColNameList     
                