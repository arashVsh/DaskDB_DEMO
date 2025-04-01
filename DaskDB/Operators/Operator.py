class Operator:
    
    def __init__(self, opName, allColumnNamesList):
        self.name=opName
        self.operands_list=[]
        self.allColumnNamesList = allColumnNamesList
        self.isRoot = False
        
    def setRoot(self):
        self.isRoot = True
        
    def getComputeStatement(self, tableName):
        stmt = tableName + '=' + tableName + '.compute()\n'
        return stmt    
        
    
        
        