import logging
import os
import platform
import warnings 
import json

import jpype
import jpype.imports

from DaskDB.table_information import get_dataframe_info, get_all_table_Names
from DaskDB.Operators.Filter import FilterOperator
from DaskDB.Operators.GroupByAggregate import GroupByAggOperator
from DaskDB.Operators.SortedGroupByAggregate import SortedGroupByAggOperator
from DaskDB.Operators.Join import JoinOperator
from DaskDB.Operators.Limit import LimitOperator
from DaskDB.Operators.Orderby import OrderByOperator
from DaskDB.Operators.Project import ProjectOperator
from DaskDB.Operators.TableScan import TableScanOperator
from DaskDB.supported_func import getSupportedAggFunc, get_udf_dict

from builtins import isinstance
import re

logger = None

def initLogger():
    global logger
    logFormatter = logging.Formatter("%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s")
    logger = logging.getLogger(__name__)
    
    logger.setLevel(logging.DEBUG) # Logs everything 
    
    consoleHandler = logging.StreamHandler()
    consoleHandler.setFormatter(logFormatter)
    
    logger.addHandler(consoleHandler)
        
def initJVM():
    
    # from pyarrow.hdfs import _maybe_set_hadoop_classpath
    global logger
    
    def set_or_check_java_home():
        """
        We have some assumptions on the JAVA_HOME, namely our jvm comes from
        a conda environment. That does not need to be true, but we should at
        least warn the user.
        """
        if "CONDA_PREFIX" not in os.environ:  # pragma: no cover
            # we are not running in a conda env
            return
    
        correct_java_path = os.path.normpath(os.environ["CONDA_PREFIX"])
        if platform.system() == "Windows":  # pragma: no cover
            correct_java_path = os.path.normpath(os.path.join(correct_java_path, "Library"))
    
        if "JAVA_HOME" not in os.environ:  # pragma: no cover
            logger.debug("Setting $JAVA_HOME to $CONDA_PREFIX")
            os.environ["JAVA_HOME"] = correct_java_path
        elif (
            os.path.normpath(os.environ["JAVA_HOME"]) != correct_java_path
        ):  # pragma: no cover
            warnings.warn(
                "You are running in a conda environment, but the JAVA_PATH is not using it. "
                f"If this is by mistake, set $JAVA_HOME to {correct_java_path}, instead of {os.environ['JAVA_HOME']}."
            )
                
    #set_or_check_java_home()
    #_maybe_set_hadoop_classpath()
    
    jvmpath = jpype.getDefaultJVMPath()
    logger.debug(f"Starting JVM from path {jvmpath}")
    jvmArgs = [
        "-ea",
        "--illegal-access=deny",
    ]

    calciteJarsPath = "DaskDB/lib/*"

    # defaultClassPath = jpype.getClassPath()
    # absPath = os.path.abspath("DaskDB/lib/")
    # allJars = os.listdir("DaskDB/lib/")
    # classPath = defaultClassPath
    # for jar in allJars:
    #     classPath += ':' + absPath + '/' + jar
    
    
    jpype.startJVM(
        *jvmArgs,
        ignoreUnrecognized=True,
        convertStrings=False,
        jvmpath=jvmpath,
        classpath = calciteJarsPath # customClassPath
    )
    
    logger.debug("JVM Initialized...")
    
initLogger()
initJVM()

org = jpype.JPackage("org")    
    
from daskDBPlanner import DaskDBPlanner
from org.apache.calcite.adapter.enumerable import EnumerableSort
from org.apache.calcite.adapter.enumerable import EnumerableSortedAggregate
from org.apache.calcite.adapter.enumerable import EnumerableConvention
from org.apache.calcite.adapter.enumerable import EnumerableFilter
from org.apache.calcite.adapter.enumerable import EnumerableHashJoin
from org.apache.calcite.adapter.enumerable import EnumerableLimit
from org.apache.calcite.adapter.enumerable import EnumerableNestedLoopJoin
from org.apache.calcite.adapter.enumerable import EnumerableProject
from org.apache.calcite.rex import RexCall
from org.apache.calcite.rex import RexLiteral
from org.apache.calcite.rex import RexNode
from org.apache.calcite.rex import RexVariable

#jpype.shutdownJVM()

            
class CalcitePlanner:
    
    CONST_CSV_DIR = "csv_dir" # Directory where the CSVs are, and where the "schema" file needs to be written
    CONST_CSV_delim = ','
    CONST_MODEL_FILNAME = "demo_model.json" # Filename of calcite compatible model
    CONST_SCHEMA_name = "demo_schema"
    calcite_dtype = {
        'int64' : 'BIGINT',
        'float64' : 'DOUBLE',
        'datetime64[ns]' : 'TIMESTAMP',
        'datetime64[s]' : 'TIMESTAMP',
        'object' : 'VARCHAR',
        'geometry' : 'VARCHAR'  #'GEOMETRY'
        }
    JSON_FILE_URI = ""
    allColumnNames = []
    
    def initSchema(self):

        metadata_info = {}
        tables_used = get_all_table_Names()
        all_table_info = self.get_all_table_info(tables_used)
        all_udf_info = self.get_all_UDF_info()
        metadata_info['tables'] = all_table_info
        metadata_info['udf'] = all_udf_info
        
        # Generate filename 
        calcite_CSV_schema_model_JSON_URI = os.getcwd() + '/' + self.CONST_MODEL_FILNAME
        
        # Write JSON
        with open(calcite_CSV_schema_model_JSON_URI, 'w', encoding='utf-8') as json_writer_f:
            json.dump(metadata_info, json_writer_f, ensure_ascii = False, indent = 4)
        
        self.JSON_FILE_URI = calcite_CSV_schema_model_JSON_URI    

    
    def get_all_table_info(self, tables_used):     
        all_table_info = []
        for table in tables_used:
            single_table_info = {}
            single_table_info['tablename'] = table
            df = get_dataframe_info(table)
            col_names = list(df.columns)
            self.allColumnNames += col_names
            
            col_with_dtype = []
            for col in col_names:
                _type = str(df[col].dtype)
                if _type.startswith('datetime64'):
                    calcite_dtype = 'TIMESTAMP'
                else:
                    calcite_dtype = self.calcite_dtype.get(_type, 'VARCHAR')  # fallback to VARCHAR for unknown types
                # calcite_dtype = self.calcite_dtype[_type]
                single_col_info = {}
                single_col_info['fieldname'] = col
                single_col_info['type'] = calcite_dtype 
                col_with_dtype.append(single_col_info)
            
            single_table_info['column_list'] = col_with_dtype
            single_table_info['row_count'] = str(len(df))
            
            all_table_info.append(single_table_info)
        return all_table_info
    
    def get_all_UDF_info(self):
        udf_dict_info = get_udf_dict()
        udf_dict = {}
        for udfName, d in udf_dict_info.items():
            udf_dict[udfName] = sum(d['param_count_list'])
        
        all_udf_info = []
        for udfName in udf_dict.keys():
            single_udf_info = {}
            single_udf_info['udfName'] = udfName
            single_udf_info['numParams'] = str(udf_dict[udfName])
            all_udf_info.append(single_udf_info)
        
        return all_udf_info    

    def getPhysicalPlan(self, queryString, tables_used):
        planner = DaskDBPlanner()
        filePath = self.JSON_FILE_URI
        phyPlan = planner.getPhysicalPlan(filePath, queryString)
        return phyPlan
    
    def getDaskPlan(self, queryString, tables_used):
        calcitePlan = self.getPhysicalPlan(queryString, tables_used)
        daskPlan = self.visitCalcitePlan(calcitePlan)
        daskPlan.setRoot()
        return daskPlan
    
    def visitCalcitePlan(self, calciteNode):
        daskDBNode = self.getDaskPlanNode(calciteNode)
        inputs = list(calciteNode.getInputs())
        for node in inputs:
            next_node = self.visitCalcitePlan(node)
            daskDBNode.operands_list.append(next_node)
        return daskDBNode     
        
        
    def getDaskPlanNode(self, node):
        opType = node.getRelTypeName()
        colNames = list(node.getRowType().getFieldNames()) #column names appears as Java String
        colNames = [str(col) for col in colNames]    
        if opType == "EnumerableTableScan":
            table = node.getTable()
            tableName = str(table.getQualifiedName().get(1))
            daskNode = TableScanOperator(tableName, colNames)

        elif opType == "EnumerableFilter":
            rxCall = node.getCondition()
            opType = str(rxCall.getOperator().getName())
            rxNodeList = list(rxCall.getOperands())
            for rxNode in rxNodeList:
                if isinstance(rxNode, RexCall):
                    raise ValueError("Operation not supported yet")
                elif isinstance(rxNode, RexVariable):
                    op = str(rxNode.getName())   #e.g. $0
                    colPos = int(re.findall("[0-9]+", op)[0]) 
                else:
                    literal = str(rxNode.toString()).split(':')
                    literal = literal[0].strip('\"') 
            daskNode = FilterOperator(opType, colPos, literal, colNames)

        elif opType == "EnumerableHashJoin" or opType == "EnumerableNestedLoopJoin":
            rxCall = node.getCondition()
            opType = str(rxCall.getOperator().getName())
            rxNodeList = list(rxCall.getOperands())
            joinColPos = []
            for rxNode in rxNodeList:
                op = self.getOperatorfromRexNodeTree(rxNode)   #e.g. $0
                pos = int(re.findall("[0-9]+", op)[0]) 
                joinColPos.append(pos)
            daskNode = JoinOperator(joinColPos[0], joinColPos[1], opType, colNames)

        elif opType == "EnumerableProject":
            rxNodeList = list(node.getProjects());
            projectedColPosList = []
            operatorName=""
            for rxNode in rxNodeList:
                if isinstance(rxNode, RexCall): 
                    operatorName = str(rxNode.getOperator().getName())
                    operandsList = list(rxNode.getOperands())
                    for op in operandsList:
                        op = str(op.toString())
 
                        #Sometime the column appears as "CAST($0):VARCHAR NOT NULL"
                        #so we use regular expression to extract "0" as column ID
                        pos = int(re.findall("[0-9]+", op)[0]) 
                        projectedColPosList.append(pos)
                elif isinstance(rxNode, RexVariable):
                    op = str(rxNode.getName().toString())
                    pos = int(re.findall("[0-9]+", op)[0]) 
                    projectedColPosList.append(pos)
                else :
                    literal = rxNode.getValue()            
            daskNode = ProjectOperator(operatorName, projectedColPosList, colNames);

        elif opType == "EnumerableAggregate" or opType == "EnumerableSortedAggregate":
            groupSets = str(node.groupSets.toString())
            colPosList = re.findall("[0-9]+", groupSets)
            GroupByColPosList = [int(pos) for pos in colPosList]

            namedAggCallList = list(node.getNamedAggCalls())
            aggCallInfo = {}
            for aggCall in namedAggCallList:
                namedCol = str(aggCall.getValue())
                aggFunc = str(aggCall.getKey().getAggregation().getName())
                aggFunc = getSupportedAggFunc(aggFunc)
                argColPos = list(aggCall.getKey().getArgList())[0]
                d = {}
                d[aggFunc] = argColPos
                aggCallInfo[namedCol] = d
            daskNode = GroupByAggOperator(GroupByColPosList, aggCallInfo, colNames)

        elif opType == "EnumerableSort": 
            collationList = list(node.getCollation().getFieldCollations())
            sortExpsList = list(node.getSortExps())
            
            #As of now sorting on a single column is only supported in Dask Dataframe (version 2022.2.0)
            if(len(collationList) > 1):
                raise ValueError("As of As of now sorting on a single column is only supported") 
            
            colPosList = []
            ascendingList = []
            for i in range(len(sortExpsList)):
                colPos = str(sortExpsList[i].toString()).replace('$','')
                colPos = int(colPos)
                direction = str(collationList[i].getDirection().toString())
                isAscending = False
                if direction.lower() == 'ascending':
                    isAscending = True
                colPosList.append(colPos)
                ascendingList.append(isAscending)
            daskNode = OrderByOperator(colPosList, ascendingList, colNames)

        elif opType == "EnumerableLimit":
            numRowsToFetch = str(node.fetch.toString())
            daskNode = LimitOperator(numRowsToFetch, colNames)
        else:
            raise ValueError("New Operation Encountered")
        
        return  daskNode
    
    
    def getOperatorfromRexNodeTree(self, rxNode):
        if isinstance(rxNode, RexCall):
            for child in list(rxNode.getOperands()):
                return self.getOperatorfromRexNodeTree(child)
        elif isinstance(rxNode, RexVariable):
            op = str(rxNode.getName())   #e.g. $0
            return op
        else:
            raise ValueError("Literal Encountered in Rex Node Tree") 
                                   
        
            
