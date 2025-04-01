tables_sizes = {}
table_division = {}
table_dataframes = {}
run_time_table_info = {}


def set_table_size(df_string, partition_size):
    tables_sizes[df_string] = partition_size
    if not df_string in table_division:
        table_division[df_string] = {}

def get_table_size(df_string):
    return tables_sizes[df_string]

def good_to_set_index(df_string):
    if df_string in tables_sizes:
        if tables_sizes[df_string] > 3:
            return True
        return True

def set_table_division(df_string,df_col,division):
    table_division[df_string][df_col] = division


def get_table_division(df_string,df_col):
    if df_col in table_division[df_string]:
        return table_division[df_string][df_col]
    return None

def print_table_sizes():
    print(tables_sizes)
    
def print_table_division():
    print(table_division)

def set_dataframe_info(relName, ddf):
    if not relName in table_dataframes:
        table_dataframes[relName] = ddf

def get_dataframe_info(relName):
    if not relName in table_dataframes:
        return None
    return table_dataframes[relName]

def get_all_table_Names():
    tableNamesList = list(table_dataframes.keys())
    return tableNamesList

def update_runtime_table_info(tableName, colList):
    run_time_table_info[tableName] = colList
    
def get_runtime_column_names(tableName):
    return run_time_table_info[tableName]

def get_runtime_table_info():
    return run_time_table_info
