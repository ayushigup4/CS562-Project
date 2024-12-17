import subprocess
import os
import psycopg2
import psycopg2.extras
import tabulate
from dotenv import load_dotenv
import pandas as pd
import datetime

    
class mf_struct:
    def __init__(self):
        self.s = [] # projected values
        self.n = 0 # number of grouping variables
        self.v = [] # group by attributes
        self.F = [] # list of aggregates
        self.sigma = [] # grouping variables predicates 
        self.G = None # having clause

mf_struct = mf_struct()
        
def schema_info():
    """
    This retrieves schema infomation from the database using 'information_schema.columns'
    """
    # hard coded query to send to database
    query = F"""
    SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'public' and TABLE_NAME = 'sales'
    """
    load_dotenv()

    user = os.getenv('USER')
    password = os.getenv('PASSWORD')
    dbname = os.getenv('DBNAME')

    conn = psycopg2.connect("dbname="+dbname+" user="+user+" password="+password,
                                cursor_factory=psycopg2.extras.DictCursor)
    cur = conn.cursor()
    cur.execute(query)
    data = cur.fetchall()
    conn.close()
    return data # return schema data   

def read_file(filename):
    """
    Reads an Extended SQL query and extracts its operands
    """
    with open(filename, 'r') as file:
        lines = file.readlines()
        for line in lines:
            line = line.lower().strip() # make everything lowercase

        # initalize vars
        select = ""
        From = ""
        where = ""
        group_by = ""
        such_that = ""
        having = ""

        # Extract info
        if len(lines) > 0:
            select = lines[0][7:].strip().lower()
        if len(lines) > 1:
            From = lines[1][5:].strip().lower()
        if len(lines) > 2:
            if "where" in lines[2].lower():
                where = lines[2][6:].strip().lower()
            elif "group" in lines[2].lower():
                group_by = lines[2][9:].strip().lower()
        if len(lines) > 3:
            if "group" in lines[3].lower():
                group_by = lines[3][9:].strip().lower()
            elif "such that" in lines[3].lower():
                such_that = lines[3][10:].strip().lower()
            elif "having" in lines[3].lower():
                having = lines[3][7:].strip().lower()
        if len(lines) > 4:
            if "such that" in lines[4].lower():
                such_that = lines[4][10:].strip().lower()
            elif "having" in lines[4].lower():
                having = lines[4][7:].strip().lower()
    return select, From, where, group_by, such_that, having

def process_info(select, group_by, such_that, having, mf_struct, schemaData):
    """
    This function processes an MF query and extracts its information,
    populating mf_struct with 6 operands of Phi
    """
    V = [] # list of grouping attributes
    F_VECT = [] # list of aggregate functions

    # potential aggregates
    aggregates = ["sum", "count", "avg", "min", "max"]
        
    # potential group by attributes
    groupAttrs = ["cust", "prod", "day", "month", "year", "state", "quant", "date"]
    
    # add projected vals to struct.s
    mf_struct.s = select 

    # add group_by vars to mf_struct.v
    group_by_vars = []
    if len(such_that) == 0:
        group_by = group_by.split(",") # normal SQL query (no such that conditions, group by seperated by comma)
    else: 
        group_by = group_by.strip().split(":") # ESQL query such that has such that conditions and group by seperated by :
        group_by_vars = group_by[1].strip().split(", ") # get only group by vars like X, Y, Z
        group_by = group_by[0].strip().split(", ") # get only the group bys
        # add group by predicates to mf_struct.sigma
        such_that = such_that.split(",")
        for predicate in such_that:
            predicate = predicate.replace('=', '==')
            mf_struct.sigma.append(predicate.strip()) # append gv predicate to struct.sigma

    # replace mf_struct.G with having clause if it exists 
    if (having):
        mf_struct.G = having 

    # split projected values        
    select = select.split(",")

    # from column name, data_type and max_char_length in schema
    for col_name, data_type, max_len in schemaData:
        if max_len == None: max_len = 0
        # iterate through each projected value
        for sel in select:
            sel = sel.strip()

            # Check if sel is possible aggregate function
            if '(' in sel and ')' in sel:                    
                left = sel.index('(')
                right = sel.index(')')
                aggregate = sel[:left].strip() # aggregate function name
                arg = sel[left + 1:right].strip() # argument name
                
                if col_name.lower() in arg and aggregate.lower() in aggregates:
                    mf_struct.F.append(sel) # append aggregates to struct.F
                    F_VECT.append({
                        "agg": sel,
                        "type": data_type,
                        "arg": arg,
                        "func": aggregate
                    })
                    continue
        # iterate through each group by
        for group in group_by:
            group = group.strip()
            if group in col_name.lower() and group in groupAttrs:
                mf_struct.v.append(group) # append gb attribute to struct.v            
                V.append({
                    "attrib": group,
                    "type": data_type,
                    "size": max_len,
                })
    # add number of gvs to mf_struct.n
    mf_struct.n = len(F_VECT)
    """    
    print("struct {")
    # Print out the grouping attributes (V)
    for i, v in enumerate(V):
        print(f"    {v['type']} {v['attrib']}[{v['size']}];")
    # Print out the aggregates (F_VECT)
    for f in F_VECT:
        print(f"    {f['type']} {f['agg']};")
    print("} mf_struct[500];")
    """
    return group_by_vars, V, F_VECT

def process_conditions(mf_struct, group_by_vars):
    """
    Processes the such that conditions in mf_struct, seperating them
    by group var, column name, and its condition
    """
    # Loop through each predicate in sigma (such_that conditions)
    operators = ['==', '!=', '>', '<']
    conditions = {gv: [] for gv in group_by_vars}
    for predicate in mf_struct.sigma:
        for op in operators:
            if op in predicate:
                # Split predicate to get the gv value, column name and condition value
                gv_col , cond = predicate.strip().split(op)
                gv, col = gv_col.strip().split('.')
                gv = gv.strip()
                conditions[gv]
                col = col.strip()
                cond = cond.strip()
                if cond.isdigit():
                    cond = int(cond)
                else: cond = str(cond).upper()
                conditions[gv].append({ # gv: group variable such as X, Y, Z
                    'col': col,     # column name of the row
                    'op': op,       # possible operator
                    'cond': cond    # the condition to compare
                })
    return conditions

def eval_conditions(row, conditions, f):
    """
    evaluates each such that condition to filter out rows depending on the condition
    """
    col_name = ["cust", "prod", "day", "month", "year", "state", "quant", "date"]
    
    # match aggregate arg
    gv = f['arg'].split('.')[0]
    #print(conditions[gv][0]['col'])

    # get index of the row
    row_index = col_name.index(conditions[gv][0]['col'])
    
    # get the value of the row to perform the condition
    row_value = row[row_index]

    condition = conditions[gv][0]["cond"]

    # make sure if value is a string, date or int to treat it as such
    # if a string make sure it is being passed as a string literal
    if isinstance(row_value, str) or isinstance(row_value, datetime.date):
        row_value = f"'{row_value}'"
    
    if isinstance(condition, str) or isinstance(condition, datetime.date):
        condition = f"'{condition}'"

    #print(f"{row_value} {conditions[gv][0]['op']} {condition}")
    if eval(f"{row_value} {conditions[gv][0]['op']} {condition}"):
        return True
    else: return False


schemaData = schema_info()

select, From, where, group_by, such_that, having = read_file("MFQuery3.txt")
    
group_by_vars, V, F_VECT = process_info(select, group_by, such_that, having, mf_struct, schemaData)

"""
print('\n Phi Operators of the Query')
print("s = ", mf_struct.s)
print("n = ", mf_struct.n)
print("v = ", mf_struct.v)
print("F = ", mf_struct.F)
print("sigma = ", mf_struct.sigma)
print("G = ", mf_struct.G)
"""
def H_table(where, such_that, having, group_by_vars, F_VECT, mf_struct): 
    load_dotenv()
    user = os.getenv('USER')
    password = os.getenv('PASSWORD')
    dbname = os.getenv('DBNAME')

    conn = psycopg2.connect("dbname="+dbname+" user="+user+" password="+password,
                                    cursor_factory=psycopg2.extras.DictCursor)
    cur = conn.cursor()

    QUERY = f"SELECT * FROM sales"
    if len(where) != 0:
        QUERY += f" WHERE {where}"
                
    cur.execute(QUERY)

    # initalize H table with headers
    H = pd.DataFrame(None, columns=mf_struct.v)
    for col in mf_struct.F:
        H[col] = None

    col_name = ["cust", "prod", "day", "month", "year", "state", "quant", "date"]
    unique = [] # store unique combos based on group by vars
    agg_values = {f['agg']: {} for f in F_VECT} 
    row_combo = tuple()
    # agg_values = {f['agg'] : {} } will hold dictionary of group by values with their corresponding data based on other aggregates
    # example: {'avg(quant)' : {(Dan, Butter) : [numbers] }, 'max(quant)' : {(Dan , Butter) : [numbers] } }
    # first scan populates gvs columns
    for row in cur:
        # only include unique rows based on group vars
        if (len(mf_struct.v) != 0):
            row_combo = tuple(row[col_name.index(gv)] for gv in mf_struct.v) # find index of group var based on col_name index to get value in data table
        else: # if no group by then use entire row as tuple
            row_combo = row
        
        # if not in unique then add it (group by function)
        if row_combo not in unique:
            unique.append(row_combo)
        # if there are aggregates, keep track of their values
        if len(F_VECT) != 0:
            for f in F_VECT:
                # if tuple of group by vals not already in agg_values, add new row
                if row_combo not in agg_values[f['agg']]:
                    agg_values[f['agg']][row_combo] = []

                # filter based on such that conditions
                if (len(such_that) != 0):
                    conditions = process_conditions(mf_struct, group_by_vars)
                    if eval_conditions(row, conditions, f) == False:
                        continue # if it does not meet such that conditions, skip that row for the aggregate

                # get rid of period and only have column name to find index
                if '.' in f['arg']:
                    name = f['arg'].split('.')[1]
                    agg_values[f['agg']][row_combo].append(row[col_name.index(name)])
                else: agg_values[f['agg']][row_combo].append(row[col_name.index(f['arg'])])   
    # populate H table, grouping by group vars
    H = pd.DataFrame(unique, columns=mf_struct.v)
    for col in mf_struct.F:
        H[col] = None

    # if there are aggregates calculate their functions
    if len(F_VECT) != 0:
        for f in F_VECT:
            for row_combo, values in agg_values[f['agg']].items():
                # Chekc if there are values in the list, otherwise drop it 
                if len(values) == 0:
                    row_index = H.loc[(H[mf_struct.v] == row_combo).all(axis=1)].index
                    H.drop(row_index, inplace=True)  # Drop the row with empty values
                    continue
                if f['func'] == 'avg':
                    if len(values) != 0:
                        result = sum(values) / len(values)
                    else: result = 0
                elif f['func'] == 'sum':
                    if len(values) != 0:
                        result = sum(values)
                    else: result = 0
                elif f['func'] == 'max':
                    if len(values) != 0:
                        result = max(values)
                    else: result = 0
                elif f['func'] == 'min':
                    if len(values) != 0:
                        result = min(values)
                    else: result = 0
                elif f['func'] == 'count':
                    if len(values) != 0:
                        result = len(values)
                    else: result = 0
                else: continue

                # select row by index position if it is equal to the group by values
                row_index = H.loc[(H[mf_struct.v] == row_combo).all(axis=1)].index

                # assign aggregate value to specific row
                H.loc[row_index, f['agg']] = result
    return H


def main():
    """
    This is the generator code. It should take in the MF structure and generate the code
    needed to run the query. That generated code should be saved to a 
    file (e.g. _generated.py) and then run.
    """
    # PUT ALGORITHM HERE:
    body = """
    H = H_table(where, such_that, having, group_by_vars, F_VECT, mf_struct)
    """

    # Note: The f allows formatting with variables.
    #       Also, note the indentation is preserved.
    tmp = f"""
import os
import psycopg2
import psycopg2.extras
import tabulate
from dotenv import load_dotenv
import pandas as pd
from generator import *
# DO NOT EDIT THIS FILE, IT IS GENERATED BY generator.py

def query():
    class mf_struct:
        def __init__(self):
            self.s = [] # projected values
            self.n = 0 # number of grouping variables
            self.v = [] # group by attributes
            self.F = [] # list of aggregates
            self.sigma = [] # grouping variables predicates 
            self.G = None # having clause
    mf_struct = mf_struct()    
    schemaData = schema_info()
    select, From, where, group_by, such_that, having = read_file("MFQuery3.txt")
    group_by_vars, V, F_VECT = process_info(select, group_by, such_that, having, mf_struct, schemaData)

    {body}
    
    return tabulate.tabulate(H,
                        headers="keys", tablefmt="psql", showindex=False)

def main():
    print(query())
    
if "__main__" == __name__:
    main()
    """

    # Write the generated code to a file
    open("_generated.py", "w").write(tmp)
    # Execute the generated code
    subprocess.run(["python", "_generated.py"])

    #H_table(where, such_that, having, group_by_vars, F_VECT, mf_struct)

    
if "__main__" == __name__:
    main()
