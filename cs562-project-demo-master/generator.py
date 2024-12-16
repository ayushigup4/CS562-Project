import subprocess
import os
import psycopg2
import psycopg2.extras
import tabulate
from dotenv import load_dotenv
import pandas as pd
import time


def main():
    """
    This is the generator code. It should take in the MF structure and generate the code
    needed to run the query. That generated code should be saved to a 
    file (e.g. _generated.py) and then run.
    """
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
                select = lines[0][7:].strip()
            if len(lines) > 1:
                From = lines[1][5:] 
            if len(lines) > 2:
                if "where" in lines[2].lower():
                    where = lines[2][6:]
                elif "group" in lines[2].lower():
                    group_by = lines[2][9:]
            if len(lines) > 3:
                if "group" in lines[3].lower():
                    group_by = lines[3][9:]
                elif "such that" in lines[3].lower():
                    such_that = lines[3][10:]
                elif "having" in lines[3].lower():
                    having = lines[3][7:]
            if len(lines) > 4:
                if "such that" in lines[4].lower():
                    such_that = lines[4][10:]
                elif "having" in lines[4].lower():
                    having = lines[4][7:]
        return select, From, where, group_by, such_that, having

    def process_info(select, From, where, group_by, such_that, having, schemaData):
        """
        This function processes an MF query and extracts its information,
        populating mf_struct with 6 operands of Phi
        """
        V = [] # list of grouping attributes
        F_VECT = [] # list of aggregate functions
        
        # potential aggregates
        aggregates = ["sum", "count", "avg", "min", "max", "variance", "median", "mode", "first_value", "last_value"]
        
        # potential group by attributes
        groupAttrs = ["cust", "prod", "day", "month", "year", "state", "quant", "date"]
    

        mf_struct.s = select # add projected vals to struct.s

        # split projected values        
        select = select.split(",")
        #print(select)
        
        group_by = group_by.split(",")
        #print(group_by)
        
        such_that = such_that.split(",")
        for predicate in such_that:
            mf_struct.sigma.append(predicate) # append gv predicate to struct.sigma
        
        if (having):
            mf_struct.G = having # replace struct.G with having clause if it exists

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
            for group in group_by:
                group = group.strip()
                if group in col_name.lower() and group in groupAttrs:
                    mf_struct.v.append(group) # append gb attribute to struct.v            
                    V.append({
                        "attrib": group,
                        "type": data_type,
                        "size": max_len
                    })
                
        mf_struct.n = len(F_VECT) # add number of gvs
        
        print("struct {")
        # Print out the grouping attributes (V)
        for i, v in enumerate(V):
            print(f"    {v['type']} {v['attrib']}[{v['size']}];")
        # Print out the aggregates (F_VECT)
        for f in F_VECT:
            print(f"    {f['type']} {f['agg']};")
        print("} mf_struct[500];")

        return V, F_VECT


    schemaData = schema_info()

    query = """SELECT prod, sum(X.quant), sum(Y.quant), sum(Z.quant)
FROM sales
WHERE year=2017
GROUP BY prod : X, Y, Z
SUCH THAT X.month = 1, Y.month = 2, Z.month = 3"""

    select, From, where, group_by, such_that, having = read_file("simpleQuery.txt")
    
    V, F_VECT = process_info(select, From, where, group_by, such_that, having, schemaData)
    
    print('\n Phi Operators of the Query')
    print("s = ", mf_struct.s)
    print("n = ", mf_struct.n)
    print("v = ", mf_struct.v)
    print("F = ", mf_struct.F)
    print("sigma = ", mf_struct.sigma)
    print("G = ", mf_struct.G)


    def H_table():
        load_dotenv()
        user = os.getenv('USER')
        password = os.getenv('PASSWORD')
        dbname = os.getenv('DBNAME')

        conn = psycopg2.connect("dbname="+dbname+" user="+user+" password="+password,
                                cursor_factory=psycopg2.extras.DictCursor)
        cur = conn.cursor()

        query = f"SELECT * FROM sales"
        if len(where) != 0:
            query += f" WHERE {where}"
            
        cur.execute(query)

        # initalize H table with headers
        H = pd.DataFrame(None, columns=mf_struct.v)
        for col in mf_struct.F:
            H[col] = None

        col_name = ["cust", "prod", "day", "month", "year", "state", "quant", "date"]
        unique = [] # store unique combos based on group by vars
        agg_values = {f['agg']: {} for f in F_VECT} 
        # agg_values = {f['agg'] : {} } will hold dictionary of group by values with their corresponding data based on other aggregates
        # example: {'avg(quant)' : {[Dan, Butter] : [numbers] }, 'max(quant)' : {[Dan , Butter] : [numbers] } }
        
        # first scan populates gvs columns
        for row in cur:
            # only include unique rows based on group vars
            row_combo = tuple(row[col_name.index(gv)] for gv in mf_struct.v) # find index of group var based on col_name index to get value in data table
            # if not in unique then add it (group by function)
            if row_combo not in unique:
                unique.append(row_combo)
            # if there are aggregates, keep track of their values
            if len(F_VECT) != 0:
                for f in F_VECT:
                    if row_combo not in agg_values[f['agg']]:
                        agg_values[f['agg']][row_combo] = []
                    agg_values[f['agg']][row_combo].append(row[col_name.index(f['arg'])])
                
        # populate H table, grouping by group vars 
        H = pd.DataFrame(unique, columns=mf_struct.v)
        for col in mf_struct.F:
            H[col] = None
        
        # filter based on such that conditions

        # if there are aggregates calculate their functions
        if len(F_VECT) != 0:
            for f in F_VECT:
                for row_combo, values in agg_values[f['agg']].items():
                    if f['func'] == 'avg':
                        result = sum(values) / len(values)
                    elif f['func'] == 'sum':
                        result = sum(values)
                    elif f['func'] == 'max':
                        result = max(values)
                    elif f['func'] == 'min':
                        result = min(values)
                    elif f['func'] == 'count':
                        result = len(values)
                    else: continue

                    # Match the first group variable
                    row_index = H.index[H[mf_struct.v[0]] == row_combo[0]]
                    if len(mf_struct.v) > 1:
                        # iterate through all group vars and find corresponding row index
                        for index, gv in enumerate(mf_struct.v):  # Match the rest of the group variables
                            # select row by index position if it is equal to the group by values
                            row_index = row_index[H.iloc[row_index][gv] == row_combo[index]]
                    # assign aggregate value to specific row
                    H.loc[row_index, f['agg']] = result
        H = tabulate.tabulate(H,
            headers="keys", tablefmt="psql")
        return H
    

             


    # PUT ALGORITHM HERE:
    body = """
    for row in cur:
        if row['year'] == 2016:
            _global.append(row)
    """

    # Note: The f allows formatting with variables.
    #       Also, note the indentation is preserved.
    tmp = f"""
import os
import psycopg2
import psycopg2.extras
import tabulate
from dotenv import load_dotenv

# DO NOT EDIT THIS FILE, IT IS GENERATED BY generator.py

def query():
    load_dotenv()

    user = os.getenv('USER')
    password = os.getenv('PASSWORD')
    dbname = os.getenv('DBNAME')

    conn = psycopg2.connect("dbname="+dbname+" user="+user+" password="+password,
                            cursor_factory=psycopg2.extras.DictCursor)
    cur = conn.cursor()
    cur.execute("SELECT * FROM sales")
    
    _global = []
    {body}
    
    return tabulate.tabulate(_global,
                        headers="keys", tablefmt="psql")

def main():
    print(query())
    
if "__main__" == __name__:
    main()
    """

    # Write the generated code to a file
    #open("_generated.py", "w").write(tmp)
    # Execute the generated code
    #subprocess.run(["python", "_generated.py"])

    print(H_table())

    
if "__main__" == __name__:
    main()
