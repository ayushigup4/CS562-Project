import subprocess
import os
import psycopg2
import psycopg2.extras
import tabulate
from dotenv import load_dotenv

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
            self.v = [] # grouping attributes
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

    def process_info(query, schemaData):
        """
        This function processes an MF query and extracts its information,
        populating mf_struct with 6 operands of Phi
        """
        V = [] # list of grouping attributes
        F_VECT = [] # list of aggregate functions
        
        # potential aggregates
        aggregates = ["sum", "count", "avg", "min", "max", "variance", "median", "mode", "first_value", "last_value"]
        
        # potential group vars
        groupvars = ["cust", "prod", "day", "month", "year", "state", "quant", "date"]
              
        # split query select, from, where, etc. by new line char
        querySplit = query.split('\n')
        for line in querySplit:
            line = line.lower() # make everything lowercase

        # initalize vars
        select = ""
        From = ""
        where = ""
        group_by = ""
        such_that = ""
        having = ""

        # extract each part of the query
        if len(querySplit) > 0:
            select = querySplit[0][7:]
        if len(querySplit) > 1:
            From = querySplit[1][5:] 
        if len(querySplit) > 2:
            if "where" in querySplit[2].lower():
                where = querySplit[2][6:]
            elif "group" in querySplit[2].lower():
                group_by = querySplit[2][9:]
        if len(querySplit) > 3:
            if "group" in querySplit[3].lower():
                group_by = querySplit[3][9:]
            elif "such that" in querySplit[3].lower():
                such_that = querySplit[3][10:]
            elif "having" in querySplit[3].lower():
                having = querySplit[3][7:]
        if len(querySplit) > 4:
            if "such that" in querySplit[4].lower():
                such_that = querySplit[4][10:]
            elif "having" in querySplit[4].lower():
                having = querySplit[4][7:]

        #print("SELECT:", select)
        #print("FROM:", From)
        #print("WHERE:", where)
        #print("GROUP BY:", group_by)
        #print("SUCH THAT:", such_that)
        #rint("HAVING:", having)

        mf_struct.s = select # add projected vals to struct.s
        
        such_that = such_that.split(",")
        for predicate in such_that:
            mf_struct.sigma.append(predicate) # append gv predicate to struct.sigma
        
        if (having):
            mf_struct.G = having # replace struct.G with having clause if it exists

        # split projected values        
        select = select.split(",")
        #print(select)

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
                            "type": data_type
                        })
                        continue
                elif sel in col_name.lower() and sel in groupvars:
                    mf_struct.v.append(sel) # append gvs to struct.v            
                    V.append({
                        "attrib": sel,
                        "type": data_type,
                        "size": max_len
                    })
    
        mf_struct.n = len(V) # add number of gvs
        
        print("struct {")
        # Print out the grouping attributes (V)
        for i, v in enumerate(V):
            print(f"    {v['type']} {v['attrib']}[{v['size']}];")
        # Print out the aggregates (F_VECT)
        for f in F_VECT:
            print(f"    {f['type']} {f['agg']};")
        print("} mf_struct[500];")

        return V, F_VECT
    
    # PUT ALGORITHM HERE:

    schemaData = schema_info()

    query = """SELECT prod, sum(X.quant), sum(Y.quant), sum(Z.quant)
FROM sales
WHERE year=2017
GROUP BY prod : X, Y, Z
SUCH THAT X.month = 1, Y.month = 2, Z.month = 3"""
    
    V, F_VECT = process_info(query, schemaData)

    body = """
    for row in cur:
        if row['quant'] > 10:
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
    open("_generated.py", "w").write(tmp)
    # Execute the generated code
    subprocess.run(["python", "_generated.py"])

    #print(schemaData)


if "__main__" == __name__:
    main()
