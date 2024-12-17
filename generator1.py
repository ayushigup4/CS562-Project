import subprocess
import os
import psycopg2
import psycopg2.extras
import pandas as pd
from dotenv import load_dotenv

# Define the MF/EMF structure
class mf_struct:
    def __init__(self):
        self.s = []  # Projected values
        self.n = 0   # Number of grouping variables
        self.v = []  # Grouping attributes
        self.F = []  # List of aggregates
        self.sigma = []  # Such that conditions
        self.G = None  # HAVING clause


mf_struct = mf_struct()


def schema_info():
    """
    Retrieves schema information from the database using 'information_schema.columns'.
    """
    query = """
    SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'public' and TABLE_NAME = 'sales'
    """
    load_dotenv()
    conn = psycopg2.connect(
        dbname=os.getenv('DBNAME'),
        user=os.getenv('USER'),
        password=os.getenv('PASSWORD'),
        host=os.getenv('HOST'),
        port=os.getenv('PORT')
    )
    cur = conn.cursor()
    cur.execute(query)
    data = cur.fetchall()
    conn.close()
    return data


def read_file(filename):
    """
    Reads an Extended SQL query and extracts its operands.
    """
    with open(filename, 'r') as file:
        lines = [line.lower().strip() for line in file]

    select, From, where, group_by, such_that, having = "", "", "", "", "", ""

    if len(lines) > 0:
        select = lines[0][7:].strip()
    if len(lines) > 1:
        From = lines[1][5:].strip()
    for line in lines[2:]:
        if "where" in line:
            where = line[6:].strip()
        elif "group by" in line:
            group_by = line[9:].strip()
        elif "such that" in line:
            such_that = line[10:].strip()
        elif "having" in line:
            having = line[7:].strip()

    return select, From, where, group_by, such_that, having


def process_info(select, group_by, such_that, having, mf_struct, schemaData):
    """
    Processes an MF/EMF query and populates the mf_struct with query components.
    """
    aggregates = ["sum", "count", "avg", "min", "max"]
    groupAttrs = ["cust", "prod", "day", "month", "year", "state", "quant", "date"]

    mf_struct.s = select
    mf_struct.v = group_by.split(",")
    mf_struct.sigma = such_that.split(",") if such_that else []
    mf_struct.G = having if having else None

    # Process aggregates
    for sel in select.split(","):
        sel = sel.strip()
        if '(' in sel and ')' in sel:
            func, arg = sel.split('(')[0], sel.split('(')[1][:-1]
            if func.lower() in aggregates:
                mf_struct.F.append(sel)

def H_table(where, such_that, having, F_VECT, mf_struct, group_by_vars, conditions): 
    """
    Handles multiple scans for EMF queries and computes aggregates for each grouping variable.
    """
    load_dotenv()
    conn = psycopg2.connect(
        dbname=os.getenv('DBNAME'),
        user=os.getenv('USER'),
        password=os.getenv('PASSWORD'),
        host=os.getenv('HOST'),
        port=os.getenv('PORT')
    )
    cur = conn.cursor()

    QUERY = f"SELECT * FROM sales"
    if where:
        QUERY += f" WHERE {where}"
    
    col_name = ["cust", "prod", "day", "month", "year", "state", "quant", "date"]
    unique = []  # Unique rows based on grouping variables
    group_aggregates = {gv: {} for gv in group_by_vars}  # Aggregates for each grouping variable
    
    # Perform multiple scans: one for each grouping variable
    for gv in group_by_vars:
        cur.execute(QUERY)
        for row in cur.fetchall():
            row_combo = tuple(row[col_name.index(attr)] for attr in mf_struct.v)  # Group by values
            
            # Apply SUCH THAT conditions for the current grouping variable
            if not all(eval(f"{row[col_name.index(cond['col'])]} {cond['op']} {repr(cond['cond'])}") 
                       for cond in conditions[gv]):
                continue  # Skip rows that don't meet the conditions
            
            # Initialize group aggregates
            if row_combo not in group_aggregates[gv]:
                group_aggregates[gv][row_combo] = {f['agg']: [] for f in F_VECT}
            
            # Collect values for aggregate functions
            for f in F_VECT:
                arg_col = f['arg'].split('.')[-1]  # Get column name (e.g., "quant" from "X.quant")
                group_aggregates[gv][row_combo][f['agg']].append(row[col_name.index(arg_col)])
    
    # Create H-table and compute final aggregates
    H = pd.DataFrame(columns=mf_struct.v + mf_struct.F)
    for row_combo in set(k for agg in group_aggregates.values() for k in agg):
        row_data = list(row_combo)
        for f in mf_struct.F:
            func = f.split("(")[0]
            values = []
            for gv in group_by_vars:
                if row_combo in group_aggregates[gv]:
                    values.extend(group_aggregates[gv][row_combo][f])
            # Compute aggregate
            if func == "avg":
                row_data.append(sum(values) / len(values) if values else None)
            elif func == "sum":
                row_data.append(sum(values))
            elif func == "max":
                row_data.append(max(values))
            elif func == "min":
                row_data.append(min(values))
            elif func == "count":
                row_data.append(len(values))
        H.loc[len(H)] = row_data
    
    # Apply HAVING clause if it exists
    if mf_struct.G:
        H = H.query(mf_struct.G)

    return H



def main():
    schemaData = schema_info()
    select, From, where, group_by, such_that, having = read_file("simpleQuery1.txt")
    process_info(select, group_by, such_that, having, mf_struct, schemaData)

    H = H_table(where, such_that, having, mf_struct)
    print(H.to_markdown(index=False))


if __name__ == "__main__":
    main()
