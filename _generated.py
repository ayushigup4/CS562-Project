
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
    select, From, where, group_by, such_that, having = read_file("simpleQuery.txt")
    group_by_vars, V, F_VECT = process_info(select, group_by, such_that, having, mf_struct, schemaData)
    conditions = process_conditions(mf_struct, group_by_vars)

    
    H = H_table(where, such_that, having, F_VECT, mf_struct)
    
    
    return tabulate.tabulate(H,
                        headers="keys", tablefmt="psql", showindex=False)

def main():
    print(query())
    
if "__main__" == __name__:
    main()
    