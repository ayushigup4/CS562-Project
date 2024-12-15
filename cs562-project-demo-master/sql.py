import os
import psycopg2 
import psycopg2.extras
import tabulate
from dotenv import load_dotenv

#  USED FOR TESTING 
def query():
    """
    Used for testing standard queries in SQL.
    """
    load_dotenv()

    user = os.getenv('USER')
    password = os.getenv('PASSWORD')
    dbname = os.getenv('DBNAME')

    # connect to database
    conn = psycopg2.connect("dbname="+dbname+" user="+user+" password="+password,
                            cursor_factory=psycopg2.extras.DictCursor)
    cur = conn.cursor()
    
    # input SQL query trying to run
    cur.execute("SELECT x.prod, sum(x.quant), sum(y.quant), sum(z.quant) FROM sales x, sales y, sales z WHERE x.prod=y.prod and x.month=1 and x.year='2017' and y.prod=z.prod and y.month=2 and y.year='2017' and z.month=3 and z.year='2017' GROUP BY x.prod")
    
    return tabulate.tabulate(cur.fetchall(),
                             headers="keys", tablefmt="psql")


def main():
    print(query())


if "__main__" == __name__:
    main()
