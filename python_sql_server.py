import pandas as pd
import pyodbc 

conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=localhost\SQLExpress;'
                      'Database=StockData;'
                      'Trusted_Connection=yes;')

df = pd.read_sql_query('SELECT * FROM VN_Intraday', conn)

print(df)
print(type(df))