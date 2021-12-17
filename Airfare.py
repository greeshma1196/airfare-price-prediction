from os import read
from IPython.display import display, HTML
import pandas as pd
import sqlite3
from sqlite3 import Error
from csv import reader

#Create connection function
def creat_connection(db_file):
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        conn.execute("PRAGMA foreign_keys = 1")
    except Error as e:
        print(e)
    return conn

#Create table function
def create_table(conn, create_table_sql):
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)
        
#Execute SQL statement function        
def execute_sql_statement(sql_statement, conn):
    cur = conn.cursor()
    cur.execute(sql_statement)
    rows = cur.fetchall()
    return rows

#Create the normalized database with the file Airfare_Predcition.csv
normalized_database_name = 'normalized_airfare_prediction.db'

conn = creat_connection(normalized_database_name)

#SQL statement for creating Years table
sql_statement_year = """
CREATE TABLE [Years](
    [ID] INTEGER PRIMARY KEY AUTOINCREMENT,
    [Year] INTEGER UNIQUE NOT NULL
);
"""
create_table(conn, sql_statement_year)

#SQL statement for creating Quarters table
sql_statement_quarter = """
CREATE TABLE [Quarters](
    [ID] INTEGER PRIMARY KEY AUTOINCREMENT,
    [Quarter] INTEGER UNIQUE NOT NULL
);
"""
create_table(conn, sql_statement_quarter)

#SQL statement for creating Carriers table
sql_statement_carrier = """
CREATE TABLE [Carriers](
    [ID] INTEGER PRIMARY KEY AUTOINCREMENT,
    [Code] TEXT UNIQUE NOT NULL
);
"""
create_table(conn, sql_statement_carrier)

#SQL statement for creating Airports table
sql_statement_airport = """
CREATE TABLE [Airports](
    [ID] INTEGER PRIMARY KEY AUTOINCREMENT,
    [Name] TEXT UNIQUE NOT NULL
);
"""
create_table(conn,sql_statement_airport)

#SQL statement for creating Airfares table
sql_statement_airfare = """
CREATE TABLE [Airfares](
    [ID] INTEGER PRIMARY KEY AUTOINCREMENT,
    [Origin] INTEGER NOT NULL,
    [Destination] INTEGER NOT NULL,
    [Average_Fare] REAL NOT NULL,
    [Distance] REAL NOT NULL,
    [Year_ID] INTEGER NOT NULL,
    [Quarter_ID] INTEGER NOT NULL,
    [CLg_ID] INTEGER NOT NULL,
    [CLow_ID] INTEGER NOT NULL,
    FOREIGN KEY(Origin) REFERENCES Airports(ID),
    FOREIGN KEY(Destination) REFERENCES Airports(ID),
    FOREIGN KEY(Year_ID) REFERENCES Years(ID),
    FOREIGN KEY(Quarter_ID) REFERENCES Quarters(ID),
    FOREIGN KEY(CLg_ID) REFERENCES Carriers(ID),
    FOREIGN KEY(CLow_ID) REFERENCES Carriers(ID)
);
"""
create_table(conn, sql_statement_airfare)

#Initialize sets for Years, Quarters, Carriers, Airports respectively
year_list = set()
quarter_list = set()
carrier_list = set()
airport_list = set()

#Read the file and extract values to store in the sets mentioned above
with open('Airfare_Prediction.csv') as file:
    next(file)
    for i in reader(file):
        if(i[0] not in year_list):
            year_list.add(i[0])
        if(i[1] not in quarter_list):
            quarter_list.add(i[1])
        if(i[2] not in airport_list):
            airport_list.add(i[2])
        if(i[3] not in airport_list):
            airport_list.add(i[3])
        if(i[6] not in carrier_list):
            carrier_list.add(i[6])
        if(i[7] not in carrier_list):
            carrier_list.add(i[7])

#Iterate through each of the set and add to the tables   
for i in year_list:
    sql_statement_insert_year = "INSERT INTO [Years] (Year) VALUES({})".format(i)
    execute_sql_statement(sql_statement_insert_year, conn)
conn.commit()
       
for i in quarter_list:
    sql_statement_insert_quarter = "INSERT INTO [Quarters] (Quarter) VALUES({})".format(i)
    execute_sql_statement(sql_statement_insert_quarter, conn)
conn.commit()
       
for i in carrier_list:
    sql_statement_insert_carrier = "INSERT INTO [Carriers] (Code) VALUES('{}')".format(i)
    execute_sql_statement(sql_statement_insert_carrier, conn)
conn.commit()
       
for i in airport_list:
    sql_statement_destinations = "INSERT INTO [Airports] (Name) VALUES('{}')".format(i)
    execute_sql_statement(sql_statement_destinations,conn)
conn.commit()

with open('Airfare_Prediction.csv') as file:
    next(file)
    for i in reader(file):
        year_id = execute_sql_statement("SELECT ID FROM Years WHERE Year={}".format(i[0]),conn)[0][0]
        quarter_id = execute_sql_statement("SELECT ID FROM Quarters WHERE Quarter={}".format(i[1]),conn)[0][0]
        origin = execute_sql_statement("SELECT ID FROM Airports WHERE Name='{}'".format(i[2]),conn)[0][0]
        destination = execute_sql_statement("SELECT ID FROM Airports WHERE Name='{}'".format(i[3]),conn)[0][0]
        carrier_lg = execute_sql_statement("SELECT ID FROM Carriers WHERE Code='{}'".format(i[6]),conn)[0][0]
        carrier_low = execute_sql_statement("SELECT ID FROM Carriers WHERE Code='{}'".format(i[7]),conn)[0][0]
        sql_statement_insert_airfares = """
            INSERT INTO [Airfares] 
            (Origin, Destination, Average_Fare, Distance, Year_ID, Quarter_ID, CLg_ID, CLow_ID) 
            VALUES ({},{},{},{},{},{},{},{})""".format(origin, destination,i[5],i[4],year_id,quarter_id,carrier_lg,carrier_low)
        execute_sql_statement(sql_statement_insert_airfares, conn)
    conn.commit()

#SQL statement to generate the entire table with normalized values
sql_statement_table = """
    SELECT Years.Year,
        Quarters.Quarter,
        Origin.Name AS Origin,
        Destination.Name AS Destination,
        Airfares.Average_Fare, 
        Airfares.Distance,
        Carrier_LG.Code AS Carrier_LG,
        Carrier_Low.Code AS Carrier_Low
    FROM Airfares
    INNER JOIN Years ON Years.ID = Airfares.Year_ID
    INNER JOIN Quarters ON Quarters.ID = Airfares.Quarter_ID
    INNER JOIN Airports AS Origin ON Origin.ID = Airfares.Origin
    INNER JOIN Airports AS Destination ON Destination.ID = Airfares.Destination
    INNER JOIN Carriers AS Carrier_LG ON Carrier_LG.ID = Airfares.CLg_ID
    INNER JOIN Carriers AS Carrier_Low ON Carrier_Low.ID = Airfares.CLow_ID
"""

df = pd.read_sql_query(sql_statement_table, conn)
print(df)
