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

#SQL statement for creating Quarter 1 table
sql_statement_q1 = """
CREATE TABLE [Quarter1](
        [Q1ID] INTEGER PRIMARY KEY AUTOINCREMENT,
        [Year] INTEGER NOT NULL,
        [Origin] TEXT NOT NULL,
        [Destination] TEXT NOT NULL,
        [Distance] REAL NOT NULL,
        [Average_Fare] REAL NOT NULL       
    );
"""
create_table(conn, sql_statement_q1)

#SQL statement for creating Quarter 2 table
sql_statement_q2 = """
CREATE TABLE [Quarter2](
        [Q2ID] INTEGER PRIMARY KEY AUTOINCREMENT,
        [Year] INTEGER NOT NULL,
        [Origin] TEXT NOT NULL,
        [Destination] TEXT NOT NULL,
        [Distance] REAL NOT NULL,
        [Average_Fare] REAL NOT NULL 
    );
"""
create_table(conn, sql_statement_q2)

#SQL statement for creating Quarter 3 table
sql_statement_q3 = """
CREATE TABLE [Quarter3](
        [Q3ID] INTEGER PRIMARY KEY AUTOINCREMENT,
        [Year] INTEGER NOT NULL,
        [Origin] TEXT NOT NULL,
        [Destination] TEXT NOT NULL,
        [Distance] REAL NOT NULL,
        [Average_Fare] REAL NOT NULL 
    );
"""
create_table(conn, sql_statement_q3)

#SQL statement for creating Quarter 4 table
sql_statement_q4 = """
CREATE TABLE [Quarter4](
        [Q4ID] INTEGER PRIMARY KEY AUTOINCREMENT,
        [Year] INTEGER NOT NULL,
        [Origin] TEXT NOT NULL,
        [Destination] TEXT NOT NULL,
        [Distance] REAL NOT NULL,
        [Average_Fare] REAL NOT NULL 
    );
"""
create_table(conn, sql_statement_q4)

#Initialize 4 lists for 4 quarters to store data
quarter1_list = []
quarter2_list = []
quarter3_list = []
quarter4_list = []


#Read the csv file to and store the data in the 4 lists created above
with open('Airfare_Prediction.csv') as file:
    next(file)
    for i in reader(file):
        if(i[1] == '1'):
            i.pop(1)
            quarter1_list.append(i)
        elif(i[1] == '2'):
            i.pop(1)
            quarter2_list.append(i)
        elif(i[1] == '3'):
            i.pop(1)
            quarter3_list.append(i)
        elif(i[1] == '4'):
            i.pop(1)
            quarter4_list.append(i)
            
for i in quarter1_list:
    year = int(i[0])
    distance = float(i[3])
    avg_fare = float(i[4])
    sql_statement_insert_q1 = "INSERT INTO [Quarter1] (Year, Origin, Destination, Distance, Average_Fare) VALUES({},'{}','{}',{},{})".format(year, i[1], i[2], distance, avg_fare)
    execute_sql_statement(sql_statement_insert_q1, conn)
conn.commit()

for i in quarter2_list:
    year = int(i[0])
    distance = float(i[3])
    avg_fare = float(i[4])
    sql_statement_insert_q2 = "INSERT INTO [Quarter2] (Year, Origin, Destination, Distance, Average_Fare) VALUES({},'{}','{}',{},{})".format(year, i[1], i[2], distance, avg_fare)
    execute_sql_statement(sql_statement_insert_q2, conn)
conn.commit()

for i in quarter3_list:
    year = int(i[0])
    distance = float(i[3])
    avg_fare = float(i[4])
    sql_statement_insert_q3 = "INSERT INTO [Quarter3] (Year, Origin, Destination, Distance, Average_Fare) VALUES({},'{}','{}',{},{})".format(year, i[1], i[2], distance, avg_fare)
    execute_sql_statement(sql_statement_insert_q3, conn)
conn.commit()

for i in quarter4_list:
    year = int(i[0])
    distance = float(i[3])
    avg_fare = float(i[4])
    sql_statement_insert_q4 = "INSERT INTO [Quarter4] (Year, Origin, Destination, Distance, Average_Fare) VALUES({},'{}','{}',{},{})".format(year, i[1], i[2], distance, avg_fare)
    execute_sql_statement(sql_statement_insert_q4, conn)
conn.commit()

sql_statement_q1_carrier = """
CREATE TABLE [Quarter1_Carrier](
        [Carrier_Q1ID] INTEGER PRIMARY KEY AUTOINCREMENT,
        [Q1ID] INTEGER NOT NULL,
        [Carrier_LG] TEXT NOT NULL,
        [Carrier_Low] TEXT NOT NULL,
        FOREIGN KEY(Q1ID) REFERENCES Quarter1(Q1ID)
    );
"""
create_table(conn, sql_statement_q1_carrier)

sql_statement_q2_carrier = """
CREATE TABLE [Quarter2_Carrier](
        [Carrier_Q2ID] INTEGER PRIMARY KEY AUTOINCREMENT,
        [Q2ID] INTEGER NOT NULL,
        [Carrier_LG] TEXT NOT NULL,
        [Carrier_Low] TEXT NOT NULL,
        FOREIGN KEY(Q2ID) REFERENCES Quarter2(Q2ID)
    );
"""
create_table(conn, sql_statement_q2_carrier)

sql_statement_q3_carrier = """
CREATE TABLE [Quarter3_Carrier](
        [Carrier_Q3ID] INTEGER PRIMARY KEY AUTOINCREMENT,
        [Q3ID] INTEGER NOT NULL,
        [Carrier_LG] TEXT NOT NULL,
        [Carrier_Low] TEXT NOT NULL,
        FOREIGN KEY(Q3ID) REFERENCES Quarter3(Q3ID)
    );
"""
create_table(conn, sql_statement_q3_carrier)

sql_statement_q4_carrier = """
CREATE TABLE [Quarter4_Carrier](
        [Carrier_Q4ID] INTEGER PRIMARY KEY AUTOINCREMENT,
        [Q4ID] INTEGER NOT NULL,
        [Carrier_LG] TEXT NOT NULL,
        [Carrier_Low] TEXT NOT NULL,
        FOREIGN KEY(Q4ID) REFERENCES Quarter4(Q4ID)
    );
"""
create_table(conn, sql_statement_q4_carrier)

sql_statement_q1_results = "SELECT Q1ID, Average_Fare FROM Quarter1"
q1_results = execute_sql_statement(sql_statement_q1_results, conn)
for i in range(len(quarter1_list)):
    if(float(quarter1_list[i][4]) == q1_results[i][1]):
        sql_statement_append_q1_carrier = "INSERT INTO [Quarter1_Carrier] (Q1ID, Carrier_LG, Carrier_Low) VALUES ({},'{}','{}')".format(q1_results[i][0],quarter1_list[i][5],quarter1_list[i][6])
        execute_sql_statement(sql_statement_append_q1_carrier, conn)
    conn.commit()

sql_statement_q2_results = "SELECT Q2ID, Average_Fare FROM Quarter2"
q2_results = execute_sql_statement(sql_statement_q2_results, conn)
for i in range(len(quarter2_list)):
    if(float(quarter2_list[i][4]) == q2_results[i][1]):
        sql_statement_append_q2_carrier = "INSERT INTO [Quarter2_Carrier] (Q2ID, Carrier_LG, Carrier_Low) VALUES ({},'{}','{}')".format(q2_results[i][0],quarter2_list[i][5],quarter2_list[i][6])
        execute_sql_statement(sql_statement_append_q2_carrier, conn)
    conn.commit()

sql_statement_q3_results = "SELECT Q3ID, Average_Fare FROM Quarter3"
q3_results = execute_sql_statement(sql_statement_q3_results, conn)
for i in range(len(quarter3_list)):
    if(float(quarter3_list[i][4]) == q3_results[i][1]):
        sql_statement_append_q3_carrier = "INSERT INTO [Quarter3_Carrier] (Q3ID, Carrier_LG, Carrier_Low) VALUES ({},'{}','{}')".format(q3_results[i][0],quarter3_list[i][5],quarter3_list[i][6])
        execute_sql_statement(sql_statement_append_q3_carrier, conn)
    conn.commit()

sql_statement_q4_results = "SELECT Q4ID, Average_Fare FROM Quarter4"
q4_results = execute_sql_statement(sql_statement_q4_results, conn)
for i in range(len(quarter4_list)):
    if(float(quarter4_list[i][4]) == q4_results[i][1]):
        sql_statement_append_q4_carrier = "INSERT INTO [Quarter4_Carrier] (Q4ID, Carrier_LG, Carrier_Low) VALUES ({},'{}','{}')".format(q4_results[i][0],quarter4_list[i][5],quarter4_list[i][6])
        execute_sql_statement(sql_statement_append_q4_carrier, conn)
    conn.commit()



