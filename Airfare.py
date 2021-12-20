from os import read
from IPython.display import display, HTML
import pandas as pd
import sqlite3
from sqlite3 import Error
from csv import reader
import matplotlib.pyplot as plt
import numpy as np

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

# major_airlines=df.where(=='DL'or df['carrier_lg']=='AA'or df['carrier_lg']=='UA'or df['carrier_lg']=='WN']
aa_average=df.where(df['Carrier_LG']=='AA').mean()['Average_Fare']
ua_average=df.where(df['Carrier_LG']=='UA').mean()['Average_Fare']
sw_average=df.where(df['Carrier_LG']=='WN').mean()['Average_Fare']
dl_average=df.where(df['Carrier_LG']=='DL').mean()['Average_Fare']
jb_average=df.where(df['Carrier_LG']=='B6').mean()['Average_Fare']

Airlines =['American','United','South West','Delta',"Jet Blue"]
pos = np.arange(len(Airlines))
avg_fares = [aa_average,ua_average,sw_average,dl_average,jb_average]

bars=plt.bar(pos, avg_fares, align='center')
bars[0].set_color('darkred')
bars[2].set_color('yellow')
bars[3].set_color('red')
bars[4].set_color('steelblue')
plt.xticks(pos, Airlines)
plt.xlabel('Airlines')
plt.ylabel('Average Fares($)')
plt.title('5 Major Carriers and their average fares')


# plt.plot(df['Distance'],df['Average Fare'])

df['Origin'].value_counts()[:10].sort_values(ascending=False)

atl_average=df.where(df['Origin']=='Atlanta, GA (Metropolitan Area)').mean()['Average_Fare']
ord_average=df.where(df['Origin']=='Chicago, IL' ).mean()['Average_Fare']
dfw_average=df.where(df['Origin']=='Dallas/Fort Worth, TX').mean()['Average_Fare']
bos_average=df.where(df['Origin']=='Boston, MA (Metropolitan Area)').mean()['Average_Fare']
lax_average=df.where(df['Origin']=='Los Angeles, CA (Metropolitan Area)').mean()['Average_Fare']
lax_average

Origin =['Atlanta','Chicago ','Dallas','Boston']
pos = np.arange(len(Origin))
origin_fares = [atl_average,ord_average,dfw_average,bos_average]
plt.scatter(Origin[0],origin_fares[0],color='red')
plt.scatter(Origin[1],origin_fares[1],color='blue')
plt.scatter(Origin[2],origin_fares[2],color='yellow')
plt.scatter(Origin[3],origin_fares[3],color='purple')
plt.scatter(Origin[4],origin_fares[4],color='brown')
plt.xlabel('Origin Airports')
plt.ylabel('Average Fares($)')
plt.title('4 Major Airports and Their Average Fares')

q1_average=df.where(df['Quarter']==1).mean()['Average_Fare']
q2_average=df.where(df['Quarter']==2).mean()['Average_Fare']
q3_average=df.where(df['Quarter']==3).mean()['Average_Fare']
q4_average=df.where(df['Quarter']==4).mean()['Average_Fare']

Quarters =['Quarter1','Quarter2','Quarter3','Quarter4']
pos = np.arange(len(Quarters))
quarter_fares = [q1_average,q2_average,q3_average,q4_average]
plt.plot(Quarters,quarter_fares,'-o',color='red',linewidth=3)

plt.xlabel('Quarters')
plt.ylabel('Average Fares($)')
plt.title('Average Fares in every Quarter')

plt.figure()
plt.hist2d(df['Distance'],df['Average_Fare'],bins=1000)
plt.colorbar()
plt.xlabel('Distance in miles')
plt.ylabel('Average Fares($)')
plt.title('Flying Distance vs Average Fares')

# Flight fare prediction
df.corr()

# Encoding categorical values
from sklearn import preprocessing
le = preprocessing.LabelEncoder()

# df=df.drop(['Average Fare'],axis=1)
data_categorical=df.select_dtypes(exclude=["int64","float","int32"])
data_numerical=df.select_dtypes(include=["int64","float","int32"])
data_categorical=data_categorical[['Origin','Destination']]
data_categorical=data_categorical.apply(le.fit_transform)

data_categorical
data_numerical=data_numerical[['Year','Quarter','Distance']]
x=pd.concat([data_categorical,data_numerical],axis=1)
y=df['Average_Fare']

from sklearn.model_selection import train_test_split

X_train,X_test,y_train,y_test=train_test_split(x,y,random_state=0)

from sklearn.ensemble import RandomForestRegressor

random_forest=RandomForestRegressor(n_estimators=100,max_depth=40).fit(X_train,y_train)
print('Training accuracy score: {}'.format(random_forest.score(X_train,y_train)))
print('Testing Accuracy score: {}'.format(random_forest.score(X_test,y_test)))

feature_importance = random_forest.feature_importances_
sorted_idx = np.argsort(feature_importance)
pos = np.arange(sorted_idx.shape[0]) + .5
print(pos)
plt.figure()
plt.barh(pos, feature_importance[sorted_idx], align='center')
plt.yticks(pos, np.array(X_train.columns)[sorted_idx])
plt.title('Feature Importance(Random Forest Regressor)')