# Practical-Optimization
In this assignment you will accomplish rudimentary data processing on a set of Capital Bikeshare data sets. This data set consists of 24 trip files 
and 1 terminal file.  You will need all 25 files to complete the assignment. I suggest you download the files into a local directory on your computer
before you get started. At the high level, you will process multiple data files, insert the data into a SQLIte Database (which you will programmatically
create), create several queries that allow you to present the data as you need it, then integrate the SQL results with Python routines to get the data 
you want. This is very basic stuff.  

`HINT:  Look at how to use executemany command in SQLite and Python as well as using lists and tuples to load large data sets (as opposed to dictionaries) 
into a database.`

## Problem 1
Get the basic station information into the database.  Write a Python routine that loads the station file “Capital_Bikeshare_Terminal_Locations.csv” 
into a table in the database.  Your solution should check to see if the data table exists and creates the table if it does not exist as well as 
delete any data that may exist in the table before you begin to upload it.  Your solution should also output to the screen the total number of 
records entered into the data table.  This problem gives you the skills required to programmatically create and connect to a database, programmatically 
delete and create tables, import data from a CSV into the data tables, and use Python abstract data structures to assist you.  

`HINT:  use the executemany command in SQLite to load the data into the database.  Recommend you use the lists/tuples to temporarily store the data in 
memory before loading it into the database (as opposed to using a dictionary – which seems to be a bit more difficult to implement correctly for some people).  
Use the following date-time format when loading the data into the SQLite database:  YYYY-MM-DD HH:SS.  The dashes are important because that is the only 
format that SQLite knows and recognizes inherently as a date.`

```
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jan 24 12:05:02 2020

@author: timyang
"""


# import 
import sqlite3
import pandas as pd

#set database by sqlite
conn = sqlite3.connect('Capital_Bike_Share.db')
cursor = conn.cursor()

#check to see if the data table exists and creates the table if it does not exist
create_tb='''
        CREATE TABLE IF NOT EXISTS Capital_Bike_Share
        ("TERMINAL_NUMBER" INTEGER,
         "ADDRESS" TEXT,
         "LATITUDE" REAL,
         "LONGITUDE" REAL,
         "DOCKS" INTEGER);
        '''
cursor.execute(create_tb)
conn.commit()

#delete any data that may exist in the table
delete_dt='''
            DELETE FROM Capital_Bike_Share;
            '''
cursor.execute(delete_dt)
conn.commit()

# load the data into a Pandas DataFrame
Capital_Bike_Share = pd.read_csv('Homework01_data_files/Capital_Bike_Share_Locations.csv')
# write the data to a sqlite table
Capital_Bike_Share.to_sql('Capital_Bike_Share', conn, if_exists='append', index = False)
conn.commit()

#show the output which includes the total number of records entered into the data table.
print(pd.read_sql('''SELECT * FROM Capital_Bike_Share ''', conn))

#close database
conn.close()
```

## Problem 2 
Get the trip data into the database.  Using the same database as you created in problem 1, write a Python routine that iterates over all the data files 
that contain trip history (they have the filename format “Year_<yyyy>_QTR_<q>_bileTrips.csv”) and load them into a single data table in the database 
you created for Problem 1.  Your solution should check for the existence of the data table that holds the bike trip data (and create it if it does 
not exist and purge any data in it before you begin to upload the a fresh data set), and load the contents of the files into the data table.  
Your solution should also output to the terminal the number of records transferred from each data set as well as return the total number of records 
that have been loaded into the data table.   This problem gives you the skills required to iterate over multiple data sets, insert data into an existing 
table, and creating tables if one does not exist.  
  
`HINT:  use the same hints for Problem 1.  Iterate over the 24 files - do not type all 24 file names into your routine. Look at how to use the Python 
“glob” module to do this more easily (or you can use the “os” module if you prefer.`

```
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jan 24 12:05:02 2020

@author: timyang
"""

# import
import sqlite3
import glob
import os
import pandas as pd

# set database by sqlite
conn = sqlite3.connect('Capital_Bike_Share.db')
cursor = conn.cursor()

# check to see if the data table exists and creates the table if it does not exist
create_tb_year='''
CREATE TABLE IF NOT EXISTS "Year_bikeTrips" (
	"TRIP_DURATION"	INTEGER,
	"START_DATE"	TEXT,
	"START_STATION"	INTEGER,
	"STOP_DATE"	TEXT,
	"STOP_STATION"	INTEGER,
	"BIKE_ID"	TEXT,
	"USER_TYPE"	TEXT
);
       '''
cursor.execute(create_tb_year)
conn.commit()

# delete any data that may exist in the table
delete_dt='''
            DELETE FROM Year_bikeTrips;
            '''
cursor.execute(delete_dt)
conn.commit()

# check the path of files
inputfile = str(os.path.dirname(os.getcwd()))+"/Practical-Optimization/Year_bikeTrips/*.csv"
outputfile =  str(os.path.dirname(os.getcwd()))+"/Practical-Optimization/Year_bikeTrips/Total_bikeTrips.csv"
csv_list = glob.glob(inputfile)

# take a file with header as the first file
filepath =  csv_list [0]
df = pd.read_csv(filepath)
df = df.to_csv(outputfile,index=False)

# add the other files without header
for i in range(1,len(csv_list)):
    filepath = csv_list [i]
    df = pd.read_csv(filepath)
    df = df.to_csv(outputfile,index=False, header=False,mode='a+')

# load the data into a Pandas DataFrame
Year_bike = pd.read_csv("Year_bikeTrips/Total_bikeTrips.csv")
# write the data to the sqlite table
Year_bike.to_sql('Year_bikeTrips', conn, if_exists='append', index = False)
conn.commit()

# show the output which includes the total number of records entered into the data table.
print(pd.read_sql('''SELECT * FROM Year_bikeTrips ''', conn))

# close database
conn.close()


```

## Problem 3 
Write a Python routine (or demonstrate that you can successfully call an existing publically available module) that takes as its arguments the LAT/LON 
for any two points and whether distance should be calculated as miles or kilometers.  The output of the routine should be the distance between the two 
points (in the specified unit of measure).  This problem exercises your ability to make routines that take in required arguments and passes out a formatted response.  

`Hint:  The function I am asking you to implement is the Haversine function.  As an alternative, you can also use the Vincenty function.  You can find 
the actual math formulas on line, or you can load and call the two libraries that already have been written.  You just need to find them so you can 
install them.  Google is your friend.`

```
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jan 24 12:05:02 2020

@author: timyang
"""
```

## Problem 4   
Create a Python routine that returns a dictionary that has as its keys all pairs of bike terminals and for each key’s value - the distance between those 
two locations.  Use the Python routine you developed in problem 3 to generate the distances between the bike terminals.  Your solution should include calling 
a query in your SQLite database that provides you the data you need to calculate the distance between all station pairs.  This problem exercises your 
ability to manipulate data sets within a database as well as use dictionaries to save data.  

`Hint:  This means you have to make a join between two tables.  You could do a Cartesian join or you could iterate over all bike terminals and then find the distance from each terminal to all other terminals`

```
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jan 24 12:05:02 2020

@author: timyang
"""
```

## Problem 5 
Create a Python routine that takes as its argument a dictionary of pairwise station distances, a Bikeshare terminal, and a specified distance and returns a 
list of all stations that are within the specified distance of the docking station in question.  You should use the return value of problem 4 as the 
dictionary that gets passed into this routine.  This problem tests your ability to write a routine that takes in arguments, passes out results, and tests 
your ability to filter off of keys in a dictionary.  

`Hint:  You don’t need to query the database for this problem if you did Problem 4 correctly.`

```
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jan 24 12:05:02 2020

@author: timyang
"""
```

## Problem 6 
Create a Python routine that takes as its argument any two BikeShare stations and a start and end date and returns the total number of trips made by riders
between those two stations over the period of time specified by the start and stop date.  You should use the database and data tables created in problems 1 
and 2 to solve the problem.  This problem tests your ability to write a select statement on a table in a database and return the results from a select query. 

`Hint:  You have to call the database for this routine to work correctly.`


```
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jan 24 12:05:02 2020

@author: timyang
"""
```

## Problem 7 
Create a main routine.  Write a main routine that calls all 6 problems you have prepared for this assignment, has appropriate parameter values that are passed into your routine and will run all 6 routines if I run your homework module.

```
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jan 24 12:05:02 2020

@author: timyang
"""
```

## Format for the data files follows:
```
Capital_Bikeshare_Terminal_Locations.csv (1 file in total)
•	TERMINAL_NUMBER:  The unique numeric identifier for the terminal
•	ADDRESS:  The street address for the terminal
•	LATITUDE:  The latitude for the terminal
•	LONGITUDE:  The longitude of the terminal
•	DOCKS:  The number of docking stations at the terminal

Year_201X_QTR_Y_bikeTrips.csv (24 files in total)
•	TRIP_DURATION:  The length of time the bike was leased (measured in milliseconds)
•	START_DATE:  The date and time the bike lease started
•	START_STATION:  The station ID where the bike trip started
•	STOP_DATE:  The date and time the bike lease ended
•	STOP_STATION:  The station ID where the bike lease ended
•	BIKE_ID:  The unique identifier for the bike that was leased
•	USER_TYPE:  Indicates whether the user was a registered user (member) or a casual user (not a member)
```
