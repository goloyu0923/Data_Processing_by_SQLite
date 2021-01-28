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

#import
import os
import sqlite3
import csv
import glob
import pandas as pd
from math import radians, cos, sin, asin, sqrt
from itertools import permutations

#############################Q1#############################
def Q1():
    print("Q1")
    # load the data into a Pandas DataFrame
    Capital_Bike_Share = pd.read_csv(str(os.path.dirname(os.getcwd()))+"/Homework1/Capital_Bike_Share_Locations/Capital_Bike_Share_Locations.csv")

    #set database by sqlite
    conn = sqlite3.connect('Capital_Bike_Share.db')
    c = conn.cursor()

    #check to see if the data table exists and creates the table if it does not exist
    create_tb='''
            CREATE TABLE IF NOT EXISTS BikeShare
            ("TERMINAL_NUMBER" INTEGER,
            "ADDRESS" TEXT,
            "LATITUDE" REAL,
            "LONGITUDE" REAL,
            "DOCKS" INTEGER);
            '''
    c.execute(create_tb)

    #delete any data that may exist in the table
    delete_dt='''
                DELETE FROM BikeShare;
                '''
    c.execute(delete_dt)

    # write the data to a sqlite table
    Capital_Bike_Share.to_sql('BikeShare', conn, if_exists='append', index = False)

    #show the output which includes the total number of records entered into the data table.
    for row in c.execute('SELECT COUNT(*) FROM BikeShare'):
        print("Q1: The total number of BikeShare records is", row[0])

    #close database
    conn.commit()
    conn.close()
    
if __name__ == "__main__":
	Q1()
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

#import
import os
import sqlite3
import csv
import glob
import pandas as pd
from math import radians, cos, sin, asin, sqrt
from itertools import permutations

#############################Q2#############################
def Q2():
    #create path
    print("Q2")
    path = r'Year_bikeTrips'
    all_files = glob.glob(os.path.join(path, '*.csv'))
    #create list
    Tripslist = []
    for eachFile in all_files:
        with open(eachFile, 'r') as f:
            reader = csv.reader(f)
            next(reader)  # skip the headers
            Tlist = list(reader)
            for row in Tlist:
                Tripslist.append((row[0], row[1], row[2], row[3], row[4],row[5],row[6]))
    #database
    conn = sqlite3.connect('Capital_Bike_Share.db')
    #check if table exists then drop
    if conn.execute(''' SELECT count(name) FROM sqlite_master  WHERE type='table' AND name='BikeTrips' ''').fetchone()[0]==1 :
        print('BikeTrips - Table exists.')
        conn.execute("DROP TABLE BikeTrips;")
        print('BikeTrips - Table Dropped')
    else:
        print('BikeTrips - Table does not exist.')
    #data table
    conn.execute('''CREATE table IF NOT EXISTS BikeTrips
                 (TRIP_DURATION INT,
                 START_DATE TEXT,
                 START_STATION INT,
                 STOP_DATE TEXT,
                 STOP_STATION INT,
                 BIKE_ID TEXT,
                 USER_TYPE TEXT);''')
    print('BikeTrips - Table created')
    c = conn.cursor()
    c.executemany('INSERT INTO BikeTrips VALUES (?,?,?,?,?,?,?)', Tripslist)
    #print data
    for row in c.execute('SELECT COUNT(*) FROM BikeTrips'):
        print('Q2: Total number of BikeTrips records is',row[0])
    #201x-Qx
    for y in range(2010,2017):
        if y == 2010:
            q = 4
            y1 = y + 1
            st = str(y) + "-" + format(10, '02d') + "-01"
            et = str(y1) + "-" + format(1, '02d') + "-01"
            x = c.execute("SELECT COUNT(*) FROM BikeTrips WHERE START_DATE BETWEEN ? AND ?",(st,et,)).fetchall()
            print('Q2: Total number of records from ',y,'_QTR_',q,' is ',x[0][0],sep = "")
        elif y < 2016:
            for q in range(1,4):
                sm = q*3 - 2
                em = q*3 + 1
                st = str(y) + "-" + format(sm, '02d') + "-01"
                et = str(y) + "-" + format(em, '02d') + "-01"
                x = c.execute("SELECT COUNT(*) FROM BikeTrips WHERE START_DATE BETWEEN ? AND ?",(st,et,)).fetchall()
                print('Q2: Total number of records from ',y,'_QTR_',q,' is ',x[0][0],sep = "")
            q = 4
            y1 = y + 1
            st = str(y) + "-" + format(10, '02d') + "-01"
            et = str(y1) + "-" + format(1, '02d') + "-01"
            x = c.execute("SELECT COUNT(*) FROM BikeTrips WHERE START_DATE BETWEEN ? AND ?",(st,et,)).fetchall()
            print('Q2: Total number of records from ',y,'_QTR_',q,' is ',x[0][0],sep = "")
        else:
            for q in range(1,4):
                sm = q*3 - 2
                em = q*3 + 1
                st = str(y) + "-" + format(sm, '02d') + "-01"
                et = str(y) + "-" + format(em, '02d') + "-01"
                x = c.execute("SELECT COUNT(*) FROM BikeTrips WHERE START_DATE BETWEEN ? AND ?",(st,et,)).fetchall()
                print('Q2: Total number of records from ',y,'_QTR_',q,' is ',x[0][0],sep = "")
    conn.commit()
    conn.close()
    
if __name__ == "__main__":
	Q2()


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
#import
import os
import sqlite3
import csv
import glob
import pandas as pd
from math import radians, cos, sin, asin, sqrt
from itertools import permutations

#############################Q3#############################
def Q3(T1=31000,T2=31001):
    print("Q3")
    def haversine(lon1, lat1, lon2, lat2):
    # convert decimal degrees to radians
        lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    # haversine formula
        dlon = lon2 - lon1
        dlat = lat2 - lat1
        a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
        c = 2 * asin(sqrt(a))
        return c
    def Distance(lon1, lat1, lon2, lat2,unit):
        if unit == "miles" :
            r = 3956
        else :
            r = 6371
        return haversine(lon1,lat1,lon2,lat2)*r
#take terminal 31000 and 31001 as variables
    conn = sqlite3.connect('Capital_Bike_Share.db')
    c = conn.cursor()
    unit = "miles"
    unit2 = "kilometers"
    ex3_1 = str(T1)
    ex3_2 = str(T2)
    example = c.execute('SELECT TERMINAL_NUMBER, LATITUDE, LONGITUDE FROM BikeShare WHERE TERMINAL_NUMBER = ? OR TERMINAL_NUMBER = ?',(ex3_1,ex3_2)).fetchall()
    conn.commit()
    conn.close()
#r = 6371 # Radius of earth in kilometers. Use 3956 for miles
    print("Q3: The distance between TERMINAL_NUMBER", T1, "and",T2,"is", Distance(example[0][2], example[0][1], example[1][2], example[1][1],unit), "in", unit, "equals to",Distance(example[0][2], example[0][1], example[1][2], example[1][1],unit2), "in", unit2)
    return Distance
    
if __name__ == "__main__":
    Distance = Q3(T1=31000,T2=31001)

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
#import
import os
import sqlite3
import csv
import glob
import pandas as pd
from math import radians, cos, sin, asin, sqrt
from itertools import permutations

#############################Q4#############################
def Q4(Distance):
    print("Q4")

    conn = sqlite3.connect('Capital_Bike_Share.db')
    c = conn.cursor()
#Terminallist by cross join
    Terminalslist = c.execute('SELECT a.TERMINAL_NUMBER, a.LATITUDE, a.LONGITUDE, b.TERMINAL_NUMBER, b.LATITUDE, b.LONGITUDE FROM BikeShare as a JOIN BikeShare as b WHERE a.TERMINAL_NUMBER != b.TERMINAL_NUMBER;').fetchall()
#dictionary[t1,t2] = dist in miles
    Terminals_dictionary = {}
    for row in Terminalslist:
        Terminals_dictionary[row[0],row[3]] = Distance(row[2],row[1],row[5],row[4],"miles")
    conn.commit()
    conn.close()
    print("Q4: Dictionary created with total",len(Terminals_dictionary),"records")
    return Terminals_dictionary

if __name__ == "__main__":
    Terminals_dictionary = Q4(Distance)
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

#import
import os
import sqlite3
import csv
import glob
import pandas as pd
from math import radians, cos, sin, asin, sqrt
from itertools import permutations

#############################Q5#############################
#with using dictionary created from Q4
#take terminal 31000 and dis = 0.4 miles as variables
def Q5(Terminals_dictionary, T1 = 31000, mile = 0.4):
    print("Q5")
    ex5_1 = T1
    ex5_2 = mile
#filter
    lessDistance = {k:v for (k,v) in Terminals_dictionary.items() if v<ex5_2 if k[0]==float(ex5_1)}
    ANS = []
    for key in lessDistance.keys():
      ANS.append(key[1])
    print("Q5: The terminals within the distance",ex5_2,"miles from Terminal", ex5_1, "are\n", ANS)
    

if __name__ == "__main__":
	Q5(Terminals_dictionary, T1 = 31000, mile = 0.4)
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
#import
import os
import sqlite3
import csv
import glob
import pandas as pd
from math import radians, cos, sin, asin, sqrt
from itertools import permutations

#############################Q6#############################
def Q6(T1 = 31000,T2 = 31001,St = "2011-01-01",Et = "2015-12-31"):
    print("Q6")
    conn = sqlite3.connect('Capital_Bike_Share.db')
    c = conn.cursor()
#take terminal 31000 and 31001; startD = 2011-01-01; endD = 2015-12-31 as variables
    ex6_1 = str(T1)
    ex6_2 = str(T2)
    ex6_3 = St
    ex6_4 = Et
#sql
    for row1 in c.execute('SELECT COUNT(*) FROM BikeTrips WHERE START_STATION = ? AND STOP_STATION =? AND START_DATE BETWEEN ? AND ? AND STOP_DATE BETWEEN ? AND ?',(ex6_1,ex6_2 ,ex6_3,ex6_4,ex6_3,ex6_4)):
        print("Q6: The total number of trips made by riders between","\n","terminal", ex6_1,"and terminal", ex6_2,"within the period from",ex6_3,"to",ex6_4 ,"is",row1[0])
    conn.close()
    
if __name__ == "__main__":
    Q6(T1 = 31000,T2 = 31001,St = "2011-01-01",Et = "2015-12-31")
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
#import
import os
import sqlite3
import csv
import glob
import pandas as pd
from math import radians, cos, sin, asin, sqrt
from itertools import permutations

#############################Q7#############################
def Q7():
    Q1()
    Q2()
    Distance = Q3(T1=31000,T2=31001)
    Terminals_dictionary = Q4(Distance)
    Q5(Terminals_dictionary, T1 = 31000, mile = 0.4)
    Q6(T1 = 31000,T2 = 31001,St = "2011-01-01",Et = "2015-12-31")
    print("Q7")
if __name__ == "__main__":
    Q7()
    print("Thanks Professor")
#############################The END#############################
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
