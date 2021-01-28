# -*- coding: utf-8 -*-
"""
Created on Tue Jan 26 01:26:21 2021

@author: Ting-Yeh Yang
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