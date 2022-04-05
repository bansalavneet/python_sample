#!/usr/bin/env python

import sys, os
import string

import numpy as np
import pandas as pd

import datetime as dt

import sqlite3 as sq


####################################
# Functions to check FIGI Digit
####################################

## Map used by checkFIGIDigit function validating checkDigit on FIGI identifiers

figiAlphaNumMap = dict(zip([chr(v) for v in range(65,65+26)], range(10, 36)))

## sum of all digits of a given number in one pass
def digitSum(x):
    return ( sum(map(int,str(x))) )


## checkdigit on the full 12 digit figi string
## Algo: Working with the first 11 characters
##       convert all alphabets to numeric (A = 10, ... , Z = 35)
##       multiply every second number by 2
##       check digit = distance from 10 of sum of all digits obtained at previous step

def checkFIGIDigit(figi):

    if len(figi) != 12: return False

    if not figi.isalnum(): return False

    checkDigit = figi[len(figi)-1]
    figi11 = figi[:-1:] if len(figi) == 12 else figi
    xx = [figiAlphaNumMap[ch] if ch.isalpha() else int(ch) for ch in figi11 ]
    yy = [xx[i]*2 if i % 2 == 1 else xx[i] for i in range(len(xx))]
    zz = sum(map(digitSum, yy))
    tmp = (10-zz)%10

    return str(tmp) == checkDigit

## end __checkFIGIDigit


####################################
# Class to represent a Bbg data file
# Assumes basic structure like following fields are mandatory:
# START-OF-FILE , START-OF-FIELDS, END-OF-FIELDS, START-OF-DATA, END-OF-DATA
# Provides basic interfacing functions
# Does almost no data cleaning apart from FIGI check digit validation
# Data cleaning left to user of class
####################################

class BbgDataFile:
    """ Load a bloomberg data file based on file path """

    def __init__(self, fname):
    
        fchannel = open(fname)
        lines = fchannel.readlines()
        
        fields, dataList = self.parseFileText(lines)
    
        self.fields = fields
        self.dataList = dataList
        self.nrows = len(dataList)
        
    ## end __init__
    
    # get fields list
    def getFields(self):
        return self.fields        
    
    # get actual dataset
    def getDataList(self):
        return self.dataList
    
    # get size of dataset
    def nrows(self):
        return self.nrows
    
    # data retrieve function that gets data for all rows for said columns
    def getDataForFields(self, columns ):
   
        goodFields = [True if f in self.fields else False for f in columns]
        if not all(goodFields):
            raise Exception('Data not available for following fields in bbg data file:' + 
                            [ columns[i] for i in range(len(columns)) if not goodFields[i] ]) 

        outData = [ [row[k] for k in columns] for row in self.dataList]

        return outData
    


    
    def parseFileText(self, lines):
                    
        fields = list()
        fieldsLoc = dict()
        dataList = list()
        dataDict = dict()

        # state starts with -1 by default
        # state is set to 0 after basic file structure validation
        # state is set to 1 when reading fields
        # state is set to 2 when reading data rows
        
        state = -1

        for line in lines:
            line = line.strip()

            if state == -1 and line == 'START-OF-FILE':
                state = 0
                continue
            
            if state == 0:
                if line == 'START-OF-FIELDS':
                    state = 1
                    continue
                elif line == 'START-OF-DATA':
                    state = 2
                    continue
                else:
                    if 'DATARECORD' in line:
                        nrow = int(line.split('=')[1])
                        if nrow != len(dataList):
                            print('WARNING: Not all data rows loaded successfully from bbg file')
                            # Could be an Exception depending on how we want to handle this
                    # TODO
                    # Other things you want to do for lines outside of FIELDS or DATA ROWS
                    # set programm name, date format etc
                    continue

            if state == 1:
                if line == 'END-OF-FIELDS':
                    fieldsLoc = dict(zip(fields, range(len(fields) ) ) )
                    #print(fieldsLoc)
                    state = 0
                    continue
                else:
                    fields.append(line)

            if state == 2:
                if line == 'END-OF-DATA':
                    #print(len(dataList))
                    state = 0
                    continue
                else:
                    data = line.split('|')[3:]
                    datarow = data[0:(len(data)-1)]
                    if len(datarow) != len(fields):
                        raise Exception('Insufficient values found in row')
                    figi = datarow[-1]
                    checkDigitValid = checkFIGIDigit(figi)
                    if (not checkDigitValid):
                        print('FIGI check digit invalid, skipping line for ID_BB_GLOBAL: ' + figi )
                        continue
                    datarow = dict(zip(fields, datarow))
                    dataList.append(datarow)
                    dataDict[figi] = datarow

        if (state == -1):
            raise Exception('File Structure Invalid. Could not find START-OF-FILE')

        if (state != 0):
            raise Exception('File Structure Invalid. File does not end gracefully')

        return( (fields, dataList) )

    

# Basic data cleaning functions to handle empty strings, N.A. strings
# Used just before preparing data to enter into database

def cleanVal(val):
    val = val.strip()
    if val == '' or val == 'N.A.':
        return 'NULL'
    else:
        return val

def cleanDate(dtstr):
    dtstr = dtstr.strip()
    if (dtstr == '' or dtstr == 'N.A.'):
        return 'NULL'
    else: 
        return format(dt.datetime.strptime(dtstr, '%Y%m%d'), '%Y-%m-%d')



##############################
# update Pref Static Data function
# Inputs are the bbg object, and the database path,
# Given the above, retrieve data from bbg data object based on definition defined in function (could be moved to config)
# Cleans the data and  enters  into database
##############################

def updatePrefStatic(bbgdata, dbpath) :

    print()
    print('Updating DB with Pref Static data')
    
    if not os.path.isfile(dbpath):
        print('Cannot find specified perf db:' + dbpath)
        print('New one will be created')

    # Open db if exists or create new one
    conn = sq.connect(dbpath)
    dbcur = conn.cursor()
    
    

    ## configs for static table    
    
    tableName = 'PrefStatic'

    # Fields to be retrieved from bloomberg data file

    prefStaticFields  = ['ID_BB_GLOBAL',
                         'NAME',
                         'CRNCY',
                         'CPN']

    # Column headers that will be entered into database

    columnHeaders = ['FIGI',
                      'Name',
                      'Currency',
                      'Coupon']

    # Must match size of columnHeaders. Used when table is created.

    headerTypes = {'FIGI' : 'TEXT',
                   'Name' : 'TEXT',
                   'Currency' : 'TEXT',
                   'Coupon': 'REAL'}

    create_table_sql = """CREATE TABLE IF NOT EXISTS """ + tableName + " (" + \
                         ", ".join([ " ".join([header, headerTypes[header]]) for header in columnHeaders ]) + ")"  
    
    # CREATE TABLE IF NOT EXISTS PrefStatic (FIGI TEXT, Name TEXT, Currency TEXT, Coupon REAL)
    
    dbcur.execute(create_table_sql)
    

    # Fetch static data from bbg data file

    print('Retrieving prefs Static Data from bbg object')

    prefStaticData = bbgdata.getDataForFields(columns = prefStaticFields)

    # Clean data (change missing or empty values to NULL, any other claculations)
    prefStaticDataInsert = [ tuple([cleanVal(x) for x in row]) for row in prefStaticData ]

    print(str(len(prefStaticDataInsert)) + ' rows found')
        
    insert_static_sql = """INSERT INTO """ + tableName + """ VALUES (""" + ",".join(['?' for x in columnHeaders]) + """) """
    
    dbcur.executemany(insert_static_sql,  prefStaticDataInsert) 
    
    print('Inserted ' + str(len(prefStaticDataInsert)) + ' rows into PrefStatic Table')
    
    print('Committing new data')
    conn.commit()
    
    conn.close()



##############################
# update Pref Price Data function
# Inputs are the bbg object, and the database path,
# Given the above, retrieve data from bbg data object based on definition defined in function (could be moved to config)
# Cleans the data and  enters  into database
##############################



def updatePrefPrice(bbgdata, dbpath) :

    print()
    print('Updating DB with Pref Price data')
    
    if not os.path.isfile(dbpath):
        print('Cannot find specified perf db:' + dbpath)
        print('New one will be created')
        

    # Open db if exists or create new one
    conn = sq.connect(dbpath)
    dbcur = conn.cursor()
    
    
    ## configs for price table
    
    tableName = 'PrefPrice'

    prefPriceFields  = ['ID_BB_GLOBAL', 
                        'PX_CLOSE_DT',
                        'PX_LAST',
                        'YLD_YTM_MID',
                        'MATURITY',
                        'YLD_YTC_MID',
                        'NXT_CALL_DT']

    columnHeaders = ['EquityId',
                     'Date',
                     'Price',
                     'YieldToMaturity',
                     'ConventionalYieldTW',
                     'WorstDate']

    headerTypes = {'EquityId' : 'TEXT',
                   'Date' : 'TEXT',
                   'Price' : 'REAL',
                   'YieldToMaturity' : 'REAL',
                   'ConventionalYieldTW' : 'REAL',
                   'WorstDate' : 'TEXT'}


    create_table_sql = """CREATE TABLE IF NOT EXISTS """ + tableName + " (" + \
                        ", ".join([ " ".join([header, headerTypes[header]]) for header in columnHeaders ]) + "," + \
                        """ FOREIGN KEY (EquityId) REFERENCES PrefStatic(FIGI) )"""    

    # create_table_sql = 
    # "CREATE TABLE IF NOT EXISTS PrefPrice 
    # (EquityId TEXT, Date TEXT, Price REAL, YieldToMaturity REAL, ConventionalYieldTW REAL, WorstDate TEXT,
    # FOREIGN KEY (EquityId) REFERENCES PrefStatic(FIGI) )"

    dbcur.execute(create_table_sql)
    

    # Get pref Price Data, clean, 

    prefPriceData = bbgdata.getDataForFields(columns = prefPriceFields)


    # Clean data ( convert empty and missing to NULL, change date format , calculate YTW)
    
    prefPriceDataInsert = list()
    for row in prefPriceData:
        equityid = row[0]
        px_dt    = cleanDate(row[1])
        px       = cleanVal(row[2])
        ytm      = cleanVal(row[3])
        maturity = cleanDate(row[4])
        ytc      = cleanVal(row[5])
        call_dt  = cleanDate(row[6])
        ytw      = 'NULL'
        ytw_dt   = 'NULL'
        if not ytm == 'NULL' and not ytc == 'NULL':
            if (ytm < ytc):
                ytw = ytm
                ytw_dt = maturity
            else:
                ytw = ytc
                ytw_dt = call_dt
        else:
            if not ytm == 'NULL':
                ytw = ytm
                ytw_dt = maturity
            else:
                ytw = ytc
                ytw_dt = call_dt
        
        prefPriceDataInsert.append( tuple([equityid, px_dt, px, ytm, ytw, ytw_dt]))


    print(str(len(prefPriceDataInsert)) + ' rows found')

    # Insert data into database
 
    insert_static_sql = """INSERT INTO """ + tableName + """ VALUES (""" + ",".join(['?' for x in columnHeaders]) + """) """
    
    dbcur.executemany(insert_static_sql,  prefPriceDataInsert) 
    
    print('Inserted ' + str(len(prefPriceDataInsert)) + ' rows into PrefPriceData Table')
    
    print('Committing new data')
    conn.commit()
    
    conn.close()
 

########################
# Testing Code 
########################

#bbgfname = 'preferreds.price.20110510'
#bbgdata = BbgDataFile(bbgfname)

#dbpath = 'datadb.db'

#updatePrefStatic(bbgdata, dbpath = dbpath)
#updatePrefPrice(bbgdata, dbpath = dbpath)



def usage():
    print('Usage: perfsDailyUpdate.py perfs_bbg_data_file_path sqlite_db_path')
    


############ MAIN ###############

def main():

    if len(sys.argv[1:]) < 2:
        usage()
        return
    
    bbgfname = sys.argv[1]
    dbpath   = sys.argv[2]
    
    
    if not os.path.isfile(bbgfname):
        Exception('Cannot find specified bbg data file:' + bbgfname)
    
    
    ######
    # Load Bbg data file
    ######
    
    print('Loading Bloomberg Data File:' + bbgfname)
    bbgdata = BbgDataFile(bbgfname)


    
    ######
    # Get Prefs data and load into DB
    ######
    
    try:

        status = updatePrefStatic(bbgdata, dbpath = dbpath)
    
        status = updatePrefPrice(bbgdata, dbpath = dbpath)

    except sq.Error as er:
        print er
	raise er

    print('done')
           
    
if __name__ == '__main__':
    main()

