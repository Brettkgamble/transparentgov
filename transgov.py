import requests
import datetime
import time
import os
import csv
import pandas as pd
# from os import environ
from sqlalchemy import MetaData, create_engine, asc
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError

import psycopg2
from psycopg2 import Error

from flask import Flask, jsonify, request
from flask_cors import cross_origin, CORS
from models.models import Base

from models.models import ExpensesRaw, Filenames
# from endpointClasses.resources import Resources
#
from sqlalchemy import Column, Integer, BigInteger, String, Text, DateTime, \
    Float, Boolean, func, ForeignKeyConstraint, Index, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, backref
from sqlalchemy.ext.hybrid import hybrid_property, hybrid_method
from datetime import datetime, timedelta, timezone
from geoalchemy2 import Geography, Geometry

run_all_flag = True

### Setup the application
app = Flask(__name__)

# Wrap CORS around the app so that the server does not block machine to machine
# or browser based requests
CORS(app)

# Engine below for Google Cloud PostgreSQL access.
engine = create_engine('postgresql+psycopg2://postgres:5413CrossFit2018@34'
                       '.70.40.80/transgov')

Base.metadata.create_all(engine)

Session = sessionmaker(bind=engine)
session = Session()

# Column Names of each column in the dataset
col_names = ["Ministry", "Position", "Name", "Type", "Category", "Date", "Amount",
         "Description", "Receipt 1", "Receipt 2", "Receipt 3"]

def getExpensesFromSource():
    # download  csv file
    # Only do this once per day
    print('Downloading .....')
    df = pd.read_csv(r'https://expenses.alberta.ca/DownloadData.aspx?type=csv'
                     r'&d=IsVE/OcdpNZJ5rBbvji3qw', names=col_names,
                     low_memory=False, parse_dates=['Date'])
    # Saving the dataframe
    print('Saving to CSV ...')
    filename = 'expenses/' + str(time.strftime('%Y%m%d')) + ".csv"
    df.to_csv(filename)
    print('done...')

    return filename

def loadDfFromFile(filename):
    # We do this because we want to skip the heading rows
    print('Reading from file...', filename)
    # filename = "/" + filename
    df=pd.read_csv(filename, names=col_names, low_memory=False, skiprows=2)
    df_size = len(df.index)
    print('Done...', df_size)
    return df

def fileCompare():
    # compare the current download to the most previous download so that we can identify
    # the changes and only process those.
    # 1. Check file sizes
    # 2. iterate through DF1 and lookup record in DF2
    # 3.   if record exists then do nothing
    # 4.   is this new record or a changed one? (hard to determine....we may have to manually see if dups become a problem)

    # 4.   otherwise add record to database

    # get the most recent entry in the table FileNames
    prevFile = pd.read_sql(
            session.query(Filenames).
        order_by(Filenames.id.desc()).statement, session.bind)

    previousFile = prevFile._get_value(0,'name')
    print('Reading from previous file...', previousFile)

    # open the file as a dataframe
    prev_df = pd.read_csv(previousFile, names=col_names, low_memory=False, skiprows=2)
    prev_df_size = len(prev_df.index)
    print('Done...', prev_df_size)
    return prev_df

def saveFileNameToTable(filename, df_size):
    # save file name to filenames table in database
    filetosave = Filenames(
            name=filename,
            records = df_size,
            created_at=datetime.now()
        )
    try:
        session.add(filetosave)
        session.commit()
    except SQLAlchemyError as e:
        session.rollback()
        print('Could not save the new filename %s', filetosave)


def dataframe_difference(df1, df2, which=None):
    """Find rows which are different between two DataFrames."""
    # compare the two dataframes
    # https://hackersandslackers.com/compare-rows-pandas-dataframes/
    comparison_df = df1.merge(
        df2,
        indicator=True,
        how='outer'
    )
    if which is None:
        diff_df = comparison_df[comparison_df['_merge'] != 'both']
    else:
        diff_df = comparison_df[comparison_df['_merge'] == which]
    diff_df.to_csv('expenses/diff.csv')

    return diff_df

def dataframe_diff(df, prev_df):
    df_diff_df = dataframe_difference(df, prev_df)
    return df_diff_df

def mergeAndSaveDataframeDiff(df_diff_df):
    df_diff_df._merge.unique()
    filename = 'expenses/' + 'diff_' + str(time.strftime('%Y%m%d')) + ".csv"
    df_diff_df.to_csv(filename)

def addNewDataToExpensesRaw(df_diff_df):
    # Add left_only to the table expensesraw and update the 'changed' field to true for any that are right_only
    start_time = time.time()
    length = len(df_diff_df)
    ctr = 0

    for index, row in df_diff_df.iterrows():

        newdate = df_diff_df._get_value(index, 'Date')
        newdate = datetime.strptime(newdate, '%m/%d/%Y')
        newAmount = df_diff_df._get_value(index, 'Amount').replace('$',
                                                                   '').replace(
            ',', '')
        newAmount = float(newAmount)

        if len(str(df_diff_df._get_value(index, 'Receipt 1'))) > 5:
            newReceipt1 = df_diff_df._get_value(index, 'Receipt 1')
        else:
            newReceipt1 = ''
        if len(str(df_diff_df._get_value(index, 'Receipt 2'))) > 5:
            newReceipt2 = df_diff_df._get_value(index, 'Receipt 2')
        else:
            newReceipt2 = ''
        if len(str(df_diff_df._get_value(index, 'Receipt 3'))) > 5:
            newReceipt3 = df_diff_df._get_value(index, 'Receipt 3')
        else:
            newReceipt3 = ''

        if df_diff_df._get_value(index, '_merge') == 'left_only':
            changed = False

        if df_diff_df._get_value(index, '_merge') == 'right_only':
            changed = True

        expense = ExpensesRaw(
            ministry=df_diff_df._get_value(index, 'Ministry'),
            position=df_diff_df._get_value(index, 'Position'),
            name=df_diff_df._get_value(index, 'Name'),
            type=df_diff_df._get_value(index, 'Type'),
            category=df_diff_df._get_value(index, 'Category'),
            expense_date=newdate,
            amount=newAmount,
            description=df_diff_df._get_value(index, 'Description'),
            receipt1=newReceipt1,
            receipt2=newReceipt2,
            receipt3=newReceipt3,
            changed=changed,
            date_last_found=datetime.now(),
            created_at=datetime.now(),
            updated_at=datetime.now(),
        )

        if ctr % 100 == 0:
            perc = "{:.3f}".format(ctr / length)
            elapsed = "{:.2f}".format(time.time() - start_time)
            print('Count: %s of %s percentage %s elapsed %s ' % (
            ctr, length, perc, elapsed))

        ctr = ctr + 1

        try:
            session.add(expense)
            session.commit()
        except SQLAlchemyError as e:
            session.rollback()
            print('Could not save the new expense %s',
                  expense)