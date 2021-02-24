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