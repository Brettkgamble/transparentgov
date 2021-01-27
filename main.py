import requests
import os
import csv
import pandas as pd
# from os import environ
from sqlalchemy import MetaData, create_engine, asc
from sqlalchemy.orm import sessionmaker

import psycopg2
from psycopg2 import Error

from flask import Flask, jsonify, request
from flask_cors import cross_origin, CORS
from models.models import Base
# from endpointClasses.resources import Resources
#
from sqlalchemy import Column, Integer, BigInteger, String, Text, DateTime, \
    Float, Boolean, func, ForeignKeyConstraint, Index, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, backref
from sqlalchemy.ext.hybrid import hybrid_property, hybrid_method
from datetime import datetime, timedelta, timezone
from geoalchemy2 import Geography, Geometry

### Setup the application
app = Flask(__name__)

# Wrap CORS around the app so that the server does not block machine to machine
# or browser based requests
CORS(app)



def main():

    print('In Main')

    # engine below for local PostgresSql access

    # try:
    #     # connect to existing database - localhost
    #     connection = psycopg2.connect(user="postgres",
    #                                   password="5413CrossFit2018",
    #                                   host="localhost",
    #                                   port="5433",
    #                                   database="postgres")
    #     # Create a cursor to perform database operations
    #     cursor = connection.cursor()
    #     # Print PosthreSQL details
    #     print("PostgreSQL information")
    #     print(connection.get_dsn_parameters(), "\n")
    #     # Executing a SQL query
    #     cursor.execute("SELECT version();")
    #     # Fetch Result
    #     record = cursor.fetchone()
    #     print("You are connected to - ", record, "\n")
    #
    # except (Exception, Error) as error:
    #     print("Error while connecting to PostgreSQL", error)
    #
    # finally:
    #     if (connection):
    #         cursor.close()
    #         connection.close()
    #         print("PostgreSQL connection is closed")

    # Engine below for Google Cloud PostgreSQL access.
    engine = create_engine('postgresql+psycopg2://postgres:5413CrossFit2018@34'
                           '.70.40.80/transgov')

    Base.metadata.create_all(engine)

    Session = sessionmaker(bind=engine)
    session = Session()


    # download  csv file
    print('Downloading .....')

    # col_names = ["Ministry", "Position", "Name", "Type", "Date", "Amount",
    #          "Description", "Receipt 1", "Receipt 2", "Receipt 3"]
    # df = pd.read_csv(r'https://expenses.alberta.ca/DownloadData.aspx?type=csv'
    #                r'&d=IsVE/OcdpNZJ5rBbvji3qw', names=col_names,
    #                  low_memory=False)
    #
    # # Saving the dataframe
    # df.to_csv('expenses.csv')
    #
    # print(df.head())

    # Save to expenses table
    print('Saving csv locally .....')
    # Identify dups.  If duplicate then add to duplicate table
    # if record already exists then simply update the lastseen field with
    # current date



# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
