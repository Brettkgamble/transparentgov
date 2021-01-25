from os import environ
from sqlalchemy import MetaData, create_engine, asc
from sqlalchemy.orm import sessionmaker

import psycopg2
from psycopg2 import Error

# from flask import Flask, jsonify, request
# from flask_cors import cross_origin, CORS
# from models.models import Base
# from endpointClasses.resources import Resources
#
# from sqlalchemy import Column, Integer, BigInteger, String, Text, DateTime, \
#     Float, Boolean, func, ForeignKeyConstraint, Index, ForeignKey
# from sqlalchemy.ext.declarative import declarative_base
# from sqlalchemy.orm import relationship, backref
# from sqlalchemy.ext.hybrid import hybrid_property, hybrid_method
# from datetime import datetime, timedelta, timezone
# from geoalchemy2 import Geography, Geometry

def main():

    # engine below for local PostgresSql access
    try:
        # connect to existing database
        connection = psycopg2.connect(user="postgres",
                                      password="5413CrossFit2018",
                                      host="localhost",
                                      port="5433",
                                      database="postgres")
        # Create a cursor to perform database operations
        cursor = connection.cursor()
        # Print PosthreSQL details
        print("PostgreSQL information")
        print(connection.get_dsn_parameters(), "\n")
        # Executing a SQL query
        cursor.execute("SELECT version();")
        # Fetch Result
        record = cursor.fetchone()
        print("You are connected to - ", record, "\n")

    except (Exception, Error) as error:
        print("Error while connecting to PostgreSQL", error)

    finally:
        if (connection):
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")

    # # Engine below for Google Cloud PostgreSQL access.
    # engine = create_engine(
    #     'postgresql+psycopg2://postgres:5413CrossFit2018@35.232.127.36/test')
    #
    # Session = sessionmaker(bind=engine)
    # session = Session()

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
