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
