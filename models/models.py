from sqlalchemy import Column, Integer, BigInteger, String, Text, \
    Date, DateTime, Float, Boolean, func, ForeignKeyConstraint, Index, \
    ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, backref
from sqlalchemy.ext.hybrid import hybrid_property, hybrid_method
from datetime import date, datetime, timedelta, timezone
from geoalchemy2 import Geography, Geometry

import os
import json


Base = declarative_base()

class ExpensesRaw(Base):
    """
    Raw Expenses Table
    """
    __tablename__ = 'expensesraw'

    id = Column(Integer(), autoincrement=True, primary_key=True)
    ministry = Column(String())
    position = Column(String())
    name = Column(String())
    type = Column(String())
    category = Column(String())
    expense_date = Column(DateTime())
    amount = Column(Float())
    description = Column(String())
    receipt1 = Column(String())
    receipt2 = Column(String())
    receipt3 = Column(String())
    is_duplicated = Column(Boolean(), default=False)
    date_last_found = Column(DateTime(), default=datetime.now)
    created_at = Column(DateTime(), default=datetime.now)
    updated_at = Column(DateTime(), default=datetime.now,
                        onupdate=datetime.now)

    def __repr__(self):
        return "ExpensesRaw(id='{self.id}' , " \
            "ministry='{self.ministry}' , " \
            "position='{self.position}' , " \
            "name='{self.name}' , " \
            "type='{self.type}' , " \
            "expense_date='{self.expense_date}' , " \
           "amount='{self.amount}' , " \
           "description='{self.description}' , " \
            "receipt1='{self.receipt1}' , "\
            "receipt2='{self.receipt2}' , " \
            "receipt3='{self.receipt3}' , " \
               "is_duplicated='{self.is_duplicate}' , " \
               "date_last_found='{self.date_last_found}' , " \
               "created_at='{self.created_at}' , " \
               "updated_at='{self.updated_at}')".format(self=self)

class Filenames(Base):
    """
    Filenames Table
    """
    __tablename__ = 'filenames'

    id = Column(Integer(), autoincrement=True, primary_key=True)
    name = Column(String())
    records = Column(Integer())
    created_at = Column(DateTime(), default=datetime.now)

    def __repr__(self):
        return "Filenames(id='{self.id}' , " \
            "name='{self.name}' , " \
               "records='{self.records}' , " \
               "created_at='{self.created_at}')".format(self=self)