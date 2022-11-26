import pandas as pd
import numpy as np
import os
import sqlalchemy
from sqlalchemy import create_engine, MetaData
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Numeric, Text, Float, Date

engine = create_engine("sqlite:///database.db", echo=True)
conn = engine.connect()

Base = declarative_base()

#poprawki
measurements = pd.read_csv('/clean_measure.csv')
stations = pd.read_csv('/clean_stations.csv')

class Measurement(Base):
    __tablename__ = "measurements"

    index = Column(Integer, primary_key=True)
    station = Column(Text)
    date = Column(Text)
    prcp = Column(Float)
    tobs = Column(Integer)

    def __repr__(self):
        return f"id={self.id}, name={self.name}"


class Station(Base):
    __tablename__ = "stations"

    station = Column(Text, primary_key=True)
    name = Column(Text)
    latitude = Column(Float)
    longitude = Column(Float)
    elevation = Column(Float)

    def __repr__(self):
        return f"id={self.id}, name={self.name}"


Base.metadata.create_all(engine)

Mdata = measurements.to_dict(orient="records")
Sdata = stations.to_dict(orient="records")

metadata = MetaData(bind=engine)
metadata.reflect()

Mtable = sqlalchemy.Table("measurements", metadata, autoload=True)
Stable = sqlalchemy.Table("stations", metadata, autoload=True)

conn.execute(Mtable.delete())
conn.execute(Stable.delete())

conn.execute(Mtable.insert(), Mdata)
conn.execute(Stable.insert(), Sdata)

result = conn.execute("SELECT * FROM stations LIMIT 5").fetchall()
for row in result:
    print(row)

ad1 = Stable.select().where(Stable.c.latitude == "21.2716").limit(6)
result_ad1 = conn.execute(ad1)
for row in result_ad1:
    print(row)

ad2 = Stable.update().where(Stable.c.station == "USC00518838").values(latitude ='111')
result_ad2 = conn.execute(ad2)

ad3 = Stable.delete().where(Stable.c.latitude == "21.5213")
result_ad3 = conn.execute(ad3)
