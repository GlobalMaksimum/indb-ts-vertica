import verticapy as vp
from verticapy import vDataFrame

# Creating a new connection
vp.new_connection({"host": "172.12.2.10",
                   "port": "5433",
                   "database": "vsunny",
                   "password": "xxx",
                   "user": "dbadmin"},
                   name = "MyVerticaConnection")

# Connecting to the Database
vp.connect("MyVerticaConnection")
vp.create_verticapy_schema()

from verticapy.learn.tsa import SARIMAX

s = SARIMAX("daily_trip",p=2,d=3,q=2,s=30,P=1)


s.drop()

vp.set_option("sql_on", True)
s.fit('nyc.bike_trip_daily', "n", ts="starttime_dt")

test  = vDataFrame('nyc.bike_trip_daily')
s.predict(test, nlead=10, name="y_pred").tail(100)
