import os
from pymongo.connection import Connection
from pymongo import MongoClient
from progressbar import ProgressBar
from osgeo import ogr
import time 





database="gisdb"
client = MongoClient('localhost', 27017)
db = client[database]
db.collection_names()
db.usa_counties.find({"geom": {"$geoWithin": {"$box": [[33.100745,-122.080078], [47.517201,-67.763672]]}}})
#db.usa_counties.findOne()
