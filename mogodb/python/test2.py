import pymongo

from pymongo.connection import Connection
import shapefile

def readSHPPoint(append):
    fileP = 'F:\\project\\Kalium_web\\shapefile\\co99_d00.shp'
    sf = shapefile.Reader(fileP)
    shapeRecs = sf.shapeRecords()

    mongodb_server='127.0.0.1'
    mongodb_port = 27017
    mongodb_collection ='test1'
    mongodb_db = 'gisdb'
    connection = Connection(mongodb_server, mongodb_port)
    print 'Getting database %s' % mongodb_db
    db = connection[mongodb_db]
    print 'Getting the collection %s' % mongodb_collection
    collection = db[mongodb_collection]
    if append == False:
        print 'Removing features from the collection...'
        collection.remove({})
    print 'Starting loading features...'
        
    for shaperec in shapeRecs:
        mongofeat = {}
        #'{x='',y=''}'
        strX = "%.3f" % shaperec.shape.points[0][0]
        strY = "%.3f" % shaperec.shape.points[0][1]
        mongogeom = '{x="'+strX+'",y="'+strY+'"}'
        print mongogeom
        mongofeat['geom'] = mongogeom
        mongofeat['name'] = shaperec.record[1]
        mongofeat['tmp'] = shaperec.record[2]
        collection.insert(mongofeat)
    #create 2d index
    collection.create_index([("geom", pymongo.GEO2D)])



if __name__ == "__main__": 
  readSHPPoint(False)
