import os
from pymongo.connection import Connection
from progressbar import ProgressBar
from osgeo import ogr

def mongodb2shape(mongodb_server, mongodb_port, mongodb_db, mongodb_collection, output_shape):
        """Convert a mongodb collection (all elements must have same attributes) to a shapefile"""
        print ' Converting a mongodb collection to a shapefile '
        connection = Connection(mongodb_server, mongodb_port)
        print 'Getting database MongoDB %s...' % mongodb_db
        db = connection[mongodb_db]
        print 'Getting the collection %s...' % mongodb_collection
        collection = db[mongodb_collection]
        print 'Exporting %s elements in collection to shapefile...' % collection.count()
        drv = ogr.GetDriverByName("ESRI Shapefile")
        ds = drv.CreateDataSource(output_shape)
        lyr = ds.CreateLayer('test', None, ogr.wkbUnknown)
        print 'Shapefile %s created...' % ds.name
        cursor = collection.find()
        # define the progressbar
        pbar = ProgressBar(collection.count()).start()
        k=0
        # iterate the features in the collection and copy them to the shapefile
        # for simplicity we export only the geometry to the shapefile
        # if we would like to store also the other fields we should have created a metadata element with fields datatype info
        for element in cursor:
                element_geom = element['geom']
                feat = ogr.Feature(lyr.GetLayerDefn())
                feat.SetGeometry(ogr.CreateGeometryFromWkt(element_geom))
                lyr.CreateFeature(feat)
                feat.Destroy()
                k = k + 1
                pbar.update(k)
        pbar.finish()
        print '%s features loaded in shapefile from MongoDb.' % lyr.GetFeatureCount()
  
output_shape ='C:\\Users\\Administrator\\Desktop\\shp img\\bou1_42.shp'

driver = ogr.GetDriverByName('ESRI Shapefile')
if os.path.exists(output_shape):
        driver.DeleteDataSource(output_shape)
mongodb_server = 'localhost'
mongodb_port = 27017
mongodb_db = 'gisdb'
mongodb_collection = 'usa_counties'

print 'Exporting data from mongodb...'
mongodb2shape(mongodb_server, mongodb_port, mongodb_db, mongodb_collection, output_shape)

