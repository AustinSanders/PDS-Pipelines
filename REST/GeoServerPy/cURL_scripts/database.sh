
# Creates  a workspace in your running instance of GeoServer
curl -v -u admin:geoserver -XPOST -H "Content-type: text/xml" -d "<workspace><name>upc</name></workspace>" http://localhost:8080/geoserver/rest/workspaces

# Creates a DB connection which is called a store in GeoServer
curl -v -u admin:geoserver -XPOST -T config.xml -H "Content-type: text/xml" http://localhost:8080/geoserver/rest/workspaces/upc/datastores

# Gets the info about the database back so you can make sure it worked
curl -v -u admin:geoserver -XGET http://localhost:8080/geoserver/rest/workspaces/upc/datastores/upcdev.xml

# This makes a table form the database available from the database or 'publish' it
curl -v -u admin:geoserver -XPOST -H "Content-type: text/xml" -d "<featureType><name>datafiles_w_footprints</name></featureType>" http://localhost:8080/geoserver/rest/workspaces/upc/datastores/upcdev/featuretypes

# This returns an image from the datafiles_w_footprints table from the upcdev database as an example
wget http://localhost:8080/geoserver/wms/reflect?layers=upc:datafiles_w_footprints
