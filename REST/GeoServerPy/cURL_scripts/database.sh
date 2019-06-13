curl -v -u admin:geoserver -XPOST -H "Content-type: text/xml" -d "<workspace><name>upc</name></workspace>" http://localhost:8080/geoserver/rest/workspaces

curl -v -u admin:geoserver -XPOST -T config.xml -H "Content-type: text/xml" http://localhost:8080/geoserver/rest/workspaces/upc/datastores

curl -v -u admin:geoserver -XGET http://localhost:8080/geoserver/rest/workspaces/upc/datastores/upcdev.xml

curl -v -u admin:geoserver -XPOST -H "Content-type: text/xml" -d "<featureType><name>datafiles_w_footprints</name></featureType>" http://localhost:8080/geoserver/rest/workspaces/upc/datastores/upcdev/featuretypes

wget http://localhost:8080/geoserver/wms/reflect?layers=upc:datafiles_w_footprints
