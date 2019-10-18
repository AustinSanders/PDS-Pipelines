#### 1. Creating a workspace (only needed if one does not exist for the current project)
- create new workspace
- give a name
- add a uri (localhost:8080/geoserver/name_goes_here or something similar)


#### 2. Create a store (need to do this if getting information from a new database)
- name the store
- enter database information
  - remember to add the database name


#### 3. Create a layer
- create new layer
- select store (the database)
- select the view you want to make a layer from
- set boundaries
  - native bounding box: compute from data
  - lat/lon bounding box: computer from native bounds
  
#### 4. layer preview
- After a layer is created, click layer preview from the menu on the left.
- Then you can view the layer. Click openlayers to open an interactive map in a new tab.
  
#### See https://docs.geoserver.org/stable/en/user/gettingstarted/postgis-quickstart/index.html for more info.
