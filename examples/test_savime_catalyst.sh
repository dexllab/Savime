#!/bin/bash

#Creating TARs
savimec 'create_tar("top", "IncidenceTopology", "implicit, incidentee, int, 0, *, 1", "incident, long:5 | type, int", "incident, incident, incidentee, incidentee, type, celltype");'
savimec 'create_tar("geo", "CartesianGeometry", "implicit, index, long, 0, *, 1", "coords, double:3", "index, index, coords, coords");'
savimec 'create_tar("data", "UnstructuredFieldData", "implicit, time, int, 0, *, 1| implicit, index, long, 0, 92540, 1", "pressure,float", "time, time, index, index");'

#Creating datasets
savimec 'create_dataset("top_data:long:5", "@'$(pwd)'/top.data");'
savimec 'create_dataset("type_data:int", "@'$(pwd)'/type.data");'
savimec 'create_dataset("hemo:float", "@'$(pwd)'/hemo.data");'
savimec 'create_dataset("coordinates:double:3", "@'$(pwd)'/coords.data");'

savimec 'load_subtar("top", "ordered, incidentee, 0, 460198", "incident, top_data | type, type_data");'
savimec 'load_subtar("geo", "ordered, index, 0, 92539", "coords, coordinates");'
savimec 'load_subtar("data",  "ordered, time, 0, 239| ordered, index, 0, 92539", "pressure, hemo");'

#Creating vizualizations
mkdir $(pwd)/viz
#savimec 'catalyze(subset(data, time, 0, 20), where(geo, coords:2 < -13.0 ), top, "'$(pwd)'/viz/", "'$(pwd)'/catalyst_grad.py");'
savimec 'catalyze(subset(data, time, 0, 20), where(geo, coords:2 < -13.0 ), top, "'$(pwd)'/viz/");'
#savimec 'scan(dimjoin(data, data, time, time, index, index));'
#savimec 'scan(derive(data, p, (pressure-32)/5));'

