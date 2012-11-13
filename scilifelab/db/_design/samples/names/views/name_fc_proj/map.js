var list; 
function(doc) {if (!doc["name"].match(/_[0-9]+$/)) {list = [doc["flowcell"], doc["sample_prj"]];emit(doc["name"], list);}}
