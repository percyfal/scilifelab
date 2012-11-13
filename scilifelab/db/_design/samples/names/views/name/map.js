function(doc) {if (!doc["name"].match(/_[0-9]+$/)) {emit(doc["name"], null);}}
