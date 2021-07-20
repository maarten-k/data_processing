###########################################################################
###########################################################################


import gridtools
import credentials
import copy
import sys, re, os



###########################################################################
###########################################################################


# Connect to CouchDB and Setup View
db=gridtools.connect_to_couchdb(url=credentials.URL, username=credentials.USERNAME, password=credentials.PASS, dbname=credentials.DBNAME)
picasitems = {
 
    "language": "javascript",
    "views": {
        "overview": {
            "map": "function(doc) {\nif(doc[\"type\"]== \"token\"){\n\tif(doc[\"done\"] == 0 && doc[\"lock\"]==0  ){  emit(\"todo\",1);}\n\telse if(doc[\"done\"]== 0 && doc[\"lock\"]>= 1  ){ emit(\"locked\",1);}\n\telse if(doc[\"done\"]>= 1 && doc[\"lock\"]>= 1  ){ emit(\"done\",1);}\n\telse{emit(\"unknown_token_status\",1)}\n}\n\n}",
            "reduce": "function(key,values,rereduce){return sum(values);}"
        },
        "todo": {
            "map": "function(doc) {\nif(doc[\"type\"]== \"token\"){\n\tif(doc[\"done\"]==0 && doc[\"lock\"]==0  ){\n\t\tif(\"total_chunks\" in doc){\n\t\t\tif (doc[\"total_chunks\"]==doc[\"chunks_ready\"]){\n\t\t\t\temit(doc._id, doc._id);\n\t\t\t}\n\t\t}else{\n\t  \t\temit(doc._id, doc._id);\n\t\t}\n\t\n\t}\n}\n}"
        },
        "done": {
            "map": "function(doc) {\nif(doc[\"type\"]== \"token\"){\n\tif(doc[\"done\"]!= 0){\n  emit(doc[\"_id\"], doc[\"_id\"]);\n}\n}\n}"
        },
        "locked": {
            "map": "function(doc) {\n\tif(doc[\"type\"]== \"token\"){\n\t\tif(doc[\"done\"]== 0 && doc[\"lock\"]>= 1  ){ \n  \t\t\temit(doc[\"_id\"],doc);\n\t\t}\n\t}\n}"
        },
	"error": {
           "map": "function(doc) {\n\tif(doc[\"type\"]==\"token\"){if (doc[\"lock\"]==-1 && doc[\"done\"]==-1) {\n\t\t\temit(doc[\"_id\"], doc);\n\t\t}\n\t}\n}"
       }
    }
}




###########################################################################
###########################################################################



# Load Views to Create into a List
inp = sys.argv[1]
with open(inp, 'r') as f:
	myViews = [ line.rstrip('\n') for line in f ]
	f.close()


# Create View with the above setup
for stage in myViews:
    picas_new = copy.deepcopy(picasitems)
    for key in picas_new["views"].keys():
        picas_new["views"][key]["map"] = picas_new[
            "views"][key]["map"].replace("token", stage)
    print("_design/" + stage)
    if "_design/" + stage not in db:
        print("creating view:"+str(stage))
        db["_design/" + stage] = picas_new
    del picas_new
