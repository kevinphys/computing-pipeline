import os, requests, json, datetime
from urllib3.filepost import encode_multipart_formdata

"""Remove periods from JSON keys - Clowder can't handle these in metadata"""
def clean_json_keys(jsonobj):
    clean_json = {}
    for key in jsonobj.keys():
        try:
            jsonobj[key].keys() # Is this a json object?
            clean_json[key.replace(".","_")] = clean_json_keys(jsonobj[key])
        except:
            clean_json[key.replace(".","_")] = jsonobj[key]
            
    return clean_json

def createCollectionIfNecessary(type, name, sess):
    if type == "SENSOR" and name in SENSOR_COLLECTION_IDS:
        return SENSOR_COLLECTION_IDS[name]
    elif type == "DATE" and name in DATE_COLLECTION_IDS:
        return DATE_COLLECTION_IDS[name]

    if type == "SENSOR":
        description = "All datasets from this sensor."
    elif type == "DATE":
        description = "All datasets on this date."
    else: description = ""

    print("Creating collection: "+name)
    r = sess.post('%sapi/collections' % CLOWDER_URL,
                  headers={"Content-Type":"application/json"},
                  data='{"name":"%s", "description":"%s"}' % (name, description))
    if (r.status_code != 200):
        print("Problem creating collection  "+name+": [%d] - %s)" % (r.status_code, r.text))
        return None

    collID = r.json()['id']
    if type == "SENSOR":
        SENSOR_COLLECTION_IDS[name] = collID
    elif type == "DATE":
        DATE_COLLECTION_IDS[name] = collID

    return collID
def createDataset(name, sess, description=""):
    print("Creating dataset: "+name)
    r = sess.post('%sapi/datasets/createempty' % CLOWDER_URL,
                  headers={"Content-Type":"application/json"},
                  data='{"name":"%s", "description":"%s"}' % (name, description))
    if (r.status_code != 200):
        print('Problem creating dataset  '+name+': [%d] - %s)' % (r.status_code, r.text))
        return None
    return r.json()['id']
def addCollectionToSpace(spaceID, collID, sess):
    r = sess.post('%sapi/spaces/%s/addCollectionToSpace/%s' % (CLOWDER_URL, spaceID, collID))
    if (r.status_code != 200):
        print('Problem adding collection to space: [%d] - %s)' % (r.status_code, r.text))
        return None
    return r.status_code
def addDatasetToSpace(spaceID, datasetID, sess):
    r = sess.post('%sapi/spaces/%s/addDatasetToSpace/%s' % (CLOWDER_URL, spaceID, datasetID))
    if (r.status_code != 200):
        print('Problem adding dataset to space: [%d] - %s)' % (r.status_code, r.text))
        return None
    return r.status_code
def addDatasetToCollection(collectionID, dsID, sess):
    r = sess.post('%sapi/collections/%s/datasets/%s' % (CLOWDER_URL, collectionID, dsID))
    if (r.status_code != 200):
        print('Problem adding dataset to collection: [%d] - %s)' % (r.status_code, r.text))
        return None
    return r.status_code
def addSubCollectionToCollection(clowder_url, coll_id, sub_coll_id, username=None, password=None):
    sess = requests.Session()
    sess.auth = (user,passwd)
    url_path = '%sapi/collections/%s/addSubCollection/%s' % (clowder_url, coll_id, sub_coll_id)
    r = sess.post(url_path)

    if (r.status_code != 200):
        print('Problem adding subcollection  : [%d] - %s)' % (r.status_code, r.text))
        return None

def uploadFilesToDataset(datasetID, filename_list, sess):
    # Prepare the files to be sent
    files_to_send = []
    for filename in filename_list:
        if not os.path.isfile(filename):
            continue
        files_to_send.append(
            ("file",'{"path":"%s"}' % filename)
        )

    (content, header) = encode_multipart_formdata(files_to_send)
    print("...uploading files")    
    r = sess.post(CLOWDER_URL+"api/uploadToDataset/"+datasetID,
                  data=content,
                  headers={'Content-Type':header})

    # Return single ID if single file, or all IDs + filenames if multiple files
    resp_json = json.loads(r.text)
    if 'id' in resp_json:
        return resp_json['id']
    else:
        return resp_json['ids']
def addDatasetMetadata(datasetID, metadata, sess):
    print("...uploading metadata")
    r = sess.post('%sapi/datasets/%s/metadata' % (CLOWDER_URL, datasetID),
                  headers={"Content-Type":"application/json"},
                  data=json.dumps(metadata))
    if (r.status_code != 200):
        print('Problem attaching dataset metadata  : [%d] - %s)' % (r.status_code, r.text))
        print(json.dumps(metadata))
        return None
def parseTimestamp(ts):
    # e.g.
    #   '2016-02-13__04-12_37_486'
    # becomes
    #   '2016-02-13 04:12:37.486'
    YY = int(ts[:4])
    MM = int(ts[5:7])
    DD = int(ts[8:10])
    hh = int(ts[12:14])
    mm = int(ts[15:17])
    ss = int(ts[18:20])
    ms = int(ts[21:24])*1000
    return str(datetime.datetime(YY, MM, DD, hh, mm, ss, ms))[:-3]
def loadJsonFile(filename):
    f = open(filename)
    jsonObj = json.load(f)
    f.close()
    return jsonObj

# CONNECTION SETTINGS
CLOWDER_URL = "http://141.142.168.72/clowder/"
user = "mburnet2@illinois.edu"
passwd = ""

SENSOR_FOLDER = "/projects/arpae/terraref/raw_data/ua-mac/MovingSensor"
SENSOR_LIST = ["co2Sensor","cropCircle","flirIrCamera","ndviSensor","priSensor","ps2Top","scanner3DTop","stereoEast",
              "stereoTop","SWIR","VNIR"]

# Space where collections and datasets will be associated
SPACE_ID = "5707f8a9e4b07d8786aca46b"
# Map sensor name to Clowder collection ID
SENSOR_COLLECTION_IDS = {}
# Map YYYY-MM-DD date to Clowder collection ID
DATE_COLLECTION_IDS = {}
# Map YYYY-MM-DD__hh:mm:ss to Clowder dataset ID
TIMESTAMP_DATASET_IDS = {}

sess = requests.Session()
sess.auth = (user, passwd)

for sensor in SENSOR_LIST:
    

    sensorDir = os.path.join(SENSOR_FOLDER, sensor)
    for date in os.listdir(sensorDir):
        if date[:3] == '2016': # date = YYYY-MM-DD
            dateDir = os.path.join(sensorDir, date)
            for dataset in os.listdir(dateDir):
                if dataset[:3] == '2016': # dataset = YYYY-MM-DD__hh-mm_ss_mss
                    dsDir = os.path.join(dateDir, dataset)

                    # Find files and metadata in the directory
                    fileList = []
                    metadata = {}
                    for filename in os.listdir(dsDir):
                        if filename[0] != ".":
                            fpath = os.path.join(dsDir, filename)
                            if filename.find("metadata.json") > -1:
                                # send contents of metadata.json to Clowder under this dataset
                                metadata = clean_json_keys(loadJsonFile(fpath))
                            else:
                                fileList.append(fpath)

                    # Don't create a dataset for metadata only
                    if fileList != []:
                        # Create sensor collection & add to space
                        sensorColl = createCollectionIfNecessary("SENSOR", sensor, sess)
                        addCollectionToSpace(SPACE_ID, sensorColl, sess)

                        # Create date collection & add to space
                        dateColl = createCollectionIfNecessary("DATE", date, sess)
                        addCollectionToSpace(SPACE_ID, dateColl, sess)
            
                        # Create dataset & add to collections & space
                        timestamp = parseTimestamp(dataset)
                        dsName = sensor+" - "+timestamp
                        dsID = createDataset(dsName, sess)
                        
                        addDatasetToSpace(SPACE_ID, dsID, sess)
                        addDatasetToCollection(sensorColl, dsID, sess)
                        addDatasetToCollection(dateColl, dsID, sess)
                    
                        uploadFilesToDataset(dsID, fileList, sess)
                        if metadata != {}:
                            addDatasetMetadata(dsID, metadata, sess)

print("Completed.")
