import requests, json, sys


apipath = "http://0.0.0.0:5455/files"
filepath = sys.argv[0] # e.g. "/root/missing_files/EnvironmentLogger_bundle.list"
sess = requests.Session()

lastRead = ""

startSend = True if lastRead == "" else False
def submit_batch(batchlist):
    print("submitting %s files to gantry API" % len(batchlist))
    print("last file in batch: %s" % batchlist[-1])
    postObj = {
        "paths": batchlist
    }

    sess.post(apipath,
              data=json.dumps(postObj),
              headers={'Content-Type': 'application/json'})

maxbatchsize = 1000
currbatch = []
with open(filepath, 'r+') as f:
    for line in f:
        l = line.rstrip()
        if l == lastRead and not startSend:
            print("found resume line; resuming queue")
            startSend = True
        elif l != "" and startSend:
            l = l.replace("Lemnatc", "LemnaTec")
            if l.startswith("/LemnaTec"):
                l = "/gantry_data" + l
            currbatch.append(l)
            if len(currbatch) >= maxbatchsize:
                submit_batch(currbatch)
                currbatch = []

if len(currbatch) > 0:
    submit_batch(currbatch)

print("done")
