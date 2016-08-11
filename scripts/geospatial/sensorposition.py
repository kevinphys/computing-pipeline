#!/usr/bin/env python
import logging
from config import *
import pyclowder.extractors as extractors
import utm
import json
import requests



def main():
	global extractorName, messageType, rabbitmqExchange, rabbitmqURL    
	#set logging
	logging.basicConfig(format='%(levelname)-7s : %(name)s -  %(message)s', level=logging.WARN)
	logging.getLogger('pyclowder.extractors').setLevel(logging.INFO)
	#connect to rabbitmq
	extractors.connect_message_bus(extractorName=extractorName, messageType=messageType, 
		processFileFunction=process_file, rabbitmqExchange=rabbitmqExchange, rabbitmqURL=rabbitmqURL)



# Process the file and upload the results
def process_file(parameters):
	###########################################
	# get metadata from parameters            #
	# based on Max's work  				      #
	###########################################
	'''for key,value in parameters.items():
		print key
		print value
		print 'cc cc cc\n'''
	data = parameters['metadata']
	###########################################
	###########################################
	#
	#
	#
	'''with open(filename, 'r') as f:
		data = json.load(f)'''
	#
	#
	#
	#check1
	if not data.has_key('lemnatec_measurement_metadata'):
		return
	tmp = data['lemnatec_measurement_metadata']
	if not tmp.has_key('gantry_system_variable_metadata'):
		return
	if not tmp.has_key('sensor_fixed_metadata'):
		return
	#get json information
	gantryInfo = data['lemnatec_measurement_metadata']['gantry_system_variable_metadata']
	sensorInfo = data['lemnatec_measurement_metadata']['sensor_fixed_metadata']
	#check2
	if not gantryInfo.has_key('Position x [m]'):
		return
	if not gantryInfo.has_key('Position y [m]'):
		return
	if not sensorInfo.has_key('location in camera box X [m]'):
		return
	if not sensorInfo.has_key('location in camera box Y [m]'):
		return
	if not sensorInfo.has_key('field of view X [m]'):
		return
	if not sensorInfo.has_key('field of view Y [m]'):
		return
	#get position of gantry
	gantryX = float(gantryInfo['Position x [m]'].encode("utf-8"))
	gantryY = float(gantryInfo['Position y [m]'].encode("utf-8"))
	#
	time = gantryInfo['Time'].encode("utf-8")
	#
	#get position of sensor
	sensorX = float(sensorInfo['location in camera box X [m]'].encode("utf-8"))
	sensorY = float(sensorInfo['location in camera box Y [m]'].encode("utf-8"))
	#get field of view (assuming field of view X and field of view Y are based on the center of the sensor)
	fovX = float(sensorInfo['field of view X [m]'].encode("utf-8"))
	fovY = float(sensorInfo['field of view Y [m]'].encode("utf-8"))
	#
	#
	#
	#############################################
	#SE Corner. 33d 04.470m N / -111d 58.485m W #
	#SW Corner. 33d 04.474m N / -111d 58.505m W #
	#NW Corner. 33d 04.592m N / -111d 58.505m W #
	#NE Corner. 33d 04.591m N / -111d 58.487m W #
	#############################################
	'''Yaping: What is the exact point on gantry that have the position recorded in the metadata?
	David: Not sure. I assume the SE corner of the sensor box and then the sensor position is relative to this
	rjstrand: With regard to the reported position, David is correct. The reported position represents the 
	 location of the SE corner of the camera box. Instrument positions are then based on offsets. Tino can speak to those.'''
	#the coordinate for postion of gantry is with origin near SE (3.8, 0.0, 0.0), 
	#positive x diretion is to North and positive y direction is to West
	#			N(x)
	#			^
	#			|
	#			|
	#			|
	#W(y)<------SE
	#########################
	#SE (3.8,	0.0,	0.0)#
	#NW (207.3,	22.135,	5.5)#
	#########################
	SElon = -111.97475
	SElat = 33.0745
	#UTM coordinates
	SEutm = utm.from_latlon(SElat, SElon)
	#be careful
	gantryUTMx = SEutm[0] - gantryY
	gantryUTMy = SEutm[1] + (gantryX - 3.8)
	#
	#
	#
	#according to the above addressment
	#			N(x)
	#			^
	#			|
	#			|
	#			|
	#W(y)<------SE
	#be careful
	sensorUTMx = gantryUTMx - sensorY
	sensorUTMy = gantryUTMy + sensorX
	#get lat and lon of sensor
	sensorLatLon = utm.to_latlon(sensorUTMx,sensorUTMy,SEutm[2],SEutm[3])
	sensorLat = sensorLatLon[0]
	sensorLon = sensorLatLon[1]
	print sensorLat
	print sensorLon
	#get NW and SE points of field of view bounding box
	#be careful
	#NW
	fovNWptUTMx = sensorUTMx - fovY/2
	fovNWptUTMy = sensorUTMy + fovX/2
	#SE
	fovSEptUTMx = sensorUTMx + fovY/2
	fovSEptUTMy = sensorUTMy - fovX/2
	#
	fovNWptLatLon = utm.to_latlon(fovNWptUTMx,fovNWptUTMy,SEutm[2],SEutm[3])
	fovNWptLat = fovNWptLatLon[0]
	fovNWptLon = fovNWptLatLon[1]
	fovSEptLatLon = utm.to_latlon(fovSEptUTMx,fovSEptUTMy,SEutm[2],SEutm[3])
	fovSEptLat = fovSEptLatLon[0]
	fovSEptLon = fovSEptLatLon[1]
	print fovNWptLat
	print fovNWptLon
	print fovSEptLat
	print fovSEptLon
	#
	#
	#
	#################################################
	# save position of sensor into postgis database #
	# based on Rob's work				            #
	#################################################
	#the folowing parameters contains useful information
	'''secretKey'
	'host'
	'datasetId'
	'filelist'''
	#
	key = parameters['secretKey']
	#host = parameters['host']
	host = "https://terraref.ncsa.illinois.edu/clowder-dev/"
	#
	#
	'''properties = {"datasetId": parameters['datasetId']}
	filelist = parameters['filelist']'''
	'''for item in filelist:
		print item['filename'] + '\t\t\t' + item['id']
		print 'cc cc cc\n'''
	#
	'''fileIDs = ""
	for item in filelist:
		fileIDs += item['id'] + ","
	fileIDs = fileIDs[:-1]#remove last ','
	properties["fileIds"] = fileIDs'''
	properties = {"sources": host+"datasets/"+parameters['datasetId']}
	fovBB = {"coordinates": [[fovNWptLon, fovNWptLat, 0],[fovSEptLon, fovSEptLat, 0]]}
	properties["fov"] =  fovBB
	#
	#
	geometry = {"coordinates": [sensorLon,sensorLat,0]}
	streamID = "1"
	body = {"start_time": time, "end_time": time, "type": "Feature", "geometry": geometry, "properties": properties, "stream_id": streamID}
	headers = {'Content-type': 'application/json','Authorization': 'Basic Y2FpMjVAaWxsaW5vaXMuZWR1OmNsb3dkZXI3Nw=='}
	r = requests.post('%sapi/geostreams/datapoints?key=%s' % (host, key), data=json.dumps(body), headers=headers)
	if r.status_code != 200:
		print("ERR  : Could not add datapoint to stream : [" + str(r.status_code) + "] - " + r.text)
	else:
		print "cc Success!"
	return
	#################################################
	#################################################

if __name__ == "__main__":
	main()
