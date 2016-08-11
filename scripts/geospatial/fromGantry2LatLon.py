import utm



def fromGantry2LatLon(Gpts):	
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
	ccR = []
	for pt in Gpts:
		#be careful
		gantryUTMx = SEutm[0] - pt[1]
		gantryUTMy = SEutm[1] + (pt[0] - 3.8)
		ccR += [[gantryUTMx,gantryUTMy]]
	return ccR