################################################################################################
### Schedule-based transit shortest path algorithm ###
################################################################################################
''' Copyright (C) 2013 by Alireza Khani
Released under the GNU General Public License, version 2.
-------------------------------------------------------
Code primarily written by Alireza Khani
Contact: akhani@umn.edu
-------------------------------------------------------
This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 2 of the License, or
(at your option) any later version.
This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.
You should have received a copy of the GNU General Public License
along with this program. If not, see <http://www.gnu.org/licenses/>. '''
################################################################################################
################################################################################################
################################################################################################

import math, time, heapq
inputDataLocation = "test network (Tong and Richardson 1988)/"

################################################################################################
class Zone:
    def __init__(self, _tmpIn):
        self.lat = float(_tmpIn[1])
        self.long = float(_tmpIn[2])
        self.nodes = []
class Stop:
    def __init__(self, _tmpIn):
        self.lat = float(_tmpIn[3])
        self.long = float(_tmpIn[4])
        self.nodes = [] # This is a list of all the nodes of trips departing from it. This contains tripID, stopID, and stop sequence
class Trip:
    def __init__(self, _tmpIn):
        self.route = _tmpIn[1]
        self.type = _tmpIn[2]
        self.capacity = int(_tmpIn[3])
        self.nodes = []
        self.links = []
class Node:
    def __init__(self, _tmpIn):
        self.trip = _tmpIn[0] # TripID
        self.seq = int(_tmpIn[4]) # Stop sequence
        self.stop = _tmpIn[3] # StopID
        if _tmpIn[0]=="access" or _tmpIn[0] == "egress":
            self.meanTime = _tmpIn[2]
        else:
            # This is mean departure time by converting departure time into minutes, seconds, milli-seconds(may be?)
            self.meanTime = (int(_tmpIn[2])//10000)*60.0 + int(_tmpIn[2])%10000//100 + (int(_tmpIn[2])%100)/60.0

        self.last = 0 # If takes the value one indicate that the node is the last stop of the trip
        self.outLinks = []
        self.inLinks = []
        self.labels = (999999.0,999999.0) #time, cost
        self.preds = ("","")
class Link:
    def __init__(self, _from, _to, _by, _time):
        self.fromNode = _from
        self.toNode = _to
        self.trip = _by # This is trip Id
        self.time = _time # This is the time to traverse this link. In case of walking transfer links, a walking time will be assigned
        if _by<"999999999":
            self.capacity = 25
        else:
            self.capacity = 999999
        self.passengers = []
class Passenger:
    def __init__(self, _tmpIn):
        self.origin = _tmpIn[1]
        self.destination = _tmpIn[2]
        self.direction = _tmpIn[5]
        self.PDT = float(_tmpIn[6])
        self.path = []
################################################################################################
def readZones():
    # Zone file has "zoneId, Latitude, Longitude"
    inFile = open(inputDataLocation+"ft_input_zones.dat")
    tmpIn = inFile.readline().strip().split("\t")
    for x in inFile:
        tmpIn = x.strip().split("\t") # Converts this into list of items in the line
        zoneId = tmpIn[0]
        # Each zoneID in the zoneSet list is an instance of Zone class with information such as lat, long and nodes
        zoneSet[zoneId] = Zone(tmpIn) # Creating a list of zones. For now this will add only lat and long. Later the node list will be modified
    inFile.close()
    print len(zoneSet), "zones"
def readStops():
    inFile = open(inputDataLocation+"ft_input_stops.dat")
    tmpIn = inFile.readline().strip().split("\t")
    for x in inFile:
        tmpIn = x.strip().split("\t")
        # Each stopId in the stopSet list is an instance of Stop class with information such as lat, and long. The node list will be modified later
        stopSet[tmpIn[0]] = Stop(tmpIn) # Creating a list of stops
    inFile.close()
    print len(stopSet), "stops"
def readTrips():
    inFile = open(inputDataLocation+"ft_input_trips.dat")
    tmpIn = inFile.readline().strip().split("\t")
    for x in inFile:
        tmpIn = x.strip().split("\t")
        # Each tripId is an instance in the tripSet list for Trips class.
        tripSet[tmpIn[0]] = Trip(tmpIn) # This is the list of all the trips. The trip route, type, capacity is added now. The node and link lists will be edited later
    inFile.close()
    print len(tripSet), "trips"
def readSchedule():
    inFile = open(inputDataLocation+"ft_input_stopTimes.dat")
    tmpIn = inFile.readline().strip().split("\t")
    prevNodeId = "" # There is no previous node ID to the first node
    for x in inFile:
        tmpIn = x.strip().split("\t")
        tripId = tmpIn[0] # Trip ID
        stopId = tmpIn[3] # Stop ID of the stop included in that trip
        seq = tmpIn[4] # The stop sequence
        nodeId = tripId + "," + seq + "," + stopId # A node id is composed of tripID, sequence of the stop, and stopID
        # This will create a list of nodes with instances for Node class
        nodeSet[nodeId] = Node(tmpIn) # For now, this will add tripId, stopId, stop sequence, mean departure time to the class.
        #  The last, incoming and outgoing links of the nodes, labels and predecessors of the nodes will be added later.
        stopSet[stopId].nodes.append(nodeId) # Now you can add the nodeId in the node list of the Stop class created earlier.
        # The list will contain several nodeIds for a particular stopId. These modeIds refer to different routes and the sequence of that stop in that route.


        # The first trip has the first stop with stop sequence = 1, It will never go into these if conditions.
        if int(seq)==1 and prevNodeId!="": # if there is change in the tripId in the iterations and first tip is occured,
            # then previous node's self.last will take the value 1, which means that the last stop was the last stop of the trip.
            nodeSet[prevNodeId].last = 1
        if int(seq)>1: # If this is not the first stop of the trip, then it will create a linkID
            linkId = tripId+","+str(int(seq)-1) # A link ID is named by tripID, and the previous stop sequence
            # The a linkSet will be created having instances for all the links in the file.
            linkSet[linkId] = Link(prevNodeId, nodeId, tripId, 0) # A Link class need from_node, to_node, by trip_id, and time which is given 0 right now
            nodeSet[prevNodeId].outLinks.append(linkId) # The outlinks in the node class is modified now because this link is coming out of the previous node
            nodeSet[nodeId].inLinks.append(linkId) # This link is going into current node also
        prevNodeId = nodeId # Changing the previous node to current node (This will take care of the stop seq = 1 also)
    inFile.close()
    print len(nodeSet), "nodes"
    print len(linkSet), "transit links"
    # Create waiting transfer links. To create a transfer link the nodes should not be on the same trip, or route, also
    for s in stopSet: # Iterating over stopID
        for n1 in stopSet[s].nodes: # Iterating over all the nodes attached to the stopID as different trips are possible from that stop
            for n2 in stopSet[s].nodes:
                # The nodes should be equal. Why? cuz you cannot create a transfer link from a node to itself
                # The first node should not be a first stop of the trip. Why? cuz a transfer link cannot be created from a first node of the trip as you haven't started the journey yet
                # The seconnd node cannot be a last stop of the trip. Why? cuz this means you are transfering from a node to second node trip, which does not go further
                # Both nodes should not have the same route as it doesn;t make sense to transfer on the same route on a different trip
                if n1!=n2 and nodeSet[n1].seq!=1 and nodeSet[n2].last!=1 and nodeSet[n1].stop==nodeSet[n2].stop and tripSet[nodeSet[n1].trip].route!=tripSet[nodeSet[n2].trip].route:
                    # Also the time of departue at the second node should be greater than the time of departure on the first node plus the waiting time like 10s.
                    if nodeSet[n2].meanTime>=nodeSet[n1].meanTime and nodeSet[n2].meanTime<nodeSet[n1].meanTime+10:   ### the constant value determines an acceptable time window
                        # A transfer linkId is created using a string called transfer +  the index of the link in the linkSet
                        linkId = "transfer"+","+str(len(linkSet)+1)
                        linkSet[linkId] = Link(n1, n2, "waitingtransfer", 0) # here by is changed cuz it's a transfer link. Previously it was the tripId
                        nodeSet[n1].outLinks.append(linkId) # You can also attach the transfer link coming out of the node
                        nodeSet[n2].inLinks.append(linkId) # You can also attach the transfer link getting into the node
                        #print i, nodeSet[n1].stop, nodeSet[n1].trip, nodeSet[n2].trip
    print len(linkSet), "transit+waitingtransfer links" # This is the number of total links including the transfer links

def readTransferLinks():
    inFile = open(inputDataLocation+"ft_input_transfers.dat")
    tmpIn = inFile.readline().strip().split("\t")
    for x in inFile:
        tmpIn = x.strip().split("\t")
        #print len(tmpIn), len(linkSet)
        # This is creating the list of all the nodes which are associated with this stopID (from and to both)
        fromNodes = stopSet[tmpIn[0]].nodes
        toNodes = stopSet[tmpIn[1]].nodes
        # Create walking transfer links
        for n1 in fromNodes:
            for n2 in toNodes:
                # Same conditions as above. The walking transfer link should not be created from first stop of the first node, last stop of the second node, and they should not share the same route.
                if nodeSet[n1].seq!=1 and nodeSet[n2].last!=1 and tripSet[nodeSet[n1].trip].route!=tripSet[nodeSet[n2].trip].route:
                    # The transfer time should be atleat walking time and at most walking time and the walking time + wait time
                    if nodeSet[n2].meanTime>=nodeSet[n1].meanTime+float(tmpIn[3]) and nodeSet[n2].meanTime<nodeSet[n1].meanTime+float(tmpIn[3])+10:    ### the constant value determines an acceptable time window
                        linkId = "transfer"+","+str(len(linkSet)+1)
                        if linkId in linkSet:
                            print "ERROR" # This will throw an error cuz there is already tranfer link that exist in the linkset.
                            # This is will avoid multiple transfer and also confusion between waiting transfer and walking transfer
                        linkSet[linkId] = Link(n1, n2, "walkingtransfer", float(tmpIn[3]))
                        nodeSet[n1].outLinks.append(linkId)
                        nodeSet[n2].inLinks.append(linkId)
    inFile.close()
    print len(linkSet), "transit+waitingtransfer+walkingtransfer links"
def readAccessLinks():
    inFile = open(inputDataLocation+"ft_input_accessLinks.dat")
    tmpIn = inFile.readline().strip().split("\t")
    for x in inFile:
        tmpIn = x.strip().split("\t")
        zoneId = tmpIn[0]
        tmpNodes = stopSet[tmpIn[1]].nodes # All the nodes associated with the stopId
        nodeId = "access" + "," + tmpIn[0] # This will create an access node
        if not(nodeId in nodeSet):
            nodeSet[nodeId] = Node(["access", -1, -1, zoneId, 0]) # Here tripId is the "access node", arrival and departure times are -1 and stopId is the TAZ and the stop sequence is 0
        zoneSet[zoneId].nodes.append(nodeId)
        # Iterating over all the nodes associated with the stopId of the access link
        for n in tmpNodes:
            # If the node associated with that stop is the last stop, there is no point going further and create an access link.
            if nodeSet[n].last!=1:
                linkId = "access"+","+str(len(linkSet)+1)
                if linkId in linkSet:
                    print "ERROR" # This access link should not be present in the linkSet
                linkSet[linkId] = Link(nodeId, n, "access", float(tmpIn[3])) # Creating the an access link from that TAZ to all the nodes associated with that stopId.
                # Don't forget thar the nodes can differ by the tripId
                nodeSet[nodeId].outLinks.append(linkId)
                nodeSet[n].inLinks.append(linkId)
        # The same access link can become egress link in the opposite direction.
        nodeId = "egress" + "," + tmpIn[0]
        nodeSet[nodeId] = Node(["egress", -1, -1, zoneId, 0])
        zoneSet[zoneId].nodes.append(nodeId)
        for n in tmpNodes:
            if nodeSet[n].seq!=1:
                linkId = "egress"+","+str(len(linkSet)+1)
                if linkId in linkSet:
                    print "ERROR"
                linkSet[linkId] = Link(n, nodeId, "egress", float(tmpIn[3])) # The link is created from node to the TAZ
                nodeSet[n].outLinks.append(linkId)
                nodeSet[nodeId].inLinks.append(linkId)
    inFile.close()
    print len(linkSet), "transit+waitingtransfer+walkingtransfer+access+egress links"
def readDemand():
    inFile = open(inputDataLocation+"ft_input_demand.dat")
    tmpIn = inFile.readline().strip().split("\t")
    for x in inFile:
        tmpIn = x.strip().split("\t")
        passengerId = tmpIn[0]
        # The passengerSet list will contain all the passengerId instances of Passenger class
        passengerSet[passengerId] = Passenger(tmpIn) # This will create a passenger in the class with origin, dest, direction, and PDT.
        #  The path of the passenger will be added later
    inFile.close()
    print len(passengerSet), "passengers"
################################################################################################
def sortConnectors(): # This is important to sort all the links according to their departure timt
    for node in nodeSet:
        if len(nodeSet[node].inLinks)>1:

            # This will sort all the link associated with the node by mean time of the departure from upnode + time to traverse the link (e.g. walking link)
            nodeSet[node].inLinks.sort(key=lambda x: nodeSet[linkSet[x].fromNode].meanTime+linkSet[x].time)

################################################################################################
def findShortestPath(orig, PDT, pathType):
    for n in nodeSet:
        nodeSet[n].labels = (999999, 999999, 1.0) # This is time, cost and don't know the third value
        nodeSet[n].pred = ("", "") # This is current node and link
    if zoneSet[orig].nodes==[]: # If there is no nodes to go from the TAZ, then return -1
        return -1
    accessNode = zoneSet[orig].nodes[0] # This is the access link
    nodeSet[accessNode].labels = (PDT,0,1) # We assign the label equal to the PDT and cost = 0
    #SE = [accessNode]
    SE = [((nodeSet[accessNode].labels[2], accessNode))]
    it=0
    iLabel = 0
    while len(SE)>0:
        #currentNode = SE[0]
        #currentLabels = nodeSet[currentNode].labels
        #SE.remove(currentNode)
        currentNode = heapq.heappop(SE)[1]
        currentLabels = nodeSet[currentNode].labels
        it = it+1
        for link in nodeSet[currentNode].outLinks:
            newNode = linkSet[link].toNode
            newPreds = [currentNode, link]
            existingLabels = nodeSet[newNode].labels
            newLabels = []
            ### Calculate new labels
            if linkSet[link].trip=="access":
                if PDT+linkSet[link].time<=nodeSet[newNode].meanTime and PDT+linkSet[link].time+30>nodeSet[newNode].meanTime:
                    newLabels.append(round(nodeSet[newNode].meanTime,3))
                    newLabels.append(round(weights[2]*linkSet[link].time+weights[1]*(nodeSet[newNode].meanTime-linkSet[link].time-PDT),3))
                else:
                    continue
            # Weights are in this order
            # IVT, WT, WK, TR
            elif linkSet[link].trip=="egress":
                newLabels.append(round(currentLabels[0]+linkSet[link].time,3))
                newLabels.append(round(currentLabels[1]+weights[2]*linkSet[link].time,3))
            elif linkSet[link].trip=="waitingtransfer":
                newLabels.append(round(nodeSet[newNode].meanTime,3))
                newLabels.append(round(currentLabels[1]+weights[3]+weights[1]*(nodeSet[newNode].meanTime-nodeSet[currentNode].meanTime),3))
            elif linkSet[link].trip=="walkingtransfer":
                newLabels.append(round(nodeSet[newNode].meanTime,3))
                newLabels.append(round(currentLabels[1]+weights[3]+weights[2]*linkSet[link].time+weights[1]*(nodeSet[newNode].meanTime-nodeSet[currentNode].meanTime-linkSet[link].time),3))
            else:
                newLabels.append(round(nodeSet[newNode].meanTime,3))
                newLabels.append(round(currentLabels[1]+weights[0]*(nodeSet[newNode].meanTime-nodeSet[currentNode].meanTime),3))
            ### Update the node labels
            if pathType=="fastest" and newLabels[0]<existingLabels[0]:
                nodeSet[newNode].labels = newLabels
                nodeSet[newNode].preds = newPreds
                #SE.append(newNode)
                heapq.heappush(SE, (newLabels[0], newNode))
            elif pathType=="optimal" and newLabels[1]<existingLabels[1]:
                nodeSet[newNode].labels = newLabels
                nodeSet[newNode].preds = newPreds
                #SE.append(newNode)
                heapq.heappush(SE, (newLabels[1], newNode))
            elif pathType=="reliable":
                if newLabels[2]>existingLabels[2] or (newLabels[2]==existingLabels[2] and newLabels[1]<existingLabels[1]):
                    nodeSet[newNode].labels = newLabels
                    nodeSet[newNode].preds = newPreds
                    #SE.append(newNode)
                    heapq.heappush(SE, (newLabels[2], newNode))
    return [it, iLabel]
def getShortetstPath(dest):
    currentNode = zoneSet[dest].nodes[1]
    if nodeSet[currentNode].labels[1]>=999999:
        return []
    path = []
    while currentNode!="":
        newNode = nodeSet[currentNode].preds[0]
        newLink = nodeSet[currentNode].preds[1]
        if newNode!="":
            path = [newLink] + path
        currentNode = newNode
    return path
################################################################################################
def printLinkFlows():
    outFile = open("linkFlows.dat", "w")
    tmpOut = "route\ttrip\tsequence\tfrom\tto\tflow"
    outFile.write(tmpOut+"\n")
    for link in linkSet:
        if len(linkSet[link].passengers)>0:
            if link.split(",")[0]<"999999999":
                tmpOut = tripSet[linkSet[link].trip].route+"\t"+linkSet[link].trip+"\t"+link.split(",")[1]+"\t"+nodeSet[linkSet[link].fromNode].stop+"\t"+nodeSet[linkSet[link].toNode].stop+"\t"+str(len(linkSet[link].passengers))
            elif link.split(",")[0]=="transfer":
                tmpOut = link.split(",")[0]+"\t"+tripSet[nodeSet[linkSet[link].fromNode].trip].route+"\t"+tripSet[nodeSet[linkSet[link].toNode].trip].route+"\t"+nodeSet[linkSet[link].fromNode].stop+"\t"+nodeSet[linkSet[link].toNode].stop+"\t"+str(len(linkSet[link].passengers))
            else:
                tmpOut = link.split(",")[0]+"\t"+linkSet[link].fromNode[0]+"\t"+linkSet[link].toNode[0]+"\t"+nodeSet[linkSet[link].fromNode].stop+"\t"+nodeSet[linkSet[link].toNode].stop+"\t"+str(len(linkSet[link].passengers))
            outFile.write(tmpOut+"\n")
    outFile.close()
def printPaths():
    outFile = open("paths.dat", "w")
    tmpOut = "passenger\torigin\tdestination\tPDT\tpath"
    outFile.write(tmpOut+"\n")
    for passenger in passengerSet:
        orig = passengerSet[passenger].origin
        dest = passengerSet[passenger].destination
        PDT = passengerSet[passenger].PDT
        path = passengerSet[passenger].path
        tmpOut = passenger+"\t"+orig+"\t"+dest+"\t"+str(PDT)+"\t"+str(path)
        outFile.write(tmpOut+"\n")
    outFile.close()        
################################################################################################
def assignPassengers(_pathType):
    print
    counter = 0
    assigned = 0
    startTime = time.clock()
    for passenger in passengerSet:
        orig = passengerSet[passenger].origin
        dest = passengerSet[passenger].destination
        PDT = passengerSet[passenger].PDT
        iter = findShortestPath(orig, PDT, _pathType)[0]
        if iter==-1:
            print "No Access"
        path = getShortetstPath(dest)
        if path==[]:
            print "No Path"
        else:
            assigned = counter + 1
        counter = counter + 1
        if counter%100==0:
            print counter,"\tout of\t",len(passengerSet), "\tpassengers;\t", iter, "\titerations\t", round(time.clock()-startTime,3), "\tsecond"
            #print passenger, orig, dest, PDT, "path:", path
        passengerSet[passenger].path = path
        for link in path:
            linkSet[link].passengers.append(passenger)
    print assigned, "passengers assigned in", round(time.clock()-startTime,3), "seconds."
    printLinkFlows()
    printPaths()
################################################################################################
zoneSet = {}
stopSet = {}
tripSet = {}
nodeSet = {}
linkSet = {}
passengerSet = {}
weights = [1.0, 1.0, 1.0, 0.0] #IVT, WT, WK, TR

readZones()
readStops()
readTrips()
readSchedule()
readTransferLinks()
readAccessLinks()
readDemand()
sortConnectors()
assignPassengers("optimal")

