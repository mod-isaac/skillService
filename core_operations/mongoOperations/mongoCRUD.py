import pymongo
from pymongo import MongoClient
import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dataConnectionManager.connectionManager import dbCon

MongoInfoList = dbCon.mongodbConnectionInfo()

client      = MongoInfoList[0]
pool        = MongoInfoList[1]
collection  = MongoInfoList[2]

db   = client[pool]
col  = db[collection]

def uniqueBulckPost(dataList):
    from progressbar import ProgressBar
    pbar = ProgressBar()
    print("START INSERTING TO MONGO")
    try:
        for key, value in pbar(dict(dataList).items()):
            col.insert_many([{'name': key.lower(), 'freq': value, 'occurrences':'null','cluster':'null', 'events':-1}]).inserted_ids
    except Exception as e:
        print (e)

def findOccurrences(dataDict,name):
    import itertools
    import sys
    dictlist = []
    jsonlist = []
    insertBulck = False
    for key, val in dataDict.items():
        if len(dataDict) == 1 or not dataDict:
            col.update({'name':name}, {'$set':{"occurrences":"NONE"}})
        else:
            for key, val in dataDict.items():
                    temp = ['item',key,'occur',val]
                    dictlist.append(temp)
                    insertBulck = True
    listSize = round(((sys.getsizeof(dictlist) / 1048576) / 10)) + 10
    chunks = [dictlist[x:x+listSize] for x in range(0, len(dictlist), listSize)]
    #To be updated
    chunks = chunks[:5000]
    from progressbar import ProgressBar
    pbar = ProgressBar()
    if insertBulck:
        print('\n=======================\nINSERTING ',name,'\n=======================')
        for subCunck in pbar(chunks):
            for block in subCunck:
                jsonlist.append(dict(itertools.zip_longest(*[iter(block)] * 2, fillvalue="")))
            try:
                col.update({'name':name}, {'$set':{"occurrences":jsonlist}})
            except Exception as e:
                print(e)
###########################################################
def findCluster(name):
    import itertools
    import sys
    dictlist = []
    jsonlist = []
    insertBulck = False
    dataDict = getSkillHierarchy(name, 100)
    if len(dataDict) == 0:
        col.update({'name':name}, {'$set':{"cluster":"NONE"}})
        return
    for key, val in dataDict.items():
        if len(dataDict) == 1 or not dataDict:
            col.update({'name':name}, {'$set':{"cluster":"NONE"}})
        else:
            for key, val in dataDict.items():
                    temp = ['item',key,'occur',val]
                    dictlist.append(temp)
                    insertBulck = True
    listSize = round(((sys.getsizeof(dictlist) / 1048576) / 10)) + 10
    chunks = [dictlist[x:x+listSize] for x in range(0, len(dictlist), listSize)]
    chunks = chunks[:5000]
    from progressbar import ProgressBar
    pbar = ProgressBar()
    if insertBulck:
        print('\n=======================\nINSERTING ',name,' CLUSTER\n=======================')
        for subCunck in pbar(chunks):
            for block in subCunck:
                jsonlist.append(dict(itertools.zip_longest(*[iter(block)] * 2, fillvalue="")))
            try:
                col.update({'name':name}, {'$set':{"cluster":jsonlist}})
            except Exception as e:
                print(e)
###########################################################
def updateOccurrences(name,dataList):
    from collections import Counter
    from itertools import chain
    newList = []
    for value in dataList:
        if len(value) > 1 and name in value:
            value.remove(name)
            newList.append(value)
    newList = list(chain.from_iterable(newList))
    newList = dict(Counter(newList))
    findOccurrences(dict(newList),name)

def getUpdateChunck(limit,dataList,field):
    names_list = list(col.find({field:"null"},{"name":True, "_id":False}).limit(limit))
    for item in names_list:
        for key,value in item.items():
            if field == "occurrences":
                updateOccurrences(value,dataList)
            if field == "cluster":
                print(value)
                findCluster(value)

def mongoTopFrequencyTerms(limit):
    topFreqList = []
    dataDict = list (col.find({},{"name":1, "_id":0}).limit(limit).sort("freq", pymongo.DESCENDING))
    for item in dataDict:
        for key,value in item.items():
            topFreqList.append(value)
    return topFreqList

def mongoTopOccurrences(skill,limit="DEAFULT",execlude="DEAFULT",removeOne=False):
    import itertools
    import operator
    allOccur = list (col.aggregate([{"$match": {"name":skill}}, {"$unwind": "$occurrences"}, {"$sort": {"occurrences.occur": -1}}, {"$group": {"_id": "$_id", "occurrences": {"$push": "$occurrences"}}}]))
    allOccur = allOccur[0]
    del allOccur['_id']
    occurrencesList = allOccur['occurrences']
    emptySymbols = ['NONE', 'null']
    if len(allOccur) == 0 or occurrencesList[0] in emptySymbols:
        return 0
    try:
        occurrencesList = [dict(t) for t in set([tuple(d.items()) for d in occurrencesList])]
    except Exception as e:
        print ('>>>>>>>>>>> ', occurrencesList,' >>>>>>>>>>>', e)
        pass
    tempOccurrencesList = []

    for i in occurrencesList:
        for key, val in i.items():
            tempOccurrencesList.append(val)
    orderedDict         = dict(itertools.zip_longest(*[iter(tempOccurrencesList)] * 2, fillvalue=""))
    sortedDict          = sorted(orderedDict.items(), key=operator.itemgetter(1))
    sortedDict          = sortedDict[::-1]

    if removeOne == True:
        tempDataDict = { k:v for k,v in dict(sortedDict).items() if v!=1 }
        tempSortedDict = []
        for key, val in tempDataDict.items():
            tempTuple = (key,val)
            tempSortedDict.append(tempTuple)
        sortedDict = tempSortedDict


    if limit == "DEAFULT":
        dataDict = (dict(sortedDict))
    else:
        dataDict = (dict(sortedDict[:limit]))

    if execlude == "DEAFULT":
        return dataDict
    else:
        skillsToExeclude    = mongoTopFrequencyTerms(execlude)
        for execItem in skillsToExeclude:
            try:
                del dataDict [execItem]
            except Exception as e:
                pass
    return dataDict

def getCleanSkillsNames(rowList):
    cleanNamesList = []
    for nameEntity in rowList:
        cleanNamesList.append(nameEntity['name'])
    return cleanNamesList

def checkIfSharedSkill(prmSkill,opSkill):
    operationalSkillsList = mongoTopOccurrences(opSkill)
    if operationalSkillsList == 0:
        return False
    existence = False
    for key in operationalSkillsList.keys():
        if key == prmSkill:
            existence = True
    return existence

def addEventsFrequency():
    ## To be continued
    from progressbar import ProgressBar
    pbar = ProgressBar()
    count = col.count()
    primaryRowSkills        =   getCleanSkillsNames(list (col.find({"events":-1}, {"name":1, "_id":0}).limit(10)))
    operationalRowSkills    =   getCleanSkillsNames(list (col.find({}, {"name":1, "_id":0})))[:10000]
    c = 0
    for i in pbar(operationalRowSkills):
        if i != "english" and checkIfSharedSkill("english",i) == True:
            c = c +1
    print (c)

def getHierarchyLevel(skill,limit):
    importingLimit = 100
    execludedLimit = 10
    removeOnes     = True
    if mongoTopOccurrences(skill,importingLimit,execludedLimit,removeOnes) == 0:
        return 0
    firstLevel     = list (mongoTopOccurrences(skill,importingLimit,execludedLimit,removeOnes).keys())
    firstLevel     = firstLevel[:limit]
    return firstLevel

def getSkillHierarchy(skill,limit):
    from collections import Counter
    skillsClusterSet = []
    intialLevel = getHierarchyLevel(skill,limit)
    if intialLevel == 0:
        return skillsClusterSet
    for innerSkill in intialLevel:
        skillsClusterSet.append(getHierarchyLevel(innerSkill,limit))
    cluster = [item for sublist in skillsClusterSet for item in sublist]
    cluster = dict(Counter (cluster))
    return cluster

def insertingSkillsCluster():
    pass
