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
            col.insert_many([{'name': key.lower(), 'freq': value, 'occurrences':'null'}]).inserted_ids
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


test_terms = ["english", "arabic", "urdu", "hindi", "الانجليزية", "french", "العربية", "malayalam"]
def getUpdateChunck(limit,dataList):
    #db = pymongo.MongoClient().baytSkills
    #names_list = list(db.new_skills.find({"occurrences":"null"},{"name":True, "_id":False}).limit(limit).sort("freq", pymongo.DESCENDING))
    names_list = list(col.find({"occurrences":"null"},{"name":True, "_id":False}).limit(limit))
    for item in names_list:
        for key,value in item.items():
            if value not in test_terms:
                updateOccurrences(value,dataList)

def mongoTopFrequencyTerms(limit):
    topFreqList = []
    dataDict = list (col.find({},{"name":1, "_id":0}).limit(limit).sort("freq", pymongo.DESCENDING))
    for item in dataDict:
        for key,value in item.items():
            topFreqList.append(value)
    #col.find({"occurrences":"null"},{"name":1, "_id":0}).limit(limit).sort({"freq":-1})

    return topFreqList
