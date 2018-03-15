import pymongo

def uniqueBulckPost(dataList,dbName,col):
    db = pymongo.MongoClient().baytSkills
    try:
        for key, value in dict(dataList).items():
            db.new_skills.insert_many([{'name': key.lower(), 'freq': value}]).inserted_ids
            print(key.lower(),"    Insertrd")
    except Exception as e:
        print (e)

def findOccurrences(dataDict,name):
    print(name)
    import itertools
    import sys
    dictlist = []
    jsonlist = []
    insertBulck = False
    db = pymongo.MongoClient().baytSkills
    for key, val in dataDict.items():
        if len(dataDict) == 1 or not dataDict:
            db.new_skills.update({'name':name}, {'$set':{"occurrences":"NONE"}})
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
                db.new_skills.update({'name':name}, {'$set':{"occurrences":jsonlist}})
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
    db = pymongo.MongoClient().baytSkills
    #names_list = list(db.new_skills.find({"occurrences":"null"},{"name":True, "_id":False}).limit(limit).sort("freq", pymongo.DESCENDING))
    names_list = list(db.new_skills.find({"occurrences":"null"},{"name":True, "_id":False}).limit(limit))
    for item in names_list:
        for key,value in item.items():
            if value not in test_terms:
                updateOccurrences(value,dataList)
