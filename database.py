from pymongo import MongoClient

client = MongoClient()

client = MongoClient('localhost', 27017)

database = client.database
collection = database.files_collection

#hash filename
juan = {"file": "juan", "tags": ["a", "c"]}
amelia = {"file": "amelia", "tags": ["b", "c"]}
amelia1 = {"file": "amelia1", "tags": ["b", "c", "d"]}
amelia2 = {"file": "amelia2", "tags": ["b", "c", "d"]}


collection.insert(juan)
collection.insert(amelia)
collection.insert(amelia1)
c = collection.insert(amelia2)

cursor = collection.find({'file': 'amelia2'})

for i in cursor:
    print('a')

print(c)
#
# tags = "b/c"
#
# r = collection.find({"tags": {"$all": tags.split('/')}})
#
#
# for c in r: print(c)
# #
# # if r is None:
# #     print(0)
# # else:
# #     print(1)
#
#


collection.remove({})

