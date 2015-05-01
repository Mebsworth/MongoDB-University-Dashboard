from pymongo import MongoClient
import sys

#establish a connection to the database
connection = MongoClient()

#get a handle to the track database
db=connection.track

file = open("data2.js", "a")

# create new collection of number of homeworks per student
def group_assignments():

    db.overall_sample_set.aggregate([
        {"$unwind":"$events"},
        {"$match":{"events.correct":True, "events.vertical_type":"problem", "events.lesson_format":"homework"}},
        {"$group": {"_id": "$student_id", "num_homeworks":{"$sum":1}}},
        {"$out":"group_assignments"}
        ])


def group_counter():
    group_assignments()
    counter = {'0-4': 0, '5-9':0, '10-14':0, '15-22':0}

    cursor = db.group_assignments.find()

    for doc in cursor:
        if 0 <= doc['num_homeworks'] < 5 :
            counter['0-4'] = counter.get('0-4') + 1
        elif 5 <= doc['num_homeworks'] < 10 :
            counter['5-9'] = counter.get('5-9') + 1
        elif 10 <= doc['num_homeworks'] < 15 :
            counter['10-14'] = counter.get('10-14') + 1
        else :
            counter['15-22'] = counter.get('15-22') + 1

    print counter

group_counter()

