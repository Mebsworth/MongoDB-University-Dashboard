import pymongo
import sys

#establish a connection to the database
connection = pymongo.Connection("mongodb://localhost", safe=True)

#get a handle to the track database
db=connection.track

file = open("data.js", "w")

# create new collection of number of completed homeworks per student
def num_completed_homeworks():

    db.overall_sample_set.aggregate([
        {"$unwind":"$events"},
        {"$match":{"events.correct":True, "events.vertical_type":"problem", "events.lesson_format":"homework"}},
        {"$group": {"_id": "$student_id", "num_assignments":{"$sum":1}}},
        {"$out":"homework_assignments"}
        ])

# find the average number of completed homeworks
def find_avg_completed_homeworks():

    average = db.homework_assignments.aggregate([
    {"$group": {"_id":"null", "avg_assignments":{"$avg":"$num_assignments"}}},
    {"$project": {"_id":0, "avg_assignments":1}}], cursor={})

    #loop will only run once
    for doc in average:
        print "Average number of homeworks completed by students:", doc['avg_assignments']
        file.write("[\n")
        file.write("[ 'Average', " + str(doc['avg_assignments']) + " ],\n")

# get number of homeworks completed for this student
def find_student_homeworks(sid):
    cursor = db.homework_assignments.find({"_id":sid})

    #loop will only run once
    for doc in cursor:
        print "Student", sid, "number of completed homeworks:", doc['num_assignments']
        file.write("[ 'Student ID: " + str(sid) +"', " + str(doc['num_assignments']) + " ]\n")
        file.write("]\n")

num_completed_homeworks()
find_avg_completed_homeworks()
find_student_homeworks(140361)
