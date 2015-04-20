import pymongo
import sys

#establish a connection to the database
connection = pymongo.Connection("mongodb://localhost", safe=True)

#get a handle to the track database
db=connection.track

file = open("data2.js", "a")

# create new collection of number of completed assignments per student
def num_completed_assignments():

    db.overall_sample_set.aggregate([
        {"$unwind":"$events"},
        {"$match":{"events.correct":True, "events.vertical_type":"problem"}},
        {"$group": {"_id": "$student_id", "num_assignments":{"$sum":1}}},
        {"$out":"number_assignments"}
        ])

# find the average number of completed assignments
def find_avg_completed_assignments():

    average = db.number_assignments.aggregate([
    {"$group": {"_id":"null", "avg_assignments":{"$avg":"$num_assignments"}}},
    {"$project": {"_id":0, "avg_assignments":1}}], cursor={})

    #loop will only run once
    for doc in average:
        print "Average number of assignments completed by students:", doc['avg_assignments']
        file.write("[\n")
        file.write("[ 'Average # assignments', " + str(doc['avg_assignments']) + " ],\n")


# get number of assignments completed for this student
def find_students_assignments(sid):
    cursor = db.number_assignments.find({"_id":sid})

    #loop will only run once
    for doc in cursor:
        print "Student", sid, "number of completed assignments:", doc['num_assignments']
        file.write("[ 'Student ID: " + str(sid) +"', " + str(doc['num_assignments']) + " ]\n")
        file.write("]")

num_completed_assignments()
find_avg_completed_assignments()
find_students_assignments(140361)
