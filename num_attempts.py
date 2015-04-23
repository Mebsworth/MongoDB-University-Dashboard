from pymongo import MongoClient
import sys

#establish a connection to the database
#connection = pymongo.Connection("mongodb://localhost", safe=True)
connection = MongoClient()

#get a handle to the track database
db=connection.track

file = open("data2.js", "a")

# create new collection of number of homeworks per student
def num_completed_homeworks():

    db.overall_sample_set.aggregate([
        {"$unwind":"$events"},
        {"$match":{"events.correct":True, "events.vertical_type":"problem", "events.lesson_format":"homework"}},
        {"$group": {"_id": "$student_id", "num_homeworks":{"$sum":1}}},
        {"$out":"homework_assignments"}
        ])

# find the number of homeworks completed correctly on the first attempt
def first_attempt_homework():
    num_completed_homeworks()
    db.overall_sample_set.aggregate([
        {"$unwind":"$events"},
        {"$match":{"events.correct":True, "events.vertical_type":"problem", "events.lesson_format":"homework",
         "events.attempt":1}},
        {"$group": {"_id": "$student_id", "num_homeworks":{"$sum":1}}},
        {"$out":"first_homework"}
        ])

def first_attempt_student(sid):
    first_attempt_homework()
    cursor = db.first_homework.find({"_id":sid})

    #loop will only run once
    for doc in cursor:
        print "First attempt:", doc['num_homeworks']
        # file.write("[\n")
        # file.write("[ 'Average # assignments', " + str(doc['avg_assignments']) + " ],\n")


# get number of homeworks completed for this student
def find_student_homeworks(sid):
    cursor = db.homework_assignments.find({"_id":sid})

    #loop will only run once
    for doc in cursor:
        print "Student", sid, "number of completed homeworks:", doc['num_homeworks']
        # file.write("[ 'Student ID: " + str(sid) +"', " + str(doc['num_homeworks']) + " ]\n")
        # file.write("]\n")

first_attempt_student(188930)
find_student_homeworks(188930)
