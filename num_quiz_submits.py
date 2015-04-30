import pymongo
import sys

#establish a connection to the database
connection = pymongo.Connection("mongodb://localhost", safe=True)

#get a handle to the track database
db=connection.track

#find the number of correct, completed quizzes by a student
def find_num_quiz_submits():

    db.overall_sample_set.aggregate([
        {"$unwind":"$events"},
        {"$match":{"events.event_type":"submit", "events.vertical_type":"problem", "events.lesson_format":"lesson"}},
        {"$group": {"_id": "$student_id", "num_quiz_submits":{"$sum":1}}},
        {"$out":"num_quiz_submits"}
        ])

def find_student_quiz_submits(sid):
    cursor = db.num_quiz_submits.find({"_id":sid})

    #loop will only run once
    for doc in cursor:
        print "Student", sid, "number of quiz submits:", doc['num_quiz_submits']
        #file.write("[ 'Student ID: " + str(sid) +"', " + str(doc['num_quiz_submits']) + " ]\n")
        #file.write("]\n")

find_num_quiz_submits()

# none found: 174552, 187718, 178521, 190945, 117401, 11469, 48974, 138833, 187761, 165189, 183869, 
ids = [85152, 140361, 185097, 188930]
for id in ids:
    find_student_quiz_submits(id)

