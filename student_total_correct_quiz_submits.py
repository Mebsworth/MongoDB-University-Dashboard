import pymongo
import sys

#establish a connection to the database
connection = pymongo.Connection("mongodb://localhost", safe=True)

#get a handle to the track database
db=connection.track

#open data file to write to
file = open("data/student_total_correct_quiz_submits.js", "w")

#returns a list of distinct students in overall_sample_set
def find_all_students():
    students = db.overall_sample_set.distinct("student_id")
    return students #5395

#find the number of correct, completed quizzes by a student
def find_num_quiz_submits():

    db.overall_sample_set.aggregate([
        {"$unwind":"$events"},
        {"$match":{"events.correct":True, "events.event_type":"submit", "events.vertical_type":"problem", "events.lesson_format":"lesson"}},
        {"$group": {"_id": "$student_id", "num_quiz_submits":{"$sum":1}}},
        {"$out":"num_quiz_submits"}
        ])

def find_student_info(sid):
    cursor = db.num_quiz_submits.find({"_id":sid})

    file.write(str(sid) +":")
    num_submits = 0

    #loop will only run once
    for doc in cursor:
        num_submits = doc['num_quiz_submits']

    file.write(str(num_submits))
    file.write(",")
    file.write("\n")

def run(): 
    file.write("var student_total_correct_quiz_submits_data = {")

    all_students = find_all_students()

    find_num_quiz_submits()

    for student in all_students:
        find_student_info(student)

    file.write("};")

run()
