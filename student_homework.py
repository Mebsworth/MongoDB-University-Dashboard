import pymongo
import sys

#establish a connection to the database
connection = pymongo.Connection("mongodb://localhost", safe=True)

#get a handle to the track database
db=connection.track

file = open("data/student_homework.js", "w")

#returns a list of distinct students in overall_sample_set
def find_all_students():
    students = db.overall_sample_set.distinct("student_id")
    return students #5395

# create new collection of number of completed (correct) homeworks per student
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

    num = 0
    #loop will only run once
    for doc in average:
        num = doc['avg_assignments']
        # print "Average number of homeworks completed by students:", doc['avg_assignments']
        # file.write("[\n")
        # file.write("[ 'Average', " + str(doc['avg_assignments']) + " ],\n")
    file.write(str(num))

# get number of homeworks completed for this student
def find_student_num_hws(sid):
    cursor = db.homework_assignments.find({"_id":sid})
    num_hw = 0
    #loop will only run once
    for doc in cursor:
        num_hw = doc['num_assignments']

    file.write("'num_hw':")
    file.write(str(num_hw))
    file.write(",")

# find the number of homeworks completed correctly on the first attempt
def first_attempt_homeworks():
    num_completed_homeworks()
    db.overall_sample_set.aggregate([
        {"$unwind":"$events"},
        {"$match":{"events.correct":True, "events.vertical_type":"problem", "events.lesson_format":"homework",
         "events.attempt":1}},
        {"$group": {"_id": "$student_id", "num_homeworks":{"$sum":1}}},
        {"$out":"first_homework"}
        ])

def find_student_num_hws_first_attempt(sid):
    cursor = db.first_homework.find({"_id":sid})
    num = 0
    #loop will only run once
    for doc in cursor:
        num = doc['num_homeworks']
    
    file.write("'first_attempt':" + str(num))


def run(): 
    all_students = find_all_students()

    num_completed_homeworks()
    first_attempt_homeworks()

    file.write("var avg_completed_homeworks = ")
    find_avg_completed_homeworks()
    file.write(";\n")

    file.write("var student_homework_data = {")
    for student in all_students:
        file.write(str(student) +": {")
        find_student_num_hws(student)
        find_student_num_hws_first_attempt(student)
        file.write("},")
        file.write("\n")

    file.write("};")

run()
