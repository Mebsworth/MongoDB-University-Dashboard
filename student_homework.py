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

#### CREATING NEW DATA COLLECTIONS ####
# create new collection of number of completed (correct) homeworks per student
def correct_homeworks():
    db.overall_sample_set.aggregate([
        {"$unwind":"$events"},
        {"$match":{"events.correct":True, "events.vertical_type":"problem", "events.lesson_format":"homework"}},
        {"$group": {"_id": "$student_id", "num_assignments":{"$sum":1}}},
        {"$out":"correct_homework"}
        ])

def incorrect_homeworks():
    db.overall_sample_set.aggregate([
        {"$unwind":"$events"},
        {"$match":{"events.correct":False, "events.vertical_type":"problem", "events.lesson_format":"homework"}},
        {"$group": {"_id": "$student_id", "num_assignments":{"$sum":1}}},
        {"$out":"incorrect_homework"}
        ])

def first_attempt_homeworks():
    db.overall_sample_set.aggregate([
        {"$unwind":"$events"},
        {"$match":{"events.correct":True, "events.vertical_type":"problem", "events.lesson_format":"homework", "events.attempt":1}},
        {"$group": {"_id": "$student_id", "num_assignments":{"$sum":1}}},
        {"$out":"first_attempt_homeworks"}
    ])

#### AVERAGES #####
# find the average number of completed homeworks
def find_avg_correct_homeworks():
    average = db.correct_homework.aggregate([
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

# find the average number of completed homeworks
def find_avg_incorrect_homeworks():
    average = db.incorrect_homework.aggregate([
    {"$group": {"_id":"null", "avg_assignments":{"$avg":"$num_assignments"}}},
    {"$project": {"_id":0, "avg_assignments":1}}], cursor={})

    num = 0
    #loop will only run once
    for doc in average:
        num = doc['avg_assignments']
    file.write(str(num))

# find the average number of completed homeworks
def find_avg_first_attempt_homeworks():
    average = db.first_attempt_homeworks.aggregate([
    {"$group": {"_id":"null", "avg_assignments":{"$avg":"$num_assignments"}}},
    {"$project": {"_id":0, "avg_assignments":1}}], cursor={})

    num = 0
    #loop will only run once
    for doc in average:
        num = doc['avg_assignments']
    file.write(str(num))

#### STUDENT INFO ####
# get number of correct homeworks  for this student
def find_student_correct_hws(sid):
    cursor = db.correct_homework.find({"_id":sid})
    num_hw = 0
    #loop will only run once
    for doc in cursor:
        num_hw = doc['num_assignments']

    file.write("'num_correct_hw':")
    file.write(str(num_hw))
    file.write(",")

# get number of incorrect homeworks for this student
def find_student_incorrect_hws(sid):
    cursor = db.incorrect_homework.find({"_id":sid})
    num_hw = 0
    #loop will only run once
    for doc in cursor:
        num_hw = doc['num_assignments']

    file.write("'num_incorrect_hw':")
    file.write(str(num_hw))
    file.write(",")


def find_student_num_hws_first_attempt(sid):
    cursor = db.first_attempt_homeworks.find({"_id":sid})
    num = 0
    #loop will only run once
    for doc in cursor:
        num = doc['num_assignments']
    
    file.write("'first_attempt':" + str(num) + ",")


def run(): 
    all_students = find_all_students()

    correct_homeworks()
    incorrect_homeworks()
    first_attempt_homeworks()

    file.write("var avg_correct_homeworks = ")
    find_avg_correct_homeworks()
    file.write(";\n")

    file.write("var avg_incorrect_homeworks = ")
    find_avg_incorrect_homeworks()
    file.write(";\n")

    file.write("var avg_num_hws_first_attempt = ")
    find_avg_first_attempt_homeworks()
    file.write(";\n")

    file.write("var student_homework_data = {")
    for student in all_students:
        file.write(str(student) +": {")
        find_student_correct_hws(student)
        find_student_incorrect_hws(student)
        find_student_num_hws_first_attempt(student)
        file.write("},")
        file.write("\n")

    file.write("};")

run()
