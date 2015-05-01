import pymongo
import sys

#establish a connection to the database
connection = pymongo.Connection("mongodb://localhost", safe=True)

#get a handle to the track database
db=connection.track

#open data file to write to
file = open("data/student_quiz.js", "w")

#returns a list of distinct students in overall_sample_set
def find_all_students():
    students = db.overall_sample_set.distinct("student_id")
    return students #5395

#find the number of correct, completed quizzes by a student
def find_num_correct_quiz_submits():
    db.overall_sample_set.aggregate([
        {"$unwind":"$events"},
        {"$match":{
            "events.event_type":"submit", 
            "events.vertical_type":"problem", 
            "events.lesson_format":"lesson", 
            "events.correct":True}},
        {"$group": {"_id": "$student_id", "num_submits":{"$sum":1}}},
        {"$out":"num_correct_quiz_submits"}
        ])

def find_num_incorrect_quiz_submits():
    db.overall_sample_set.aggregate([
        {"$unwind":"$events"},
        {"$match":{
            "events.event_type":"submit", 
            "events.vertical_type":"problem", 
            "events.lesson_format":"lesson", 
            "events.correct":False}},
        {"$group": {"_id": "$student_id", "num_submits":{"$sum":1}}},
        {"$out":"num_incorrect_quiz_submits"}
        ])

# find the average number of correct quiz submissions
def find_avg_correct_quizzes():
    average = db.num_correct_quiz_submits.aggregate([
    {"$group": {"_id":"null", "avg_submits":{"$avg":"$num_submits"}}},
    {"$project": {"_id":0, "avg_submits":1}}], cursor={})

    num = 0
    #loop will only run once
    for doc in average:
        num = doc['avg_submits']
        
    file.write(str(num))

# find the average number of incorrect quiz submissions
def find_avg_incorrect_quizzes():
    average = db.num_incorrect_quiz_submits.aggregate([
    {"$group": {"_id":"null", "avg_submits":{"$avg":"$num_submits"}}},
    {"$project": {"_id":0, "avg_submits":1}}], cursor={})

    num = 0
    #loop will only run once
    for doc in average:
        num = doc['avg_submits']
        
    file.write(str(num))

def find_student_info(sid):
    cursor_correct = db.num_correct_quiz_submits.find({"_id":sid})
    cursor_incorrect = db.num_incorrect_quiz_submits.find({"_id":sid})
    
    num_correct = 0
    num_incorrect = 0

    for doc in cursor_correct:
        num_correct = doc['num_submits']

    for doc in cursor_incorrect:
        num_incorrect = doc['num_submits']

    file.write(str(sid) +": {'correct':" + str(num_correct) + ", 'incorrect': " + str(num_incorrect) + "}, ")
    file.write("\n")

def run(): 

    find_num_correct_quiz_submits()
    find_num_incorrect_quiz_submits()

    file.write("var avg_correct_quizzes = ")
    find_avg_correct_quizzes()
    file.write(";\n")

    file.write("var avg_incorrect_quizzes = ")
    find_avg_incorrect_quizzes()
    file.write(";\n")

    file.write("var student_quiz_data = {")

    all_students = find_all_students()

    for student in all_students:
        find_student_info(student)

    file.write("};")


run()