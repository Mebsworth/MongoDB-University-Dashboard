from pymongo import MongoClient
import sys

#establish a connection to the database
connection = MongoClient()

#get a handle to the track database
db=connection.track

#open data file to write to
file = open("data/student_num_assignments_completed.js", "w")

#returns a list of distinct students in overall_sample_set
def find_all_students():
    students = db.overall_sample_set.distinct("student_id")
    return students #5395

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
        #print "Average number of assignments completed by students:", doc['avg_assignments']
        file.write(str(doc['avg_assignments']))

# get number of assignments completed for this student
def find_student_info(sid):
    cursor = db.number_assignments.find({"_id":sid})

    file.write(str(sid) +":")
    num = 0
    #loop will only run once
    for doc in cursor:
        num = doc['num_assignments']
        # print "Student", sid, "number of completed assignments:", doc['num_assignments']
        # file.write("[ 'Student ID: " + str(sid) +"', " + str(doc['num_assignments']) + " ]\n")
        # file.write("]")
    file.write(str(num))
    file.write(",")
    file.write("\n")

def run(): 
    all_students = find_all_students()

    file.write("var avg_completed_assignments = ")
    find_avg_completed_assignments() 
    file.write(";\n")

    file.write("var student_num_assignments_completed_data = {")

    num_completed_assignments()

    for student in all_students:
        find_student_info(student)

    file.write("};")


run()
