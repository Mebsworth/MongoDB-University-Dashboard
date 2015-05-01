import pymongo
import sys

#establish a connection to the database
connection = pymongo.Connection("mongodb://localhost", safe=True)

#get a handle to the track database
db=connection.track

#open data file to write to
file = open("data/student_hw_submits_per_chapter.js", "w")

#returns a list of distinct students in overall_sample_set
def find_all_students():
    students = db.overall_sample_set.distinct("student_id")
    return students #5395

def find_num_hw_per_chapter():
    db.overall_sample_set.aggregate([
        {"$unwind":"$events"},
        {"$match":{"events.vertical_type":"problem", "events.event_type":"submit", "events.lesson_format":"homework", "events.correct":True}},
        {"$group": {"_id": { "student_id": "$student_id", "chapter": "$events.chapter" }, "num_hw_submits":{"$sum":1}}},
        {"$out":"student_num_hw_submits_per_chapter"}
        ])

def find_student_info(student_id):
    cursor = db.student_num_hw_submits_per_chapter.find({"_id.student_id":student_id})
    file.write(str(student_id) +": {")
    for doc in cursor:
        #print doc
        #print "Student:", doc['_id']['student_id'], "Chapter:", doc['_id']['chapter'], "Num Homework Submissions:", doc['num_hw_submits']
        file.write("'"+ doc['_id']['chapter'] + "':" + str(doc['num_hw_submits']) + ", ")
    file.write("},")
    file.write("\n")
    

def run(): 
    file.write("var student_hw_per_chapter_data = {")

    all_students = find_all_students()
    num_students = len(all_students)
    print "Total Number of students = ", num_students
    find_num_hw_per_chapter()
    for student in all_students:
        find_student_info(student)

    file.write("};")


run()