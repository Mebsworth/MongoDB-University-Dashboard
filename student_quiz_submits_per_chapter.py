import pymongo
import sys

#establish a connection to the database
connection = pymongo.Connection("mongodb://localhost", safe=True)

#get a handle to the track database
db=connection.track

#open data file to write to
file = open("data/student_quiz_submits_per_chapter.js", "w")

#returns a list of distinct students in overall_sample_set
def find_all_students():
    students = db.overall_sample_set.distinct("student_id")
    return students #5395

def find_num_quizzes_per_chapter():
    db.overall_sample_set.aggregate([
        {"$unwind":"$events"},
        {"$match":{"events.vertical_type":"problem", "events.event_type":"submit", "events.lesson_format":"lesson", "events.correct":True}},
        {"$group": {"_id": { "student_id": "$student_id", "chapter": "$events.chapter" }, "num_quiz_submits":{"$sum":1}}},
        {"$out":"student_num_quiz_submits_per_chapter"}
        ])

def find_chapters():
    db.overall_sample_set.aggregate([
        {"$unwind":"$events"},
        {"$match": {"events.vertical_type":"problem", "events.lesson_format": "lesson"}}, 
        {"$group": {"_id": "$events.chapter"} },
        {"$out":"all_chapters"}
        ])
    chapters = db.all_chapters.distinct('_id')
    print chapters
    consolidated = set()
    for c in chapters:
        c = chapter_string_parser(c)
        if c:
            consolidated.add(c)
    print consolidated
    return consolidated

def chapter_string_parser(s):
    if s[0:7] == "Chapter":
        words = s.split('_')
        s = words[0] + '_' + words[1]
        return s

def find_student_info(student_id, chapters):
    cursor = db.student_num_quiz_submits_per_chapter.find({"_id.student_id":student_id})
    
    file.write(str(student_id) +": {")

    d = {}
    for c in chapters:
        d[c] = 0

    for doc in cursor:
        chapter = chapter_string_parser(doc['_id']['chapter'])
        if (chapter):
            d[chapter] = d[chapter] + 1

    for key in d:
        file.write("'" + key +"':" + str(d[key]) + ", ")
        # file.write("'"+ doc['_id']['chapter'] + "':")
        # file.write("[" + str(doc['num_hw_submits']) + ", "+ str(doc['num_quiz_submits']) + "], ")

    file.write("},")
    file.write("\n")
    

def run(): 
    file.write("var student_quiz_submits_per_chapter_data = {")

    all_students = find_all_students()
    num_students = len(all_students)
    print "Total Number of students = ", num_students

    chapters = find_chapters()

    find_num_quizzes_per_chapter()
    for student in all_students:
        find_student_info(student, chapters)

    file.write("};")


run()