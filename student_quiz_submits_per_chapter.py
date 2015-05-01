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

def find_correct_quizzes_per_chapter():
    db.overall_sample_set.aggregate([
        {"$unwind":"$events"},
        {"$match":{
            "events.vertical_type":"problem", 
            "events.event_type":"submit", 
            "events.lesson_format":"lesson", 
            "events.correct":True}},
        {"$group": {"_id": { "student_id": "$student_id", "chapter": "$events.chapter" }, "num_quiz_submits":{"$sum":1}}},
        {"$out":"student_correct_quiz_submits_per_chapter"}
        ])

def find_incorrect_quizzes_per_chapter():
    db.overall_sample_set.aggregate([
        {"$unwind":"$events"},
        {"$match":{
            "events.vertical_type":"problem", 
            "events.event_type":"submit", 
            "events.lesson_format":"lesson", 
            "events.correct":False}},
        {"$group": {"_id": { "student_id": "$student_id", "chapter": "$events.chapter" }, "num_quiz_submits":{"$sum":1}}},
        {"$out":"student_incorrect_quiz_submits_per_chapter"}
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
    out = ""
    if s[0:7] == "Chapter" or s[0:4] == "Week":
        words = s.split('_')
        out = 'Chapter_' + words[1]
        return out
    elif s[0:4] == "week":
        out = 'Chapter_'+s[-1:]
        return out
    elif s == "Unit_1_Introduction" or s == "Introduction":
        return "Chapter_1"
    elif s == "Performance" or s == "Schema_Design":
        return "Chapter_3"
    elif s == "Aggregation":
        return "Chapter_5"
    else:
        print s


def find_student_info(student_id, chapters):
    cursor_correct = db.student_correct_quiz_submits_per_chapter.find({"_id.student_id":student_id})
    cursor_incorrect = db.student_incorrect_quiz_submits_per_chapter.find({"_id.student_id":student_id})

    file.write(str(student_id) +": {")

    d = {}
    for c in chapters:
        d[c] = {}
        d[c]['correct'] = 0
        d[c]['incorrect'] = 0
  
    sum = 0

    for doc in cursor_correct:
        sum = sum + 1
        chapter = chapter_string_parser(doc['_id']['chapter'])
        if (chapter):
            d[chapter]["correct"] = d[chapter]["correct"] + 1

    for doc in cursor_incorrect:
        sum = sum + 1
        chapter = chapter_string_parser(doc['_id']['chapter'])
        if (chapter):
            d[chapter]["incorrect"] = d[chapter]["incorrect"] + 1

    if student_id == 180925:
        print sum

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

    find_correct_quizzes_per_chapter()
    find_incorrect_quizzes_per_chapter()

    for student in all_students:
        find_student_info(student, chapters)

    file.write("};")


run()