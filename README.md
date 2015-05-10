# MongoDB-University-Dashboard

To summarize, the student dashboard is populated with 5 charts. Some charts combine some of the queries we previously worked on, and some contain new ones. 

The charts (for a specific Student X):
Quiz Performance Compared to Average (top left)
- Correct quiz submissions
- Incorrect quiz submissions
- Number of correct quiz submissions on the first attempt
Python script: student_quiz.py
Data file: data/student_quiz.js
Quiz Performance per Chapter (top right)
- How many correct and incorrect quiz submissions Student X did for each chapter
Python script: student_quiz_submits_per_chapter.py
Data file: data/student_quiz_submits_per_chapter.js
Homework Performance Compared to Average (middle left)
- Correct homework submissions
- Incorrect homework submissions
- Number of correct homework submissions on the first attempt
Python script: student_homework.py
Data file: data/student_homework.js
Homework Performance per Chapter (middle right)
- How many correct and incorrect homework submissions Student X did for each chapter
Python script: student_hw_submits_per_chapter.py
Data file: data/student_hw_submits_per_chapter.js
Number of Students at Each Level of Homework Progress (bottom)
- For each 'level' of homework process (grouping together 0-4 submissions, 5-9 submissions, etc.) counting the number of students who made it that far
Python script: grouped_homeworks.py
Data file: data/grouped_homeworks.js
