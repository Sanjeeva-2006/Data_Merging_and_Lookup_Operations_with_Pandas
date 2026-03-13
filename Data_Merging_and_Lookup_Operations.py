# Data Merging and Lookup Operations with Pandas

import pandas as pd

print("=== TASK 1: Data Preparation ===\n")

# -------------------------------------------------
# Create Students DataFrame
# -------------------------------------------------

students_data = {
    'student_id': [101,102,103,104,105,106,107],
    'name': ['Alice','Bob',None,'David','Emma','Frank','Grace'],
    'email': ['alice@email.com','bob@email.com','charlie@email.com',None,
              'emma@email.com','frank@email.com','grace@email.com'],
    'city': ['Mumbai','Delhi','Bangalore','Mumbai',None,'Chennai','Delhi']
}

students = pd.DataFrame(students_data)

print("Original Students DataFrame")
print(students)


# -------------------------------------------------
# Enrollments DataFrame
# -------------------------------------------------

enrollments_data = {
    'student_id': [101,102,103,105,108,109],
    'course_name': ['Python','Data Science','Python','Machine Learning','AI','Python'],
    'enrollment_date': ['2024-01-15','2024-01-20','2024-02-01',
                        '2024-02-10','2024-02-15','2024-03-01']
}

enrollments = pd.DataFrame(enrollments_data)


# -------------------------------------------------
# Scores DataFrame
# -------------------------------------------------

scores_data = {
    'student_id': [101,102,104,105,106],
    'exam_score': [85,92,78,88,95]
}

scores = pd.DataFrame(scores_data)


# -------------------------------------------------
# Null Value Analysis
# -------------------------------------------------

print("\nNull Value Analysis")

for column in students.columns:
    null_count = students[column].isnull().sum()
    percentage = (null_count / len(students)) * 100
    print(f"Column: {column}, Nulls: {null_count} ({percentage:.2f}%)")


# -------------------------------------------------
# Handle Missing Values
# -------------------------------------------------

students['city'].fillna('Unknown', inplace=True)

students_clean = students.dropna(subset=['name']).copy()

print("\nCleaned Students DataFrame")
print(students_clean)



# =================================================
# TASK 2: JOIN OPERATIONS
# =================================================

print("\n=== TASK 2: Join Operations ===\n")


# INNER JOIN
inner_join = pd.merge(students_clean, enrollments, on='student_id', how='inner')

print("Inner Join Result")
print(inner_join)

print("Total rows:", len(inner_join))


excluded_students = students_clean[
    ~students_clean['student_id'].isin(enrollments['student_id'])
]

print("\nExcluded students:")
print(excluded_students[['student_id','name']])


# LEFT JOIN
left_join = pd.merge(students_clean, enrollments, on='student_id', how='left')

print("\nLeft Join Result")
print(left_join)

print("Total rows:", len(left_join))

print("\nStudents with null course_name")
print(left_join[left_join['course_name'].isnull()][['student_id','name']])


# RIGHT JOIN
right_join = pd.merge(students_clean, enrollments, on='student_id', how='right')

print("\nRight Join Result")
print(right_join)

print("Total rows:", len(right_join))

print("\nStudent IDs without names")
print(right_join[right_join['name'].isnull()]['student_id'])


# OUTER JOIN
outer_join = pd.merge(students_clean, enrollments, on='student_id', how='outer')

print("\nFull Outer Join Result")
print(outer_join)

print("Total rows:", len(outer_join))


print("\nRows where name OR course missing")
print(
    outer_join[
        (outer_join['name'].isnull()) |
        (outer_join['course_name'].isnull())
    ]
)


# OUTER JOIN WITH INDICATOR
outer_indicator = pd.merge(
    students_clean,
    enrollments,
    on='student_id',
    how='outer',
    indicator=True
)

print("\nMerge Indicator Distribution")
print(outer_indicator['_merge'].value_counts())



# =================================================
# TASK 3: LOOKUP OPERATIONS
# =================================================

print("\n=== TASK 3: Lookup Operations ===\n")


# LOOKUP USING MAP
score_dict = scores.set_index('student_id')['exam_score'].to_dict()

students_clean.loc[:, 'exam_score'] = students_clean['student_id'].map(score_dict)

print("Students with exam scores using lookup")
print(students_clean[['student_id','name','exam_score']])


# -------------------------------------------------
# ADD SCORES USING MERGE
# -------------------------------------------------

students_for_merge = students.dropna(subset=['name']).copy()

merge_scores = pd.merge(
    students_for_merge,
    scores,
    on='student_id',
    how='left'
)

print("\nStudents with exam scores using merge")
print(merge_scores[['student_id','name','exam_score']])


# -------------------------------------------------
# Explanation
# -------------------------------------------------

print("\nWhy map() is faster than merge():")
print("map() performs a direct dictionary lookup using student_id.")
print("merge() compares entire DataFrames, which is slower for simple lookups.")



# =================================================
# AUTOMATION FUNCTION
# =================================================

def auto_merge(df1, df2, join_type, key_column):

    merged_df = pd.merge(df1, df2, how=join_type, on=key_column)

    result = {
        'result_df': merged_df,
        'row_count': len(merged_df),
        'join_type': join_type
    }

    return result


print("\nAutomation Function Test\n")

test_inner = auto_merge(students_clean, enrollments, "inner", "student_id")

print("Join Type:", test_inner['join_type'])
print("Rows:", test_inner['row_count'])
print(test_inner['result_df'].head())


print("\n")

test_left = auto_merge(students_clean, enrollments, "left", "student_id")

print("Join Type:", test_left['join_type'])
print("Rows:", test_left['row_count'])
print(test_left['result_df'].head())