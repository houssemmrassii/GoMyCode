
Q1
SELECT s.session_id, s.date
FROM students st
JOIN attendance a ON st.student_id = a.student_id
JOIN sessions s ON a.session_id = s.session_id
WHERE st.name = 'Alice';

Q2
SELECT st.name, COUNT(*) AS session_count
FROM students st
JOIN attendance a ON st.student_id = a.student_id
GROUP BY st.name
ORDER BY session_count DESC
LIMIT 3;

Q3
SELECT s.session_id, COUNT(a.student_id) AS student_count
FROM sessions s
JOIN attendance a ON s.session_id = a.session_id
GROUP BY s.session_id;

Q4
SELECT s.module, COUNT(DISTINCT a.student_id) AS unique_students
FROM sessions s
JOIN attendance a ON s.session_id = a.session_id
GROUP BY s.module;

Q5
SELECT st.name, st.student_id
FROM students st
LEFT JOIN attendance a ON st.student_id = a.student_id
WHERE a.session_id IS NULL;

