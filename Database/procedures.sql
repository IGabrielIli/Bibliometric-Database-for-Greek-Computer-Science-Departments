-- =======================
-- Departments Procedures
-- =======================
use citations_v2;
DELIMITER //
CREATE PROCEDURE get_all_departments()
BEGIN
  SELECT * FROM departments;
END //

CREATE PROCEDURE get_count_staff(IN dept_id INT)
BEGIN
  SELECT COUNT(*) AS staff_count FROM staff_dept_role WHERE department_id = dept_id;
END //

CREATE PROCEDURE get_avg_h_index(IN dept_id INT)
BEGIN
  SELECT AVG(h_index) AS avg_h_index, AVG(last_5_years_h_index) AS avg_h_index_5yr
  FROM staff_statistics s
  JOIN staff_dept_role sd ON s.staff_id = sd.staff_id
  WHERE sd.department_id = dept_id;
END //

CREATE PROCEDURE get_avg_i10_index(IN dept_id INT)
BEGIN
  SELECT AVG(i10_index) AS avg_i10_index, AVG(last_5_years_i10_index) AS avg_i10_index_5yr
  FROM staff_statistics s
  JOIN staff_dept_role sd ON s.staff_id = sd.staff_id
  WHERE sd.department_id = dept_id;
END //

-- =======================
-- Roles Procedures
-- =======================

CREATE PROCEDURE get_all_roles()
BEGIN
  SELECT * FROM roles;
END //

-- =======================
-- Staff Procedures
-- =======================

CREATE PROCEDURE get_all_staff()
BEGIN
  SELECT * FROM staff;
END //

CREATE PROCEDURE get_staff_dept_role()
BEGIN
  SELECT * FROM staff_dept_role;
END //

CREATE PROCEDURE get_staff_dept_role_by_dept(IN dept_id INT)
BEGIN
  SELECT * FROM staff_dept_role WHERE department_id = dept_id;
END //

-- =======================
-- H-index Procedures
-- =======================

CREATE PROCEDURE get_staff_h_index_all(IN staff_id INT)
BEGIN
  SELECT h_index FROM staff_statistics WHERE staff_id = staff_id;
END //

CREATE PROCEDURE get_staff_h_index_all_5yr(IN staff_id INT)
BEGIN
  SELECT last_5_years_h_index FROM staff_statistics WHERE staff_id = staff_id;
END //

CREATE PROCEDURE get_staff_h_index_scholar(IN staff_id INT)
BEGIN
  SELECT h_index, last_5_years_h_index FROM staff_statistics WHERE staff_id = staff_id;
END //

CREATE PROCEDURE get_staff_h_index_local(IN staff_id INT)
BEGIN
  SELECT h_index_local, last_5_years_h_index_local FROM staff_statistics WHERE staff_id = staff_id;
END //

CREATE PROCEDURE get_staff_h_index_from_graph(IN staff_id INT)
BEGIN
  SELECT h_index_from_graph, last_5_years_h_index_from_graph FROM staff_statistics WHERE staff_id = staff_id;
END //



CREATE PROCEDURE get_staff_h_index_scholar_5yr(IN p_staff_id INT)
BEGIN
  SELECT last_5_years_h_index 
  FROM staff_statistics 
  WHERE staff_id = p_staff_id;
END //



-- =======================
-- i10-index Procedures
-- =======================

CREATE PROCEDURE get_staff_i10_index_all(IN staff_id INT)
BEGIN
  SELECT i10_index FROM staff_statistics WHERE staff_id = staff_id;
END //

CREATE PROCEDURE get_staff_i10_index_all_5yr(IN staff_id INT)
BEGIN
  SELECT last_5_years_i10_index FROM staff_statistics WHERE staff_id = staff_id;
END //

CREATE PROCEDURE get_staff_i10_index_scholar(IN staff_id INT)
BEGIN
  SELECT i10_index, last_5_years_i10_index FROM staff_statistics WHERE staff_id = staff_id;
END //

CREATE PROCEDURE get_staff_i10_index_local(IN staff_id INT)
BEGIN
  SELECT i10_index_local, last_5_years_i10_index_local FROM staff_statistics WHERE staff_id = staff_id;
END //

CREATE PROCEDURE get_staff_i10_index_from_graph(IN staff_id INT)
BEGIN
  SELECT i10_index_from_graph, last_5_years_i10_index_from_graph FROM staff_statistics WHERE staff_id = staff_id;
END //



CREATE PROCEDURE get_staff_i10_index_scholar_5yr(IN p_staff_id INT)
BEGIN
  SELECT last_5_years_i10_index 
  FROM staff_statistics 
  WHERE staff_id = p_staff_id;
END //


-- =======================
-- Citations Per Year
-- =======================

CREATE PROCEDURE get_staff_citations_per_year(IN staff_id INT)
BEGIN
  SELECT * FROM staff_citations_per_year WHERE staff_id = staff_id;
END //

CREATE PROCEDURE get_staff_citations_per_year_by_dept(IN dept_id INT)
BEGIN
  SELECT s.*
  FROM staff_citations_per_year s
  JOIN staff_dept_role sd ON s.staff_id = sd.staff_id
  WHERE sd.department_id = dept_id;
END //

CREATE PROCEDURE get_publication_citations_per_year(IN publication_id INT)
BEGIN
  SELECT * FROM publication_citations_per_year WHERE publication_id = publication_id;
END //

-- =======================
-- Publications
-- =======================

CREATE PROCEDURE get_publications(IN staff_id INT)
BEGIN
  SELECT p.*
  FROM publications p
  JOIN publications_staff ps ON p.publication_id = ps.publication_id
  WHERE ps.staff_id = staff_id;
END //

CREATE PROCEDURE get_publications_staff_by_dept(IN dept_id INT)
BEGIN
  SELECT p.*
  FROM publications p
  JOIN publications_staff ps ON p.publication_id = ps.publication_id
  JOIN staff_dept_role sd ON ps.staff_id = sd.staff_id
  WHERE sd.department_id = dept_id;
END //

CREATE PROCEDURE get_count_publications(IN staff_id INT)
BEGIN
  SELECT COUNT(*) AS publication_count
  FROM publications_staff
  WHERE staff_id = staff_id;
END //

CREATE PROCEDURE get_count_citations(IN staff_id INT)
BEGIN
  SELECT SUM(citations) AS total_citations
  FROM publications p
  JOIN publications_staff ps ON p.publication_id = ps.publication_id
  WHERE ps.staff_id = staff_id;
END //

CREATE PROCEDURE get_count_publications_5y(IN staff_id INT)
BEGIN
  SELECT COUNT(*) AS publication_count_5y
  FROM publications p
  JOIN publications_staff ps ON p.publication_id = ps.publication_id
  WHERE ps.staff_id = staff_id AND publication_year >= YEAR(CURDATE()) - 5;
END //

CREATE PROCEDURE get_count_citations_5y(IN staff_id INT)
BEGIN
  SELECT SUM(citations) AS total_citations_5y
  FROM publications p
  JOIN publications_staff ps ON p.publication_id = ps.publication_id
  WHERE ps.staff_id = staff_id AND publication_year >= YEAR(CURDATE()) - 5;
END //

-- =======================
-- Special
-- =======================

CREATE PROCEDURE get_age(IN staff_id INT)
BEGIN
  SELECT MAX(publication_year) - MIN(publication_year) AS age
  FROM publications p
  JOIN publications_staff ps ON p.publication_id = ps.publication_id
  WHERE ps.staff_id = staff_id;
END //

DELIMITER ;


DELIMITER //
CREATE PROCEDURE get_staff_by_dept(IN department_id INT)
BEGIN
  SELECT 
    s.staff_id,
    CONCAT(s.first_name, ' ', s.last_name) AS staff_full_name,
    r.role_id,
    r.role_title,
    d.department_id AS dept_id,
    d.short_code AS dept_code,
    d.department_title AS dept_title,
    d.university AS university
  FROM 
    staff s
  JOIN 
    staff_dept_role sdr ON s.staff_id = sdr.staff_id
  JOIN 
    roles r ON sdr.role_id = r.role_id
  JOIN 
    departments d ON sdr.department_id = d.department_id
  WHERE 
    d.department_id = department_id;
END //

DELIMITER $$

DROP PROCEDURE IF EXISTS get_all_basic_stats_for_a_staff $$
CREATE PROCEDURE get_all_basic_stats_for_a_staff(IN p_staff_id INT)
BEGIN
    SELECT
        -- Total publications
        (SELECT COUNT(*) 
         FROM publications_staff 
         WHERE staff_id = p_staff_id) AS publication_count,

        -- Publications in last 5 years
        (SELECT COUNT(*) 
         FROM publications p
         JOIN publications_staff ps ON p.publication_id = ps.publication_id
         WHERE ps.staff_id = p_staff_id AND publication_year >= YEAR(CURDATE()) - 5
        ) AS publication_count_5y,

        -- Total citations
        (SELECT SUM(citations) 
         FROM publications p
         JOIN publications_staff ps ON p.publication_id = ps.publication_id
         WHERE ps.staff_id = p_staff_id
        ) AS total_citations,

        -- Citations last 5y
        (SELECT SUM(citations) 
         FROM publications p
         JOIN publications_staff ps ON p.publication_id = ps.publication_id
         WHERE ps.staff_id = p_staff_id AND publication_year >= YEAR(CURDATE()) - 5
        ) AS total_citations_5y,

        -- h-index
        (SELECT h_index 
         FROM staff_statistics 
         WHERE staff_id = p_staff_id
        ) AS h_index,

        -- h-index 5y
        (SELECT last_5_years_h_index 
         FROM staff_statistics 
         WHERE staff_id = p_staff_id
        ) AS h_index_5y,

        -- i10-index
        (SELECT i10_index 
         FROM staff_statistics 
         WHERE staff_id = p_staff_id
        ) AS i10_index,

        -- i10-index 5y
        (SELECT last_5_years_i10_index 
         FROM staff_statistics 
         WHERE staff_id = p_staff_id
        ) AS i10_index_5y;
END $$

DELIMITER ;


DELIMITER $$

DROP PROCEDURE IF EXISTS get_all_staff_summary $$
CREATE PROCEDURE get_all_staff_summary(
    IN dept_ids TEXT,
    IN role_ids TEXT
)
BEGIN
    SELECT 
        s.staff_id,
        s.first_name,
        s.last_name,
        s.scholar_id,
        sdr.department_id,
        sdr.role_id,

        ss.total_citations,
        ss.last_5_years_citations,
        
        ss.h_index,
        ss.last_5_years_h_index,
        ss.h_index_local,
        ss.last_5_years_h_index_local,
        ss.h_index_from_graph,
        ss.last_5_years_h_index_from_graph,
        
        ss.i10_index,
        ss.last_5_years_i10_index,
        ss.i10_index_local,
        ss.last_5_years_i10_index_local,
        ss.i10_index_from_graph,
        ss.last_5_years_i10_index_from_graph,

        IFNULL(pub_stats.age, 0) AS age,
        IFNULL(pub_stats.total_publications, 0) AS total_publications,
        IFNULL(pub_stats.total_publications_5y, 0) AS total_publications_5y

    FROM staff s

    INNER JOIN staff_dept_role sdr ON s.staff_id = sdr.staff_id
    LEFT JOIN staff_statistics ss ON s.staff_id = ss.staff_id

    LEFT JOIN (
        SELECT 
            ps.staff_id,
            MAX(p.publication_year) - MIN(p.publication_year) AS age,
            COUNT(DISTINCT ps.publication_id) AS total_publications,
            SUM(CASE 
                    WHEN p.publication_year >= YEAR(CURDATE()) - 5 
                    THEN 1 ELSE 0 
                END
            ) AS total_publications_5y
        FROM publications p
        JOIN publications_staff ps ON p.publication_id = ps.publication_id
        GROUP BY ps.staff_id
    ) AS pub_stats ON pub_stats.staff_id = s.staff_id

    WHERE 
        FIND_IN_SET(sdr.department_id, dept_ids)
        AND FIND_IN_SET(sdr.role_id, role_ids);
END $$

DELIMITER ;


DELIMITER $$

DROP PROCEDURE IF EXISTS get_publications_per_year_by_staff $$
CREATE PROCEDURE get_publications_per_year_by_staff(IN p_staff_id INT)
BEGIN
    SELECT 
        p.publication_year,
        COUNT(*) AS total_publications
    FROM publications p
    JOIN publications_staff ps ON p.publication_id = ps.publication_id
    WHERE 
        ps.staff_id = p_staff_id
        AND p.publication_year IS NOT NULL
        AND p.publication_year <= YEAR(CURDATE())
    GROUP BY p.publication_year
    ORDER BY p.publication_year;
END $$

DELIMITER ;

DELIMITER $$

DROP PROCEDURE IF EXISTS get_citations_per_year_by_staff $$
CREATE PROCEDURE get_citations_per_year_by_staff(IN p_staff_id INT)
BEGIN
    SELECT 
        p.publication_year,
        SUM(p.citations) AS total_citations
    FROM publications p
    JOIN publications_staff ps ON p.publication_id = ps.publication_id
    WHERE 
        ps.staff_id = p_staff_id
        AND p.publication_year IS NOT NULL
    GROUP BY p.publication_year
    ORDER BY p.publication_year;
END $$

DELIMITER ;

DELIMITER $$

DROP PROCEDURE IF EXISTS get_all_publications_by_staff $$
CREATE PROCEDURE get_all_publications_by_staff(IN p_staff_id INT)
BEGIN
	SELECT 
		p.publication_id,
		p.publication_title,
		p.authors,
        ps.author_order,
		p.publication_date,
		p.journal,
		p.publisher,
		p.citations,
		p.publication_url
	FROM publications p 
    JOIN publications_staff ps ON p.publication_id = ps.publication_id
	JOIN staff s ON ps.staff_id = s.staff_id
	WHERE s.staff_id = p_staff_id;
END $$

DELIMITER ;


DELIMITER $$

DROP PROCEDURE IF EXISTS get_department_stats_by_roles $$
CREATE PROCEDURE get_department_stats_by_roles(IN role_ids TEXT)
BEGIN
    SELECT
        d.department_id,
        d.department_title,

        COUNT(DISTINCT sdr.staff_id) AS staff_count,

        COUNT(DISTINCT ps.publication_id) AS total_pubs,
        SUM(p.citations) AS total_citations,

        -- Μέσοι όροι
        ROUND(AVG(ss.h_index), 2) AS avg_h_index,
        ROUND(AVG(ss.h_index_local), 2) AS avg_h_index_local,
        ROUND(AVG(ss.h_index_from_graph), 2) AS avg_h_index_from_graph,

        ROUND(AVG(ss.i10_index), 2) AS avg_i10_index,
        ROUND(AVG(ss.i10_index_local), 2) AS avg_i10_index_local,
        ROUND(AVG(ss.i10_index_from_graph), 2) AS avg_i10_index_from_graph,

        -- Stats per staff
        ROUND(AVG(staff_stats.total_pubs), 2) AS avg_pubs,
        ROUND(AVG(staff_stats.total_citations), 2) AS avg_citations,
        ROUND(AVG(staff_stats.age), 2) AS avg_age,

        ROUND(STDDEV(staff_stats.total_pubs) / NULLIF(AVG(staff_stats.total_pubs), 0), 2) AS cv_pubs,
        ROUND(STDDEV(staff_stats.total_citations) / NULLIF(AVG(staff_stats.total_citations), 0), 2) AS cv_cits,

        MAX(staff_stats.total_pubs) AS max_pubs,
        MIN(staff_stats.total_pubs) AS min_pubs,

        MAX(staff_stats.total_citations) AS max_cits,
        MIN(staff_stats.total_citations) AS min_cits,

        MAX(ss.h_index) AS max_h_index,
        MIN(ss.h_index) AS min_h_index,

        MAX(ss.i10_index) AS max_i10_index,
        MIN(ss.i10_index) AS min_i10_index

    FROM staff_dept_role sdr
    INNER JOIN departments d ON sdr.department_id = d.department_id
    INNER JOIN staff s ON sdr.staff_id = s.staff_id
    LEFT JOIN staff_statistics ss ON ss.staff_id = s.staff_id

    -- υπολογισμός per-staff stats για aggregation
    LEFT JOIN (
        SELECT
            ps.staff_id,
            COUNT(DISTINCT ps.publication_id) AS total_pubs,
            SUM(p.citations) AS total_citations,
            MAX(p.publication_year) - MIN(p.publication_year) AS age
        FROM publications_staff ps
        JOIN publications p ON p.publication_id = ps.publication_id
        WHERE p.publication_year IS NOT NULL AND p.publication_year <= YEAR(CURDATE())
        GROUP BY ps.staff_id
    ) AS staff_stats ON staff_stats.staff_id = s.staff_id

    WHERE FIND_IN_SET(sdr.role_id, role_ids)

    GROUP BY d.department_id, d.department_title
    ORDER BY d.department_title;
END $$
DELIMITER ;


DELIMITER $$

DROP PROCEDURE IF EXISTS get_department_stats_by_depts_and_roles $$

CREATE PROCEDURE get_department_stats_by_depts_and_roles(
  IN dept_ids TEXT,
  IN role_ids TEXT
)
BEGIN
    SELECT
        d.department_id,
        d.short_code,

        COUNT(DISTINCT sdr.staff_id) AS staff_count,

        COUNT(DISTINCT pub_data.publication_id) AS total_pubs,
        SUM(pub_data.citations) AS total_citations,

        ROUND(AVG(ss.h_index), 1) AS avg_h_index,
        ROUND(AVG(ss.last_5_years_h_index), 1) AS avg_h_index_5y,

        ROUND(AVG(ss.h_index_local), 1) AS avg_h_index_local,
        ROUND(AVG(ss.last_5_years_h_index_local), 1) AS avg_h_index_local_5y,

        ROUND(AVG(ss.h_index_from_graph), 1) AS avg_h_index_from_graph,
        ROUND(AVG(ss.last_5_years_h_index_from_graph), 1) AS avg_h_index_from_graph_5y,

        ROUND(AVG(ss.i10_index), 1) AS avg_i10_index,
        ROUND(AVG(ss.last_5_years_i10_index), 1) AS avg_i10_index_5y,

        ROUND(AVG(ss.i10_index_local), 1) AS avg_i10_index_local,
        ROUND(AVG(ss.last_5_years_i10_index_local), 1) AS avg_i10_index_local_5y,

        ROUND(AVG(ss.i10_index_from_graph), 1) AS avg_i10_index_from_graph,
        ROUND(AVG(ss.last_5_years_i10_index_from_graph), 1) AS avg_i10_index_from_graph_5y,

        -- Per-staff aggregation
        ROUND(AVG(staff_stats.total_pubs), 1) AS avg_pubs,
        ROUND(AVG(staff_stats.total_pubs_5y), 1) AS avg_pubs_5y,

        ROUND(AVG(staff_stats.total_citations), 1) AS avg_citations,
        ROUND(AVG(staff_stats.total_citations_5y), 1) AS avg_citations_5y,

        ROUND(AVG(staff_stats.age), 2) AS avg_age,

        ROUND(STDDEV(staff_stats.total_pubs) / NULLIF(AVG(staff_stats.total_pubs), 0) * 100, 1) AS cv_pubs,
        ROUND(STDDEV(staff_stats.total_citations) / NULLIF(AVG(staff_stats.total_citations), 0) * 100, 1) AS cv_cits,

        MAX(staff_stats.total_pubs) AS max_pubs,
        MIN(staff_stats.total_pubs) AS min_pubs,

        MAX(staff_stats.total_citations) AS max_cits,
        MIN(staff_stats.total_citations) AS min_cits,

        MAX(ss.h_index) AS max_h_index,
        MIN(ss.h_index) AS min_h_index,

        MAX(ss.i10_index) AS max_i10_index,
        MIN(ss.i10_index) AS min_i10_index

    FROM staff_dept_role sdr
    INNER JOIN departments d ON sdr.department_id = d.department_id
    INNER JOIN staff s ON sdr.staff_id = s.staff_id
    LEFT JOIN staff_statistics ss ON ss.staff_id = s.staff_id

    -- Publications joined for total pubs/citations
    LEFT JOIN (
        SELECT
            ps.staff_id,
            ps.publication_id,
            p.citations
        FROM publications_staff ps
        JOIN publications p ON p.publication_id = ps.publication_id
        WHERE p.publication_year IS NOT NULL AND p.publication_year <= YEAR(CURDATE())
    ) AS pub_data ON pub_data.staff_id = s.staff_id

    -- Aggregation per staff
    LEFT JOIN (
        SELECT
            ps.staff_id,
            COUNT(DISTINCT ps.publication_id) AS total_pubs,
            SUM(CASE WHEN p.publication_year >= YEAR(CURDATE()) - 5 THEN 1 ELSE 0 END) AS total_pubs_5y,

            SUM(p.citations) AS total_citations,
            SUM(CASE WHEN p.publication_year >= YEAR(CURDATE()) - 5 THEN p.citations ELSE 0 END) AS total_citations_5y,

            MAX(p.publication_year) - MIN(p.publication_year) AS age
        FROM publications_staff ps
        JOIN publications p ON p.publication_id = ps.publication_id
        WHERE p.publication_year IS NOT NULL AND p.publication_year <= YEAR(CURDATE())
        GROUP BY ps.staff_id
    ) AS staff_stats ON staff_stats.staff_id = s.staff_id

    WHERE FIND_IN_SET(sdr.role_id, role_ids)
      AND FIND_IN_SET(sdr.department_id, dept_ids)

    GROUP BY d.department_id, d.department_title
    ORDER BY d.department_title;
END $$
DELIMITER ;


DELIMITER $$

DROP PROCEDURE IF EXISTS get_overall_stats_by_staff_ids $$

CREATE PROCEDURE get_overall_stats_by_staff_ids(IN staff_ids TEXT)
BEGIN
    SELECT
        -- Αριθμός μελών
        COUNT(DISTINCT s.staff_id) AS count_staff,

        -- Σύνολο δημοσιεύσεων
        SUM(pub_stats.total_pubs) AS total_pubs,
        SUM(pub_stats.total_citations) AS total_citations,

        SUM(pub_stats.total_pubs_5y) AS total_pubs_5y,
        SUM(pub_stats.total_citations_5y) AS total_citations_5y,

        -- Μέσοι όροι per member
        ROUND(AVG(pub_stats.total_pubs), 1) AS avg_pubs_per_m,
        ROUND(AVG(pub_stats.total_citations), 1) AS avg_cits_per_m,

        ROUND(AVG(pub_stats.age), 2) AS avg_age,

        ROUND(AVG(pub_stats.total_pubs / NULLIF(pub_stats.age, 0)), 1) AS avg_pubs_per_m_per_y,
        ROUND(AVG(pub_stats.total_citations / NULLIF(pub_stats.age, 0)), 1) AS avg_cits_per_m_per_y,

        -- h-indexes
        ROUND(AVG(ss.h_index), 1) AS avg_h_index,
        ROUND(AVG(ss.h_index_local), 1) AS avg_h_index_local,
        ROUND(AVG(ss.h_index_from_graph), 1) AS avg_h_index_from_graph,

        ROUND(AVG(ss.last_5_years_h_index), 1) AS avg_h_index_5y,
        ROUND(AVG(ss.last_5_years_h_index_local), 1) AS avg_h_index_local_5y,
        ROUND(AVG(ss.last_5_years_h_index_from_graph), 1) AS avg_h_index_from_graph_5y,

        -- i10-indexes
        ROUND(AVG(ss.i10_index), 1) AS avg_i10_index,
        ROUND(AVG(ss.i10_index_local), 1) AS avg_i10_index_local,
        ROUND(AVG(ss.i10_index_from_graph), 1) AS avg_i10_index_from_graph,

        ROUND(AVG(ss.last_5_years_i10_index), 1) AS avg_i10_index_5y,
        ROUND(AVG(ss.last_5_years_i10_index_local), 1) AS avg_i10_index_local_5y,
        ROUND(AVG(ss.last_5_years_i10_index_from_graph), 1) AS avg_i10_index_from_graph_5y

    FROM staff s
    LEFT JOIN staff_statistics ss ON s.staff_id = ss.staff_id

    LEFT JOIN (
        SELECT
            ps.staff_id,
            COUNT(DISTINCT ps.publication_id) AS total_pubs,
            SUM(p.citations) AS total_citations,

            SUM(CASE WHEN p.publication_year >= YEAR(CURDATE()) - 5 THEN 1 ELSE 0 END) AS total_pubs_5y,
            SUM(CASE WHEN p.publication_year >= YEAR(CURDATE()) - 5 THEN p.citations ELSE 0 END) AS total_citations_5y,

            MAX(p.publication_year) - MIN(p.publication_year) AS age

        FROM publications_staff ps
        JOIN publications p ON p.publication_id = ps.publication_id
        WHERE p.publication_year IS NOT NULL AND p.publication_year <= YEAR(CURDATE())
        GROUP BY ps.staff_id
    ) AS pub_stats ON pub_stats.staff_id = s.staff_id

    WHERE FIND_IN_SET(s.staff_id, staff_ids);
END $$

DELIMITER ;

