-- Previous Fracture events covariates construction

SELECT 100000800 AS covariate_id, row_id, 1 AS covariate_value
FROM (
	SELECT DISTINCT condition_concept_id,
		cohort.subject_id AS row_id
	FROM @cohort_table cohort
	INNER JOIN @cdm_database_schema.condition_occurrence
		ON cohort.subject_id =condition_occurrence.person_id
		WHERE condition_start_date < DATEADD(DAY, 0, cohort.cohort_start_date) AND condition_start_date >= DATEADD(DAY, -365, cohort.cohort_start_date)
		AND condition_concept_id in (select ca.descendant_concept_id from @cdm_database_schema.CONCEPT_ANCESTOR ca where ca.ancestor_concept_id in (4129394,4053828)) -- vertebral
		AND cohort.cohort_definition_id IN (@cohort_id)
) by_row_id
union all
SELECT 100001800 AS covariate_id, row_id, 1 AS covariate_value
FROM (
	SELECT DISTINCT condition_concept_id,
		cohort.subject_id AS row_id
	FROM @cohort_table cohort
	INNER JOIN @cdm_database_schema.condition_occurrence
		ON cohort.subject_id =condition_occurrence.person_id
		WHERE condition_start_date < DATEADD(DAY, 0, cohort.cohort_start_date) AND condition_start_date >= DATEADD(DAY, -365, cohort.cohort_start_date)
		AND condition_concept_id in (select ca.descendant_concept_id from @cdm_database_schema.CONCEPT_ANCESTOR ca where ca.ancestor_concept_id in (4138412,4230399,45767037)) -- Hip (Proximal femur)
		AND cohort.cohort_definition_id IN (@cohort_id)
) by_row_id
union all
SELECT 100002800 AS covariate_id, row_id, 1 AS covariate_value
FROM (
	SELECT DISTINCT condition_concept_id,
		cohort.subject_id AS row_id
	FROM @cohort_table cohort
	INNER JOIN @cdm_database_schema.condition_occurrence
		ON cohort.subject_id =condition_occurrence.person_id
		WHERE condition_start_date < DATEADD(DAY, 0, cohort.cohort_start_date) AND condition_start_date >= DATEADD(DAY, -365, cohort.cohort_start_date)
		AND condition_concept_id in (select ca.descendant_concept_id from @cdm_database_schema.CONCEPT_ANCESTOR ca where ca.ancestor_concept_id in (4300192)) -- Pelvis
		AND cohort.cohort_definition_id IN (@cohort_id)
) by_row_id
union all
SELECT 100003800 AS covariate_id, row_id, 1 AS covariate_value
FROM (
	SELECT DISTINCT condition_concept_id,
		cohort.subject_id AS row_id
	FROM @cohort_table cohort
	INNER JOIN @cdm_database_schema.condition_occurrence
		ON cohort.subject_id =condition_occurrence.person_id
		WHERE condition_start_date < DATEADD(DAY, 0, cohort.cohort_start_date) AND condition_start_date >= DATEADD(DAY, -365, cohort.cohort_start_date)
		AND condition_concept_id in (select ca.descendant_concept_id from @cdm_database_schema.CONCEPT_ANCESTOR ca where ca.ancestor_concept_id in (46270317,4302740)) -- Shoulder
		AND cohort.cohort_definition_id IN (@cohort_id)
) by_row_id
union all
SELECT 100004800 AS covariate_id, row_id, 1 AS covariate_value
FROM (
	SELECT DISTINCT condition_concept_id,
		cohort.subject_id AS row_id
	FROM @cohort_table cohort
	INNER JOIN @cdm_database_schema.condition_occurrence
		ON cohort.subject_id =condition_occurrence.person_id
		WHERE condition_start_date < DATEADD(DAY, 0, cohort.cohort_start_date) AND condition_start_date >= DATEADD(DAY, -365, cohort.cohort_start_date)
		AND condition_concept_id in (select ca.descendant_concept_id from @cdm_database_schema.CONCEPT_ANCESTOR ca where ca.ancestor_concept_id in (440825,4135750,4135749)) -- Upper leg
		AND cohort.cohort_definition_id IN (@cohort_id)
) by_row_id
union all
SELECT 100005800 AS covariate_id, row_id, 1 AS covariate_value
FROM (
	SELECT DISTINCT condition_concept_id,
		cohort.subject_id AS row_id
	FROM @cohort_table cohort
	INNER JOIN @cdm_database_schema.condition_occurrence
		ON cohort.subject_id =condition_occurrence.person_id
		WHERE condition_start_date < DATEADD(DAY, 0, cohort.cohort_start_date) AND condition_start_date >= DATEADD(DAY, -365, cohort.cohort_start_date)
		AND condition_concept_id in (select ca.descendant_concept_id from @cdm_database_schema.CONCEPT_ANCESTOR ca where ca.ancestor_concept_id in (4185758)) -- Lower leg
		AND cohort.cohort_definition_id IN (@cohort_id)
) by_row_id
union all
SELECT 100006800 AS covariate_id, row_id, 1 AS covariate_value
FROM (
	SELECT DISTINCT condition_concept_id,
		cohort.subject_id AS row_id
	FROM @cohort_table cohort
	INNER JOIN @cdm_database_schema.condition_occurrence
		ON cohort.subject_id =condition_occurrence.person_id
		WHERE condition_start_date < DATEADD(DAY, 0, cohort.cohort_start_date) AND condition_start_date >= DATEADD(DAY, -365, cohort.cohort_start_date)
		AND condition_concept_id in (select ca.descendant_concept_id from @cdm_database_schema.CONCEPT_ANCESTOR ca where ca.ancestor_concept_id in (442619)) -- Upper arm
		AND cohort.cohort_definition_id IN (@cohort_id)
) by_row_id
union all
SELECT 100007800 AS covariate_id, row_id, 1 AS covariate_value
FROM (
	SELECT DISTINCT condition_concept_id,
		cohort.subject_id AS row_id
	FROM @cohort_table cohort
	INNER JOIN @cdm_database_schema.condition_occurrence
		ON cohort.subject_id =condition_occurrence.person_id
		WHERE condition_start_date < DATEADD(DAY, 0, cohort.cohort_start_date) AND condition_start_date >= DATEADD(DAY, -365, cohort.cohort_start_date)
		AND condition_concept_id in (select ca.descendant_concept_id from @cdm_database_schema.CONCEPT_ANCESTOR ca where ca.ancestor_concept_id in (4278672)) -- Lower arm
		AND cohort.cohort_definition_id IN (@cohort_id)
) by_row_id
union all
SELECT 100008800 AS covariate_id, row_id, 1 AS covariate_value
FROM (
	SELECT DISTINCT condition_concept_id,
		cohort.subject_id AS row_id
	FROM @cohort_table cohort
	INNER JOIN @cdm_database_schema.condition_occurrence
		ON cohort.subject_id =condition_occurrence.person_id
		WHERE condition_start_date < DATEADD(DAY, 0, cohort.cohort_start_date) AND condition_start_date >= DATEADD(DAY, -365, cohort.cohort_start_date)
		AND condition_concept_id in (select ca.descendant_concept_id from @cdm_database_schema.CONCEPT_ANCESTOR ca where ca.ancestor_concept_id in (4218884)) -- Wrist
		AND cohort.cohort_definition_id IN (@cohort_id)
) by_row_id
union all
SELECT 100009800 AS covariate_id, row_id, 1 AS covariate_value
FROM (
	SELECT DISTINCT condition_concept_id,
		cohort.subject_id AS row_id
	FROM @cohort_table cohort
	INNER JOIN @cdm_database_schema.condition_occurrence
		ON cohort.subject_id =condition_occurrence.person_id
		WHERE condition_start_date < DATEADD(DAY, 0, cohort.cohort_start_date) AND condition_start_date >= DATEADD(DAY, -365, cohort.cohort_start_date)
		AND condition_concept_id in (select ca.descendant_concept_id from @cdm_database_schema.CONCEPT_ANCESTOR ca where ca.ancestor_concept_id in (4105127,4059173,4085552,4136841)) -- Ankle
		AND cohort.cohort_definition_id IN (@cohort_id)
) by_row_id
union all
SELECT 100010800 AS covariate_id, row_id, 1 AS covariate_value
FROM (
	SELECT DISTINCT condition_concept_id,
		cohort.subject_id AS row_id
	FROM @cohort_table cohort
	INNER JOIN @cdm_database_schema.condition_occurrence
		ON cohort.subject_id =condition_occurrence.person_id
		WHERE condition_start_date < DATEADD(DAY, 0, cohort.cohort_start_date) AND condition_start_date >= DATEADD(DAY, -365, cohort.cohort_start_date)
		AND condition_concept_id in (select ca.descendant_concept_id from @cdm_database_schema.CONCEPT_ANCESTOR ca where ca.ancestor_concept_id in (33737001)) -- Rib
		AND cohort.cohort_definition_id IN (@cohort_id)
) by_row_id
union all
SELECT 100011800 AS covariate_id, row_id, 1 AS covariate_value
FROM (
	SELECT DISTINCT condition_concept_id,
		cohort.subject_id AS row_id
	FROM @cohort_table cohort
	INNER JOIN @cdm_database_schema.condition_occurrence
		ON cohort.subject_id =condition_occurrence.person_id
		WHERE condition_start_date < DATEADD(DAY, 0, cohort.cohort_start_date) AND condition_start_date >= DATEADD(DAY, -365, cohort.cohort_start_date)
		AND condition_concept_id in (select ca.descendant_concept_id from @cdm_database_schema.CONCEPT_ANCESTOR ca where ca.ancestor_concept_id in (4071876)) -- Hand
		AND cohort.cohort_definition_id IN (@cohort_id)
) by_row_id
union all
SELECT 100012800 AS covariate_id, row_id, 1 AS covariate_value
FROM (
	SELECT DISTINCT condition_concept_id,
		cohort.subject_id AS row_id
	FROM @cohort_table cohort
	INNER JOIN @cdm_database_schema.condition_occurrence
		ON cohort.subject_id =condition_occurrence.person_id
		WHERE condition_start_date < DATEADD(DAY, 0, cohort.cohort_start_date) AND condition_start_date >= DATEADD(DAY, -365, cohort.cohort_start_date)
		AND condition_concept_id in (select ca.descendant_concept_id from @cdm_database_schema.CONCEPT_ANCESTOR ca where ca.ancestor_concept_id in (4048393)) -- Foot
		AND cohort.cohort_definition_id IN (@cohort_id)
) by_row_id
union all
SELECT 100013800 AS covariate_id, row_id, 1 AS covariate_value
FROM (
	SELECT DISTINCT condition_concept_id,
		cohort.subject_id AS row_id
	FROM @cohort_table cohort
	INNER JOIN @cdm_database_schema.condition_occurrence
		ON cohort.subject_id =condition_occurrence.person_id
		WHERE condition_start_date < DATEADD(DAY, 0, cohort.cohort_start_date) AND condition_start_date >= DATEADD(DAY, -365, cohort.cohort_start_date)
		AND condition_concept_id in (select ca.descendant_concept_id from @cdm_database_schema.CONCEPT_ANCESTOR ca where ca.ancestor_concept_id in (4177025)) -- Knee
		AND cohort.cohort_definition_id IN (@cohort_id)
) by_row_id
union all
SELECT 100014800 AS covariate_id, row_id, 1 AS covariate_value
FROM (
	SELECT DISTINCT condition_concept_id,
		cohort.subject_id AS row_id
	FROM @cohort_table cohort
	INNER JOIN @cdm_database_schema.condition_occurrence
		ON cohort.subject_id =condition_occurrence.person_id
		WHERE condition_start_date < DATEADD(DAY, 0, cohort.cohort_start_date) AND condition_start_date >= DATEADD(DAY, -365, cohort.cohort_start_date)
		AND condition_concept_id in (select ca.descendant_concept_id from @cdm_database_schema.CONCEPT_ANCESTOR ca where ca.ancestor_concept_id in (4205238,4083488)) -- Elbow
		AND cohort.cohort_definition_id IN (@cohort_id)
) by_row_id
union all
SELECT 100015800 AS covariate_id, row_id, 1 AS covariate_value
FROM (
	SELECT DISTINCT condition_concept_id,
		cohort.subject_id AS row_id
	FROM @cohort_table cohort
	INNER JOIN @cdm_database_schema.condition_occurrence
		ON cohort.subject_id =condition_occurrence.person_id
		WHERE condition_start_date < DATEADD(DAY, 0, cohort.cohort_start_date) AND condition_start_date >= DATEADD(DAY, -365, cohort.cohort_start_date)
		AND condition_concept_id in (select ca.descendant_concept_id from @cdm_database_schema.CONCEPT_ANCESTOR ca where ca.ancestor_concept_id in (4302223)) -- Skull
		AND cohort.cohort_definition_id IN (@cohort_id)
) by_row_id
union all
SELECT 100016800 AS covariate_id, row_id, 1 AS covariate_value
FROM (
	SELECT DISTINCT condition_concept_id,
		cohort.subject_id AS row_id
	FROM @cohort_table cohort
	INNER JOIN @cdm_database_schema.condition_occurrence
		ON cohort.subject_id =condition_occurrence.person_id
		WHERE condition_start_date < DATEADD(DAY, 0, cohort.cohort_start_date) AND condition_start_date >= DATEADD(DAY, -365, cohort.cohort_start_date)
		AND condition_concept_id in (select ca.descendant_concept_id from @cdm_database_schema.CONCEPT_ANCESTOR ca where ca.ancestor_concept_id in (4085248,4129393)) -- Neck
		AND cohort.cohort_definition_id IN (@cohort_id)
) by_row_id
union all
SELECT 100017800 AS covariate_id, row_id, 1 AS covariate_value
FROM (
	SELECT DISTINCT condition_concept_id,
		cohort.subject_id AS row_id
	FROM @cohort_table cohort
	INNER JOIN @cdm_database_schema.condition_occurrence
		ON cohort.subject_id =condition_occurrence.person_id
		WHERE condition_start_date < DATEADD(DAY, 0, cohort.cohort_start_date) AND condition_start_date >= DATEADD(DAY, -365, cohort.cohort_start_date)
		AND condition_concept_id in (4003483,4069306,4067768) -- Unspecified
		AND cohort.cohort_definition_id IN (@cohort_id)
) by_row_id
union all
SELECT 100018800 AS covariate_id, row_id, 1 AS covariate_value
FROM (
	SELECT DISTINCT condition_concept_id,
		cohort.subject_id AS row_id
	FROM @cohort_table cohort
	INNER JOIN @cdm_database_schema.condition_occurrence
		ON cohort.subject_id =condition_occurrence.person_id
		WHERE condition_start_date < DATEADD(DAY, 0, cohort.cohort_start_date) AND condition_start_date >= DATEADD(DAY, -365, cohort.cohort_start_date)
		AND condition_concept_id in (select ca.descendant_concept_id from @cdm_database_schema.CONCEPT_ANCESTOR ca where ca.ancestor_concept_id in (75053)) -- ALL
		AND cohort.cohort_definition_id IN (@cohort_id)
) by_row_id
union all
SELECT 100000801 AS covariate_id, row_id, 1 AS covariate_value
FROM (
	SELECT DISTINCT condition_concept_id,
		cohort.subject_id AS row_id
	FROM @cohort_table cohort
	INNER JOIN @cdm_database_schema.condition_occurrence
		ON cohort.subject_id =condition_occurrence.person_id
		WHERE condition_start_date < DATEADD(DAY, 0, cohort.cohort_start_date) AND condition_start_date >= DATEADD(DAY, -30, cohort.cohort_start_date)
		AND condition_concept_id in (select ca.descendant_concept_id from @cdm_database_schema.CONCEPT_ANCESTOR ca where ca.ancestor_concept_id in (4129394,4053828)) -- vertebral
		AND cohort.cohort_definition_id IN (@cohort_id)
) by_row_id
union all
SELECT 100001801 AS covariate_id, row_id, 1 AS covariate_value
FROM (
	SELECT DISTINCT condition_concept_id,
		cohort.subject_id AS row_id
	FROM @cohort_table cohort
	INNER JOIN @cdm_database_schema.condition_occurrence
		ON cohort.subject_id =condition_occurrence.person_id
		WHERE condition_start_date < DATEADD(DAY, 0, cohort.cohort_start_date) AND condition_start_date >= DATEADD(DAY, -30, cohort.cohort_start_date)
		AND condition_concept_id in (select ca.descendant_concept_id from @cdm_database_schema.CONCEPT_ANCESTOR ca where ca.ancestor_concept_id in (4138412,4230399,45767037)) -- Hip (Proximal femur)
		AND cohort.cohort_definition_id IN (@cohort_id)
) by_row_id
union all
SELECT 100002801 AS covariate_id, row_id, 1 AS covariate_value
FROM (
	SELECT DISTINCT condition_concept_id,
		cohort.subject_id AS row_id
	FROM @cohort_table cohort
	INNER JOIN @cdm_database_schema.condition_occurrence
		ON cohort.subject_id =condition_occurrence.person_id
		WHERE condition_start_date < DATEADD(DAY, 0, cohort.cohort_start_date) AND condition_start_date >= DATEADD(DAY, -30, cohort.cohort_start_date)
		AND condition_concept_id in (select ca.descendant_concept_id from @cdm_database_schema.CONCEPT_ANCESTOR ca where ca.ancestor_concept_id in (4300192)) -- Pelvis
		AND cohort.cohort_definition_id IN (@cohort_id)
) by_row_id
union all
SELECT 100003801 AS covariate_id, row_id, 1 AS covariate_value
FROM (
	SELECT DISTINCT condition_concept_id,
		cohort.subject_id AS row_id
	FROM @cohort_table cohort
	INNER JOIN @cdm_database_schema.condition_occurrence
		ON cohort.subject_id =condition_occurrence.person_id
		WHERE condition_start_date < DATEADD(DAY, 0, cohort.cohort_start_date) AND condition_start_date >= DATEADD(DAY, -30, cohort.cohort_start_date)
		AND condition_concept_id in (select ca.descendant_concept_id from @cdm_database_schema.CONCEPT_ANCESTOR ca where ca.ancestor_concept_id in (46270317,4302740)) -- Shoulder
		AND cohort.cohort_definition_id IN (@cohort_id)
) by_row_id
union all
SELECT 100004801 AS covariate_id, row_id, 1 AS covariate_value
FROM (
	SELECT DISTINCT condition_concept_id,
		cohort.subject_id AS row_id
	FROM @cohort_table cohort
	INNER JOIN @cdm_database_schema.condition_occurrence
		ON cohort.subject_id =condition_occurrence.person_id
		WHERE condition_start_date < DATEADD(DAY, 0, cohort.cohort_start_date) AND condition_start_date >= DATEADD(DAY, -30, cohort.cohort_start_date)
		AND condition_concept_id in (select ca.descendant_concept_id from @cdm_database_schema.CONCEPT_ANCESTOR ca where ca.ancestor_concept_id in (440825,4135750,4135749)) -- Upper leg
		AND cohort.cohort_definition_id IN (@cohort_id)
) by_row_id
union all
SELECT 100005801 AS covariate_id, row_id, 1 AS covariate_value
FROM (
	SELECT DISTINCT condition_concept_id,
		cohort.subject_id AS row_id
	FROM @cohort_table cohort
	INNER JOIN @cdm_database_schema.condition_occurrence
		ON cohort.subject_id =condition_occurrence.person_id
		WHERE condition_start_date < DATEADD(DAY, 0, cohort.cohort_start_date) AND condition_start_date >= DATEADD(DAY, -30, cohort.cohort_start_date)
		AND condition_concept_id in (select ca.descendant_concept_id from @cdm_database_schema.CONCEPT_ANCESTOR ca where ca.ancestor_concept_id in (4185758)) -- Lower leg
		AND cohort.cohort_definition_id IN (@cohort_id)
) by_row_id
union all
SELECT 100006801 AS covariate_id, row_id, 1 AS covariate_value
FROM (
	SELECT DISTINCT condition_concept_id,
		cohort.subject_id AS row_id
	FROM @cohort_table cohort
	INNER JOIN @cdm_database_schema.condition_occurrence
		ON cohort.subject_id =condition_occurrence.person_id
		WHERE condition_start_date < DATEADD(DAY, 0, cohort.cohort_start_date) AND condition_start_date >= DATEADD(DAY, -30, cohort.cohort_start_date)
		AND condition_concept_id in (select ca.descendant_concept_id from @cdm_database_schema.CONCEPT_ANCESTOR ca where ca.ancestor_concept_id in (442619)) -- Upper arm
		AND cohort.cohort_definition_id IN (@cohort_id)
) by_row_id
union all
SELECT 100007801 AS covariate_id, row_id, 1 AS covariate_value
FROM (
	SELECT DISTINCT condition_concept_id,
		cohort.subject_id AS row_id
	FROM @cohort_table cohort
	INNER JOIN @cdm_database_schema.condition_occurrence
		ON cohort.subject_id =condition_occurrence.person_id
		WHERE condition_start_date < DATEADD(DAY, 0, cohort.cohort_start_date) AND condition_start_date >= DATEADD(DAY, -30, cohort.cohort_start_date)
		AND condition_concept_id in (select ca.descendant_concept_id from @cdm_database_schema.CONCEPT_ANCESTOR ca where ca.ancestor_concept_id in (4278672)) -- Lower arm
		AND cohort.cohort_definition_id IN (@cohort_id)
) by_row_id
union all
SELECT 100008801 AS covariate_id, row_id, 1 AS covariate_value
FROM (
	SELECT DISTINCT condition_concept_id,
		cohort.subject_id AS row_id
	FROM @cohort_table cohort
	INNER JOIN @cdm_database_schema.condition_occurrence
		ON cohort.subject_id =condition_occurrence.person_id
		WHERE condition_start_date < DATEADD(DAY, 0, cohort.cohort_start_date) AND condition_start_date >= DATEADD(DAY, -30, cohort.cohort_start_date)
		AND condition_concept_id in (select ca.descendant_concept_id from @cdm_database_schema.CONCEPT_ANCESTOR ca where ca.ancestor_concept_id in (4218884)) -- Wrist
		AND cohort.cohort_definition_id IN (@cohort_id)
) by_row_id
union all
SELECT 100009801 AS covariate_id, row_id, 1 AS covariate_value
FROM (
	SELECT DISTINCT condition_concept_id,
		cohort.subject_id AS row_id
	FROM @cohort_table cohort
	INNER JOIN @cdm_database_schema.condition_occurrence
		ON cohort.subject_id =condition_occurrence.person_id
		WHERE condition_start_date < DATEADD(DAY, 0, cohort.cohort_start_date) AND condition_start_date >= DATEADD(DAY, -30, cohort.cohort_start_date)
		AND condition_concept_id in (select ca.descendant_concept_id from @cdm_database_schema.CONCEPT_ANCESTOR ca where ca.ancestor_concept_id in (4105127,4059173,4085552,4136841)) -- Ankle
		AND cohort.cohort_definition_id IN (@cohort_id)
) by_row_id
union all
SELECT 100010801 AS covariate_id, row_id, 1 AS covariate_value
FROM (
	SELECT DISTINCT condition_concept_id,
		cohort.subject_id AS row_id
	FROM @cohort_table cohort
	INNER JOIN @cdm_database_schema.condition_occurrence
		ON cohort.subject_id =condition_occurrence.person_id
		WHERE condition_start_date < DATEADD(DAY, 0, cohort.cohort_start_date) AND condition_start_date >= DATEADD(DAY, -30, cohort.cohort_start_date)
		AND condition_concept_id in (select ca.descendant_concept_id from @cdm_database_schema.CONCEPT_ANCESTOR ca where ca.ancestor_concept_id in (33737001)) -- Rib
		AND cohort.cohort_definition_id IN (@cohort_id)
) by_row_id
union all
SELECT 100011801 AS covariate_id, row_id, 1 AS covariate_value
FROM (
	SELECT DISTINCT condition_concept_id,
		cohort.subject_id AS row_id
	FROM @cohort_table cohort
	INNER JOIN @cdm_database_schema.condition_occurrence
		ON cohort.subject_id =condition_occurrence.person_id
		WHERE condition_start_date < DATEADD(DAY, 0, cohort.cohort_start_date) AND condition_start_date >= DATEADD(DAY, -30, cohort.cohort_start_date)
		AND condition_concept_id in (select ca.descendant_concept_id from @cdm_database_schema.CONCEPT_ANCESTOR ca where ca.ancestor_concept_id in (4071876)) -- Hand
		AND cohort.cohort_definition_id IN (@cohort_id)
) by_row_id
union all
SELECT 100012801 AS covariate_id, row_id, 1 AS covariate_value
FROM (
	SELECT DISTINCT condition_concept_id,
		cohort.subject_id AS row_id
	FROM @cohort_table cohort
	INNER JOIN @cdm_database_schema.condition_occurrence
		ON cohort.subject_id =condition_occurrence.person_id
		WHERE condition_start_date < DATEADD(DAY, 0, cohort.cohort_start_date) AND condition_start_date >= DATEADD(DAY, -30, cohort.cohort_start_date)
		AND condition_concept_id in (select ca.descendant_concept_id from @cdm_database_schema.CONCEPT_ANCESTOR ca where ca.ancestor_concept_id in (4048393)) -- Foot
		AND cohort.cohort_definition_id IN (@cohort_id)
) by_row_id
union all
SELECT 100013801 AS covariate_id, row_id, 1 AS covariate_value
FROM (
	SELECT DISTINCT condition_concept_id,
		cohort.subject_id AS row_id
	FROM @cohort_table cohort
	INNER JOIN @cdm_database_schema.condition_occurrence
		ON cohort.subject_id =condition_occurrence.person_id
		WHERE condition_start_date < DATEADD(DAY, 0, cohort.cohort_start_date) AND condition_start_date >= DATEADD(DAY, -30, cohort.cohort_start_date)
		AND condition_concept_id in (select ca.descendant_concept_id from @cdm_database_schema.CONCEPT_ANCESTOR ca where ca.ancestor_concept_id in (4177025)) -- Knee
		AND cohort.cohort_definition_id IN (@cohort_id)
) by_row_id
union all
SELECT 100014801 AS covariate_id, row_id, 1 AS covariate_value
FROM (
	SELECT DISTINCT condition_concept_id,
		cohort.subject_id AS row_id
	FROM @cohort_table cohort
	INNER JOIN @cdm_database_schema.condition_occurrence
		ON cohort.subject_id =condition_occurrence.person_id
		WHERE condition_start_date < DATEADD(DAY, 0, cohort.cohort_start_date) AND condition_start_date >= DATEADD(DAY, -30, cohort.cohort_start_date)
		AND condition_concept_id in (select ca.descendant_concept_id from @cdm_database_schema.CONCEPT_ANCESTOR ca where ca.ancestor_concept_id in (4205238,4083488)) -- Elbow
		AND cohort.cohort_definition_id IN (@cohort_id)
) by_row_id
union all
SELECT 100015801 AS covariate_id, row_id, 1 AS covariate_value
FROM (
	SELECT DISTINCT condition_concept_id,
		cohort.subject_id AS row_id
	FROM @cohort_table cohort
	INNER JOIN @cdm_database_schema.condition_occurrence
		ON cohort.subject_id =condition_occurrence.person_id
		WHERE condition_start_date < DATEADD(DAY, 0, cohort.cohort_start_date) AND condition_start_date >= DATEADD(DAY, -30, cohort.cohort_start_date)
		AND condition_concept_id in (select ca.descendant_concept_id from @cdm_database_schema.CONCEPT_ANCESTOR ca where ca.ancestor_concept_id in (4302223)) -- Skull
		AND cohort.cohort_definition_id IN (@cohort_id)
) by_row_id
union all
SELECT 100016801 AS covariate_id, row_id, 1 AS covariate_value
FROM (
	SELECT DISTINCT condition_concept_id,
		cohort.subject_id AS row_id
	FROM @cohort_table cohort
	INNER JOIN @cdm_database_schema.condition_occurrence
		ON cohort.subject_id =condition_occurrence.person_id
		WHERE condition_start_date < DATEADD(DAY, 0, cohort.cohort_start_date) AND condition_start_date >= DATEADD(DAY, -30, cohort.cohort_start_date)
		AND condition_concept_id in (select ca.descendant_concept_id from @cdm_database_schema.CONCEPT_ANCESTOR ca where ca.ancestor_concept_id in (4085248,4129393)) -- Neck
		AND cohort.cohort_definition_id IN (@cohort_id)
) by_row_id
union all
SELECT 100017801 AS covariate_id, row_id, 1 AS covariate_value
FROM (
	SELECT DISTINCT condition_concept_id,
		cohort.subject_id AS row_id
	FROM @cohort_table cohort
	INNER JOIN @cdm_database_schema.condition_occurrence
		ON cohort.subject_id =condition_occurrence.person_id
		WHERE condition_start_date < DATEADD(DAY, 0, cohort.cohort_start_date) AND condition_start_date >= DATEADD(DAY, -30, cohort.cohort_start_date)
		AND condition_concept_id in (4003483,4069306,4067768) -- Unspecified
		AND cohort.cohort_definition_id IN (@cohort_id)
) by_row_id
union all
SELECT 100018801 AS covariate_id, row_id, 1 AS covariate_value
FROM (
	SELECT DISTINCT condition_concept_id,
		cohort.subject_id AS row_id
	FROM @cohort_table cohort
	INNER JOIN @cdm_database_schema.condition_occurrence
		ON cohort.subject_id =condition_occurrence.person_id
		WHERE condition_start_date < DATEADD(DAY, 0, cohort.cohort_start_date) AND condition_start_date >= DATEADD(DAY, -30, cohort.cohort_start_date)
		AND condition_concept_id in (select ca.descendant_concept_id from @cdm_database_schema.CONCEPT_ANCESTOR ca where ca.ancestor_concept_id in (75053)) -- ALL
		AND cohort.cohort_definition_id IN (@cohort_id)
) by_row_id
union all
SELECT 200000802 AS covariate_id, subject_id as row_id, 1 AS covariate_value
from (
select cohort.*, min(drug_exposure_start_date) as drug_start_date, max(drug_exposure_end_date) as drug_end_date FROM @cohort_table cohort
INNER JOIN @cdm_database_schema.DRUG_EXPOSURE drug
ON cohort.subject_id =drug.person_id
where drug_concept_id in (
select descendant_concept_id from @cdm_database_schema.CONCEPT_ANCESTOR where ancestor_concept_id in (21602737,21602730,21602734,21602732,21602729))
AND drug_exposure_start_date < DATEADD(DAY, 0, cohort.cohort_start_date) AND drug_exposure_end_date >= DATEADD(DAY, -365, cohort.cohort_start_date)
AND cohort.cohort_definition_id IN (@cohort_id)
group by cohort_definition_id, subject_id, cohort_start_date, cohort_end_date
) drug
where datediff(day, drug_start_date, drug_end_date)>=90

;
