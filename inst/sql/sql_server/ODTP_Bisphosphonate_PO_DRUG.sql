CREATE TABLE #Codesets (
  codeset_id int NOT NULL,
  concept_id bigint NOT NULL
)
;

INSERT INTO #Codesets (codeset_id, concept_id)
SELECT 0 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
(
  select concept_id from @vocabulary_database_schema.CONCEPT where concept_id in (80502,4001645,40401864)
UNION  select c.concept_id
  from @vocabulary_database_schema.CONCEPT c
  join @vocabulary_database_schema.CONCEPT_ANCESTOR ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (80502)
  and c.invalid_reason is null

) I
LEFT JOIN
(
  select concept_id from @vocabulary_database_schema.CONCEPT where concept_id in (42538151,4002134,40480160,45766159)
UNION  select c.concept_id
  from @vocabulary_database_schema.CONCEPT c
  join @vocabulary_database_schema.CONCEPT_ANCESTOR ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (42538151,4002134,40480160,45766159)
  and c.invalid_reason is null

) E ON I.concept_id = E.concept_id
WHERE E.concept_id is null
) C UNION ALL
SELECT 1 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
(
  select concept_id from @vocabulary_database_schema.CONCEPT where concept_id in (40173589,40173601,40174497)
UNION  select c.concept_id
  from @vocabulary_database_schema.CONCEPT c
  join @vocabulary_database_schema.CONCEPT_ANCESTOR ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (40173589,40173601,40174497)
  and c.invalid_reason is null

) I
) C UNION ALL
SELECT 2 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
(
  select concept_id from @vocabulary_database_schema.CONCEPT where concept_id in (40173605,40174493,19123238,21104349)
  UNION
  select c.concept_id
  from @vocabulary_database_schema.CONCEPT c
  join @vocabulary_database_schema.CONCEPT_ANCESTOR ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (40173605,40174493,19123238,21104349)
  and c.invalid_reason is null

) I
) C UNION ALL
SELECT 3 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
(
  select concept_id from @vocabulary_database_schema.CONCEPT where concept_id in (40174363,40174485)
UNION  select c.concept_id
  from @vocabulary_database_schema.CONCEPT c
  join @vocabulary_database_schema.CONCEPT_ANCESTOR ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (40174363,40174485)
  and c.invalid_reason is null

) I
) C UNION ALL
SELECT 4 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
(
  select concept_id from @vocabulary_database_schema.CONCEPT where concept_id in (40173589,40173601,40174497,40174363,40174485,40173605,40174493)
UNION  select c.concept_id
  from @vocabulary_database_schema.CONCEPT c
  join @vocabulary_database_schema.CONCEPT_ANCESTOR ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (40173589,40173601,40174497,40174363,40174485,40173605,40174493)
  and c.invalid_reason is null

) I
) C
;

-- BP PO
DELETE FROM @target_database_schema.@target_cohort_table where cohort_definition_id = @target_cohort_id;
INSERT INTO @target_database_schema.@target_cohort_table (cohort_definition_id, subject_id, drug_concept_id, cohort_start_date, cohort_end_date, days_supply, duration)
select @target_cohort_id as cohort_definition_id, d.person_id, d.drug_concept_id, d.drug_exposure_start_date, d.drug_exposure_end_date, d.days_supply, d.duration
from (
  select de.person_id, de.drug_exposure_id, de.drug_exposure_start_date, de.drug_exposure_end_date, de.drug_concept_id, datediff(day, de.drug_exposure_start_date, de.drug_exposure_end_date)+1 as 'days_supply', 1 as 'duration'
  FROM @cdm_database_schema.DRUG_EXPOSURE de
  JOIN #Codesets codesets on ((de.drug_concept_id = codesets.concept_id and codesets.codeset_id = 1))
  WHERE datediff(day, de.drug_exposure_start_date, de.drug_exposure_end_date)+1 >= 28
union all
  select de.person_id, de.drug_exposure_id, de.drug_exposure_start_date, de.drug_exposure_end_date, de.drug_concept_id, datediff(day, de.drug_exposure_start_date, de.drug_exposure_end_date)+1 as 'days_supply', 7 as 'duration'
  FROM @cdm_database_schema.DRUG_EXPOSURE de
  JOIN #Codesets codesets on ((de.drug_concept_id = codesets.concept_id and codesets.codeset_id = 2))
  WHERE datediff(day, de.drug_exposure_start_date, de.drug_exposure_end_date)+1 >= 4
union all
  select de.person_id, de.drug_exposure_id, de.drug_exposure_start_date, de.drug_exposure_end_date, de.drug_concept_id, datediff(day, de.drug_exposure_start_date, de.drug_exposure_end_date)+1 as 'days_supply', 30 as 'duration'
  FROM @cdm_database_schema.DRUG_EXPOSURE de
  JOIN #Codesets codesets on ((de.drug_concept_id = codesets.concept_id and codesets.codeset_id = 3))
  WHERE datediff(day, de.drug_exposure_start_date, de.drug_exposure_end_date)+1 >= 1
  ) d;

TRUNCATE TABLE #Codesets;
DROP TABLE #Codesets;
