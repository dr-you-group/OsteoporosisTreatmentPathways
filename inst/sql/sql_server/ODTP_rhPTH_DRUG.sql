CREATE TABLE #Codesets (
  codeset_id int NOT NULL,
  concept_id bigint NOT NULL
)
;

INSERT INTO #Codesets (codeset_id, concept_id)
SELECT 2 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
( 
   select concept_id from @vocabulary_database_schema.CONCEPT where concept_id in (21602783)
UNION  select c.concept_id
  from @vocabulary_database_schema.CONCEPT c
  join @vocabulary_database_schema.CONCEPT_ANCESTOR ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (21602783)
  and c.invalid_reason is null
) I
) C
;

-- rhPTH
DELETE FROM @target_database_schema.@target_cohort_table where cohort_definition_id = @target_cohort_id;
INSERT INTO @target_database_schema.@target_cohort_table (cohort_definition_id, subject_id, drug_concept_id, cohort_start_date, cohort_end_date, days_supply, duration)
select @target_cohort_id as cohort_definition_id, d.person_id, d.drug_concept_id, d.drug_exposure_start_date, d.drug_exposure_end_date, d.days_supply, d.duration
from (
  select de.*, 7 as duration
  FROM @cdm_database_schema.DRUG_EXPOSURE de
  JOIN #Codesets codesets on ((de.drug_concept_id = codesets.concept_id and codesets.codeset_id = 2)) 
  WHERE days_supply >= 1
  ) d;


TRUNCATE TABLE #Codesets;
DROP TABLE #Codesets;
