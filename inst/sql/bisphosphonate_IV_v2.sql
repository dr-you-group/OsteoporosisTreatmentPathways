-- BP IV 
DELETE FROM @target_database_schema.@target_cohort_table where cohort_definition_id = @target_cohort_id;
INSERT INTO @target_database_schema.@target_cohort_table (cohort_definition_id, subject_id, cohort_start_date, cohort_end_date)
select @target_cohort_id as cohort_definition_id, d.person_id, d.drug_exposure_start_date, d.drug_exposure_end_date 
FROM @cdm_database_schema.DRUG_EXPOSURE d 
where d.drug_concept_id in (21118274,42919399,19126450,21176092,40173414) and days_supply >=1;