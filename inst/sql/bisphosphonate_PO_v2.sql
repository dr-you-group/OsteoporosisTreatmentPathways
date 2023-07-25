-- BP PO
DELETE FROM @target_database_schema.@target_cohort_table where cohort_definition_id = @target_cohort_id;
INSERT INTO @target_database_schema.@target_cohort_table (cohort_definition_id, subject_id, cohort_start_date, cohort_end_date)
select @target_cohort_id as cohort_definition_id, d.person_id, d.drug_exposure_start_date, d.drug_exposure_end_date 
from (
  select de.* 
  FROM CDM_v531_YUHS.CDM.DRUG_EXPOSURE de
  where de.drug_concept_id in (2056263,42971440,42971625,42971627,21154250,40174364,42943771,40174486,40174487) and days_supply >= 1
union all
  select de.* 
  FROM CDM_v531_YUHS.CDM.DRUG_EXPOSURE de
  where de.drug_concept_id in (44033466,42952382,44033467,42952318,42952430,40173609,40173612,40173613,42971533,42971564,42971566,42943685,42943686,42943638,40228826,40174494,40174495)
  and days_supply >= 4
union all
  select de.* 
  FROM CDM_v531_YUHS.CDM.DRUG_EXPOSURE de
  where de.drug_concept_id in (42958475,42958476,40173590,40173591,40173602,40174498,40174499)
  and days_supply >= 28
 ) d;