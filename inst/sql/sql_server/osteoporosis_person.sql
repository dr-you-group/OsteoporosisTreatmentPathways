with osteoporosis as (
	SELECT 10 as codeset_id, c.concept_id FROM (select distinct I.concept_id FROM
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
	) C
), person_osteoporosis as (
	select co.person_id, p.year_of_birth, min(condition_start_date) as condition_start_date 
	from @cdm_database_schema.condition_occurrence co, osteoporosis o, @cdm_database_schema.person p
	where co.condition_concept_id = o.concept_id and p.person_id = co.person_id and p.gender_concept_id = 8532
	group by co.person_id, p.year_of_birth
)
select *, year(condition_start_date)-year_of_birth as age from person_osteoporosis;