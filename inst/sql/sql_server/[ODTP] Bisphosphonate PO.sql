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
UNION  select c.concept_id
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
  select concept_id from @vocabulary_database_schema.CONCEPT where concept_id in (40173589,40173601,40174497,40174363,40174485,40173605,40174493,19123238,21104349)
UNION  select c.concept_id
  from @vocabulary_database_schema.CONCEPT c
  join @vocabulary_database_schema.CONCEPT_ANCESTOR ca on c.concept_id = ca.descendant_concept_id
  and ca.ancestor_concept_id in (40173589,40173601,40174497,40174363,40174485,40173605,40174493,19123238,21104349)
  and c.invalid_reason is null

) I
) C
;

with primary_events (event_id, person_id, start_date, end_date, op_start_date, op_end_date, visit_occurrence_id) as
(
-- Begin Primary Events
select P.ordinal as event_id, P.person_id, P.start_date, P.end_date, op_start_date, op_end_date, cast(P.visit_occurrence_id as bigint) as visit_occurrence_id
FROM
(
  select E.person_id, E.start_date, E.end_date,
         row_number() OVER (PARTITION BY E.person_id ORDER BY E.sort_date ASC) ordinal,
         OP.observation_period_start_date as op_start_date, OP.observation_period_end_date as op_end_date, cast(E.visit_occurrence_id as bigint) as visit_occurrence_id
  FROM
  (
  -- Begin Drug Exposure Criteria
select C.person_id, C.drug_exposure_id as event_id, C.drug_exposure_start_date as start_date,
       COALESCE(C.drug_exposure_end_date, DATEADD(day, 1, C.drug_exposure_start_date)) as end_date, C.drug_concept_id as TARGET_CONCEPT_ID, C.visit_occurrence_id,
       C.drug_exposure_start_date as sort_date
from
(
  select de.*
  FROM @cdm_database_schema.DRUG_EXPOSURE de
  where (drug_source_value like '%644500230%'
	or drug_source_value like '%657200460%'
	or drug_source_value like '%655500710%'
	or drug_source_value like '%A02003951%'
	or drug_source_value like '%A09703641%'
	or drug_source_value like '%A50703171%'
	or drug_source_value like '%A78800531%'
	or drug_source_value like '%E09060741%'
	or drug_source_value like '%657200450%'
	or drug_source_value like '%644500240%'
	or drug_source_value like '%A02003941%'
	or drug_source_value like '%A09703631%'
	or drug_source_value like '%642103130%'
	or drug_source_value like '%643304750%'
	or drug_source_value like '%652100900%'
	or drug_source_value like '%B07404671%')
) C

WHERE C.days_supply >= 28
-- End Drug Exposure Criteria

UNION ALL
-- Begin Drug Exposure Criteria
select C.person_id, C.drug_exposure_id as event_id, C.drug_exposure_start_date as start_date,
       COALESCE(C.drug_exposure_end_date, DATEADD(day, 1, C.drug_exposure_start_date)) as end_date, C.drug_concept_id as TARGET_CONCEPT_ID, C.visit_occurrence_id,
       C.drug_exposure_start_date as sort_date
from
(
  select de.*
  FROM @cdm_database_schema.DRUG_EXPOSURE de
  where (drug_source_value like '%643304010%'
	or drug_source_value like '%649804050%'
	or drug_source_value like '%647303660%'
	or drug_source_value like '%647500030%'
	or drug_source_value like '%661306120%'
	or drug_source_value like '%52700280%'
	or drug_source_value like '%654201280%'
	or drug_source_value like '%653403041%'
	or drug_source_value like '%643501790%'
	or drug_source_value like '%655500570%'
	or drug_source_value like '%668900380%'
	or drug_source_value like '%697100010%'
	or drug_source_value like '%53600910%'
	or drug_source_value like '%652604800%'
	or drug_source_value like '%644806040%'
	or drug_source_value like '%645902160%'
	or drug_source_value like '%693902310%'
	or drug_source_value like '%653404511%'
	or drug_source_value like '%670600660%'
	or drug_source_value like '%640900650%'
	or drug_source_value like '%653000980%'
	or drug_source_value like '%654701010%'
	or drug_source_value like '%679800310%'
	or drug_source_value like '%649402690%'
	or drug_source_value like '%661901370%'
	or drug_source_value like '%646800300%'
	or drug_source_value like '%A03805801%'
	or drug_source_value like '%645202710%'
	or drug_source_value like '%641600770%'
	or drug_source_value like '%650204530%'
	or drug_source_value like '%648504020%'
	or drug_source_value like '%643701650%'
	or drug_source_value like '%652300460%'
	or drug_source_value like '%649605220%'
	or drug_source_value like '%670101390%'
	or drug_source_value like '%683601450%'
	or drug_source_value like '%690300790%'
	or drug_source_value like '%641802230%'
	or drug_source_value like '%621800930%'
	or drug_source_value like '%649501910%'
	or drug_source_value like '%654301780%'
	or drug_source_value like '%694000310%'
	or drug_source_value like '%655402100%'
	or drug_source_value like '%689101020%'
	or drug_source_value like '%669604920%'
	or drug_source_value like '%643200650%'
	or drug_source_value like '%693201130%'
	or drug_source_value like '%623005820%'
	or drug_source_value like '%663602830%'
	or drug_source_value like '%671803390%'
	or drug_source_value like '%625800450%'
	or drug_source_value like '%669902660%'
	or drug_source_value like '%646200230%'
	or drug_source_value like '%647205470%'
	or drug_source_value like '%664601020%'
	or drug_source_value like '%647801610%'
	or drug_source_value like '%644000520%'
	or drug_source_value like '%648200960%'
	or drug_source_value like '%665000690%'
	or drug_source_value like '%643105030%'
	or drug_source_value like '%650301160%'
	or drug_source_value like '%699801590%'
	or drug_source_value like '%657501210%'
	or drug_source_value like '%658600720%'
	or drug_source_value like '%657302000%'
	or drug_source_value like '%658106500%'
	or drug_source_value like '%655601330%'
	or drug_source_value like '%A00306591%'
	or drug_source_value like '%A01207491%'
	or drug_source_value like '%A01306731%'
	or drug_source_value like '%A01402391%'
	or drug_source_value like '%A02704791%'
	or drug_source_value like '%A03006081%'
	or drug_source_value like '%A03505501%'
	or drug_source_value like '%A03805801%'
	or drug_source_value like '%A04304361%'
	or drug_source_value like '%A05404911%'
	or drug_source_value like '%A05607921%'
	or drug_source_value like '%A06502401%'
	or drug_source_value like '%A06653831%'
	or drug_source_value like '%A06703931%'
	or drug_source_value like '%A07104681%'
	or drug_source_value like '%A08504471%'
	or drug_source_value like '%A10004041%'
	or drug_source_value like '%A10703811%'
	or drug_source_value like '%A11103910%'
	or drug_source_value like '%A11204211%'
	or drug_source_value like '%A12202661%'
	or drug_source_value like '%A12603911%'
	or drug_source_value like '%A12703451%'
	or drug_source_value like '%A12804371%'
	or drug_source_value like '%A13302401%'
	or drug_source_value like '%A15204811%'
	or drug_source_value like '%A15603511%'
	or drug_source_value like '%A16604351%'
	or drug_source_value like '%A17001211%'
	or drug_source_value like '%A19203331%'
	or drug_source_value like '%A20602781%'
	or drug_source_value like '%A21404141%'
	or drug_source_value like '%A22401711%'
	or drug_source_value like '%A22607041%'
	or drug_source_value like '%A23503391%'
	or drug_source_value like '%A25005321%'
	or drug_source_value like '%A25803451%'
	or drug_source_value like '%A26401251%'
	or drug_source_value like '%A29503531%'
	or drug_source_value like '%A31805061%'
	or drug_source_value like '%A32202661%'
	or drug_source_value like '%A33203101%'
	or drug_source_value like '%A34003321%'
	or drug_source_value like '%A35104451%'
	or drug_source_value like '%A36705671%'
	or drug_source_value like '%A42900481%'
	or drug_source_value like '%A45900521%'
	or drug_source_value like '%A50703461%'
	or drug_source_value like '%A59500071%'
	or drug_source_value like '%A59500091%'
	or drug_source_value like '%A60300241%'
	or drug_source_value like '%A60603021%'
	or drug_source_value like '%A62755371%'
	or drug_source_value like '%A79100531%'
	or drug_source_value like '%A82800181%'
	or drug_source_value like '%E02670321%'
	or drug_source_value like '%622801280%'
	or drug_source_value like '%623006470%'
	or drug_source_value like '%625200160%'
	or drug_source_value like '%629701920%'
	or drug_source_value like '%640004850%'
	or drug_source_value like '%640005790%'
	or drug_source_value like '%640902430%'
	or drug_source_value like '%641606500%'
	or drug_source_value like '%641800230%'
	or drug_source_value like '%641904630%'
	or drug_source_value like '%642103120%'
	or drug_source_value like '%642103130%'
	or drug_source_value like '%652104500%'
	or drug_source_value like '%652100910%'
	or drug_source_value like '%642202580%'
	or drug_source_value like '%642303060%'
	or drug_source_value like '%642401990%'
	or drug_source_value like '%642505320%'
	or drug_source_value like '%642704290%'
	or drug_source_value like '%642801860%'
	or drug_source_value like '%642902870%'
	or drug_source_value like '%643105180%'
	or drug_source_value like '%643504110%'
	or drug_source_value like '%643902070%'
	or drug_source_value like '%644306200%'
	or drug_source_value like '%644702930%'
	or drug_source_value like '%644803240%'
	or drug_source_value like '%644912320%'
	or drug_source_value like '%645102850%'
	or drug_source_value like '%645207310%'
	or drug_source_value like '%645302340%'
	or drug_source_value like '%645402540%'
	or drug_source_value like '%645602340%'
	or drug_source_value like '%645701450%'
	or drug_source_value like '%645903560%'
	or drug_source_value like '%646002650%'
	or drug_source_value like '%646202490%'
	or drug_source_value like '%648102550%'
	or drug_source_value like '%648203400%'
	or drug_source_value like '%648504750%'
	or drug_source_value like '%648602160%'
	or drug_source_value like '%649001490%'
	or drug_source_value like '%649101530%'
	or drug_source_value like '%649702500%'
	or drug_source_value like '%649804340%'
	or drug_source_value like '%650202660%'
	or drug_source_value like '%650301780%'
	or drug_source_value like '%651600810%'
	or drug_source_value like '%651804950%'
	or drug_source_value like '%651903370%'
	or drug_source_value like '%652601680%'
	or drug_source_value like '%653004400%'
	or drug_source_value like '%653102070%'
	or drug_source_value like '%653401610%'
	or drug_source_value like '%654303670%'
	or drug_source_value like '%655200060%'
	or drug_source_value like '%657201250%'
	or drug_source_value like '%657305310%'
	or drug_source_value like '%657803180%'
	or drug_source_value like '%658001880%'
	or drug_source_value like '%658602100%'
	or drug_source_value like '%660702540%'
	or drug_source_value like '%661602550%'
	or drug_source_value like '%661902130%'
	or drug_source_value like '%662502120%'
	or drug_source_value like '%668400320%'
	or drug_source_value like '%669802260%'
	or drug_source_value like '%670001120%'
	or drug_source_value like '%670302210%'
	or drug_source_value like '%670400980%'
	or drug_source_value like '%670604180%'
	or drug_source_value like '%671804160%'
	or drug_source_value like '%674400910%'
	or drug_source_value like '%684500260%'
	or drug_source_value like '%689001100%'
	or drug_source_value like '%690302340%'
	or drug_source_value like '%693201140%'
	or drug_source_value like '%693901000%'
	or drug_source_value like '%694204750%'
	or drug_source_value like '%697100030%'
	or drug_source_value like '%698000980%'
	or drug_source_value like '%A07705281%'
	or drug_source_value like '%A12804941%'
	or drug_source_value like '%A00358501%'
	or drug_source_value like '%A00702121%'
	or drug_source_value like '%A00803151%'
	or drug_source_value like '%A01208931%'
	or drug_source_value like '%A01307341%'
	or drug_source_value like '%A01403031%'
	or drug_source_value like '%A01559741%'
	or drug_source_value like '%A02108051%'
	or drug_source_value like '%A02352001%'
	or drug_source_value like '%A02507671%'
	or drug_source_value like '%A02706171%'
	or drug_source_value like '%A03405441%'
	or drug_source_value like '%A03603021%'
	or drug_source_value like '%A04204881%'
	or drug_source_value like '%A04507191%'
	or drug_source_value like '%A04705791%'
	or drug_source_value like '%A04804931%'
	or drug_source_value like '%A05003411%'
	or drug_source_value like '%A05303001%'
	or drug_source_value like '%A05405851%'
	or drug_source_value like '%A05706371%'
	or drug_source_value like '%A06104191%'
	or drug_source_value like '%A06907201%'
	or drug_source_value like '%A07208841%'
	or drug_source_value like '%A07705281%'
	or drug_source_value like '%A08505021%'
	or drug_source_value like '%A08603591%'
	or drug_source_value like '%A08803911%'
	or drug_source_value like '%A09306361%'
	or drug_source_value like '%A09704221%'
	or drug_source_value like '%A10004411%'
	or drug_source_value like '%A10704091%'
	or drug_source_value like '%A11656001%'
	or drug_source_value like '%A11801491%'
	or drug_source_value like '%A12605031%'
	or drug_source_value like '%A12804941%'
	or drug_source_value like '%A12951050%'
	or drug_source_value like '%A13153291%'
	or drug_source_value like '%A13306241%'
	or drug_source_value like '%A15205121%'
	or drug_source_value like '%A15301821%'
	or drug_source_value like '%A15603941%'
	or drug_source_value like '%A15902611%'
	or drug_source_value like '%A16205661%'
	or drug_source_value like '%A18953271%'
	or drug_source_value like '%A20403571%'
	or drug_source_value like '%A20756061%'
	or drug_source_value like '%A21405151%'
	or drug_source_value like '%A22607641%'
	or drug_source_value like '%A23403711%'
	or drug_source_value like '%A25005981%'
	or drug_source_value like '%A27804401%'
	or drug_source_value like '%A29506621%'
	or drug_source_value like '%A31804911%'
	or drug_source_value like '%A35104821%'
	or drug_source_value like '%A37804131%'
	or drug_source_value like '%A43903871%'
	or drug_source_value like '%A47404431%'
	or drug_source_value like '%A66303221%'
	or drug_source_value like '%A82300121%'
	or drug_source_value like '%B07404681%')
) C

WHERE C.days_supply >= 4
-- End Drug Exposure Criteria

UNION ALL
-- Begin Drug Exposure Criteria
select C.person_id, C.drug_exposure_id as event_id, C.drug_exposure_start_date as start_date,
       COALESCE(C.drug_exposure_end_date, DATEADD(day, 1, C.drug_exposure_start_date)) as end_date, C.drug_concept_id as TARGET_CONCEPT_ID, C.visit_occurrence_id,
       C.drug_exposure_start_date as sort_date
from
(
  select de.*
  FROM @cdm_database_schema.DRUG_EXPOSURE de
  where (drug_source_value like '%625200540%'
	or drug_source_value like '%626900310%'
	or drug_source_value like '%628800520%'
	or drug_source_value like '%641704410%'
	or drug_source_value like '%642003080%'
	or drug_source_value like '%642904480%'
	or drug_source_value like '%643703610%'
	or drug_source_value like '%644308590%'
	or drug_source_value like '%644501860%'
	or drug_source_value like '%644602670%'
	or drug_source_value like '%645403680%'
	or drug_source_value like '%645604140%'
	or drug_source_value like '%645905420%'
	or drug_source_value like '%645000310%'
	or drug_source_value like '%646203670%'
	or drug_source_value like '%648506730%'
	or drug_source_value like '%648602510%'
	or drug_source_value like '%649506250%'
	or drug_source_value like '%650102490%'
	or drug_source_value like '%650204450%'
	or drug_source_value like '%650303090%'
	or drug_source_value like '%651203800%'
	or drug_source_value like '%652105740%'
	or drug_source_value like '%652301120%'
	or drug_source_value like '%654004120%'
	or drug_source_value like '%655403010%'
	or drug_source_value like '%656003260%'
	or drug_source_value like '%657202450%'
	or drug_source_value like '%657502780%'
	or drug_source_value like '%657804920%'
	or drug_source_value like '%664602070%'
	or drug_source_value like '%665001720%'
	or drug_source_value like '%669804200%'
	or drug_source_value like '%670103390%'
	or drug_source_value like '%670606180%'
	or drug_source_value like '%670701640%'
	or drug_source_value like '%671703940%'
	or drug_source_value like '%674401320%'
	or drug_source_value like '%697100370%'
	or drug_source_value like '%698502260%'
	or drug_source_value like '%E01840821%'
	or drug_source_value like '%53300070%'
	or drug_source_value like '%625200790%'
	or drug_source_value like '%628900630%'
	or drug_source_value like '%640005770%'
	or drug_source_value like '%642103870%'
	or drug_source_value like '%642403470%'
	or drug_source_value like '%642506210%'
	or drug_source_value like '%642705420%'
	or drug_source_value like '%643305680%'
	or drug_source_value like '%643505490%'
	or drug_source_value like '%644307910%'
	or drug_source_value like '%644913010%'
	or drug_source_value like '%645203450%'
	or drug_source_value like '%645207300%'
	or drug_source_value like '%645403390%'
	or drug_source_value like '%645702130%'
	or drug_source_value like '%648103450%'
	or drug_source_value like '%648203310%'
	or drug_source_value like '%648506260%'
	or drug_source_value like '%649102980%'
	or drug_source_value like '%649404730%'
	or drug_source_value like '%649702470%'
	or drug_source_value like '%649805050%'
	or drug_source_value like '%650302690%'
	or drug_source_value like '%651805570%'
	or drug_source_value like '%652602900%'
	or drug_source_value like '%652903120%'
	or drug_source_value like '%653102330%'
	or drug_source_value like '%653102330%'
	or drug_source_value like '%653401980%'
	or drug_source_value like '%653701880%'
	or drug_source_value like '%653804430%'
	or drug_source_value like '%655604220%'
	or drug_source_value like '%659900580%'
	or drug_source_value like '%665506010%'
	or drug_source_value like '%668901770%'
	or drug_source_value like '%669501060%'
	or drug_source_value like '%670401250%'
	or drug_source_value like '%670605380%'
	or drug_source_value like '%684500480%'
	or drug_source_value like '%694204930%'
	or drug_source_value like '%697100580%'
	or drug_source_value like '%652103880%'
	or drug_source_value like '%A33203581%'
	or drug_source_value like '%E01300571%')
) C

WHERE C.days_supply >= 1
-- End Drug Exposure Criteria

  ) E
	JOIN @cdm_database_schema.observation_period OP on E.person_id = OP.person_id and E.start_date >=  OP.observation_period_start_date and E.start_date <= op.observation_period_end_date
  WHERE DATEADD(day,0,OP.OBSERVATION_PERIOD_START_DATE) <= E.START_DATE AND DATEADD(day,0,E.START_DATE) <= OP.OBSERVATION_PERIOD_END_DATE
) P

-- End Primary Events

)
SELECT event_id, person_id, start_date, end_date, op_start_date, op_end_date, visit_occurrence_id
INTO #qualified_events
FROM
(
  select pe.event_id, pe.person_id, pe.start_date, pe.end_date, pe.op_start_date, pe.op_end_date, row_number() over (partition by pe.person_id order by pe.start_date ASC) as ordinal, cast(pe.visit_occurrence_id as bigint) as visit_occurrence_id
  FROM primary_events pe

) QE

;

--- Inclusion Rule Inserts

select 0 as inclusion_rule_id, person_id, event_id
INTO #Inclusion_0
FROM
(
  select pe.person_id, pe.event_id
  FROM #qualified_events pe

JOIN (
-- Begin Criteria Group
select 0 as index_id, person_id, event_id
FROM
(
  select E.person_id, E.event_id
  FROM #qualified_events E
  INNER JOIN
  (
    -- Begin Correlated Criteria
SELECT 0 as index_id, p.person_id, p.event_id
FROM #qualified_events P
INNER JOIN
(
  -- Begin Condition Occurrence Criteria
SELECT C.person_id, C.condition_occurrence_id as event_id, C.condition_start_date as start_date, COALESCE(C.condition_end_date, DATEADD(day,1,C.condition_start_date)) as end_date,
       C.CONDITION_CONCEPT_ID as TARGET_CONCEPT_ID, C.visit_occurrence_id,
       C.condition_start_date as sort_date
FROM
(
  SELECT co.*
  FROM @cdm_database_schema.CONDITION_OCCURRENCE co
  JOIN #Codesets codesets on ((co.condition_concept_id = codesets.concept_id and codesets.codeset_id = 0))
) C


-- End Condition Occurrence Criteria

) A on A.person_id = P.person_id  AND A.START_DATE >= P.OP_START_DATE AND A.START_DATE <= P.OP_END_DATE AND A.START_DATE >= P.OP_START_DATE AND A.START_DATE <= DATEADD(day,0,P.START_DATE)
GROUP BY p.person_id, p.event_id
HAVING COUNT(A.TARGET_CONCEPT_ID) >= 1
-- End Correlated Criteria

  ) CQ on E.person_id = CQ.person_id and E.event_id = CQ.event_id
  GROUP BY E.person_id, E.event_id
  HAVING COUNT(index_id) = 1
) G
-- End Criteria Group
) AC on AC.person_id = pe.person_id AND AC.event_id = pe.event_id
) Results
;

SELECT inclusion_rule_id, person_id, event_id
INTO #inclusion_events
FROM (select inclusion_rule_id, person_id, event_id from #Inclusion_0) I;
TRUNCATE TABLE #Inclusion_0;
DROP TABLE #Inclusion_0;


with cteIncludedEvents(event_id, person_id, start_date, end_date, op_start_date, op_end_date, ordinal) as
(
  SELECT event_id, person_id, start_date, end_date, op_start_date, op_end_date, row_number() over (partition by person_id order by start_date ASC) as ordinal
  from
  (
    select Q.event_id, Q.person_id, Q.start_date, Q.end_date, Q.op_start_date, Q.op_end_date, SUM(coalesce(POWER(cast(2 as bigint), I.inclusion_rule_id), 0)) as inclusion_rule_mask
    from #qualified_events Q
    LEFT JOIN #inclusion_events I on I.person_id = Q.person_id and I.event_id = Q.event_id
    GROUP BY Q.event_id, Q.person_id, Q.start_date, Q.end_date, Q.op_start_date, Q.op_end_date
  ) MG -- matching groups

  -- the matching group with all bits set ( POWER(2,# of inclusion rules) - 1 = inclusion_rule_mask
  WHERE (MG.inclusion_rule_mask = POWER(cast(2 as bigint),1)-1)

)
select event_id, person_id, start_date, end_date, op_start_date, op_end_date
into #included_events
FROM cteIncludedEvents Results

;

-- custom era strategy

with ctePersons(person_id) as (
	select distinct person_id from #included_events
)

select person_id, drug_exposure_start_date, drug_exposure_end_date
INTO #drugTarget
FROM (
	select de.PERSON_ID, DRUG_EXPOSURE_START_DATE,  COALESCE(DRUG_EXPOSURE_END_DATE, DATEADD(day,DAYS_SUPPLY,DRUG_EXPOSURE_START_DATE), DATEADD(day,1,DRUG_EXPOSURE_START_DATE)) as DRUG_EXPOSURE_END_DATE
	FROM @cdm_database_schema.DRUG_EXPOSURE de
	JOIN ctePersons p on de.person_id = p.person_id
	where (drug_source_value like '%644500230%'
	or drug_source_value like '%657200460%'
	or drug_source_value like '%655500710%'
	or drug_source_value like '%A02003951%'
	or drug_source_value like '%A09703641%'
	or drug_source_value like '%A50703171%'
	or drug_source_value like '%A78800531%'
	or drug_source_value like '%E09060741%'
	or drug_source_value like '%657200450%'
	or drug_source_value like '%644500240%'
	or drug_source_value like '%A02003941%'
	or drug_source_value like '%A09703631%'
	or drug_source_value like '%642103130%'
	or drug_source_value like '%643304750%'
	or drug_source_value like '%652100900%'
	or drug_source_value like '%B07404671%'
	or drug_source_value like '%643304010%'
	or drug_source_value like '%649804050%'
	or drug_source_value like '%647303660%'
	or drug_source_value like '%647500030%'
	or drug_source_value like '%661306120%'
	or drug_source_value like '%52700280%'
	or drug_source_value like '%654201280%'
	or drug_source_value like '%653403041%'
	or drug_source_value like '%643501790%'
	or drug_source_value like '%655500570%'
	or drug_source_value like '%668900380%'
	or drug_source_value like '%697100010%'
	or drug_source_value like '%53600910%'
	or drug_source_value like '%652604800%'
	or drug_source_value like '%644806040%'
	or drug_source_value like '%645902160%'
	or drug_source_value like '%693902310%'
	or drug_source_value like '%653404511%'
	or drug_source_value like '%670600660%'
	or drug_source_value like '%640900650%'
	or drug_source_value like '%653000980%'
	or drug_source_value like '%654701010%'
	or drug_source_value like '%679800310%'
	or drug_source_value like '%649402690%'
	or drug_source_value like '%661901370%'
	or drug_source_value like '%646800300%'
	or drug_source_value like '%A03805801%'
	or drug_source_value like '%645202710%'
	or drug_source_value like '%641600770%'
	or drug_source_value like '%650204530%'
	or drug_source_value like '%648504020%'
	or drug_source_value like '%643701650%'
	or drug_source_value like '%652300460%'
	or drug_source_value like '%649605220%'
	or drug_source_value like '%670101390%'
	or drug_source_value like '%683601450%'
	or drug_source_value like '%690300790%'
	or drug_source_value like '%641802230%'
	or drug_source_value like '%621800930%'
	or drug_source_value like '%649501910%'
	or drug_source_value like '%654301780%'
	or drug_source_value like '%694000310%'
	or drug_source_value like '%655402100%'
	or drug_source_value like '%689101020%'
	or drug_source_value like '%669604920%'
	or drug_source_value like '%643200650%'
	or drug_source_value like '%693201130%'
	or drug_source_value like '%623005820%'
	or drug_source_value like '%663602830%'
	or drug_source_value like '%671803390%'
	or drug_source_value like '%625800450%'
	or drug_source_value like '%669902660%'
	or drug_source_value like '%646200230%'
	or drug_source_value like '%647205470%'
	or drug_source_value like '%664601020%'
	or drug_source_value like '%647801610%'
	or drug_source_value like '%644000520%'
	or drug_source_value like '%648200960%'
	or drug_source_value like '%665000690%'
	or drug_source_value like '%643105030%'
	or drug_source_value like '%650301160%'
	or drug_source_value like '%699801590%'
	or drug_source_value like '%657501210%'
	or drug_source_value like '%658600720%'
	or drug_source_value like '%657302000%'
	or drug_source_value like '%658106500%'
	or drug_source_value like '%655601330%'
	or drug_source_value like '%A00306591%'
	or drug_source_value like '%A01207491%'
	or drug_source_value like '%A01306731%'
	or drug_source_value like '%A01402391%'
	or drug_source_value like '%A02704791%'
	or drug_source_value like '%A03006081%'
	or drug_source_value like '%A03505501%'
	or drug_source_value like '%A03805801%'
	or drug_source_value like '%A04304361%'
	or drug_source_value like '%A05404911%'
	or drug_source_value like '%A05607921%'
	or drug_source_value like '%A06502401%'
	or drug_source_value like '%A06653831%'
	or drug_source_value like '%A06703931%'
	or drug_source_value like '%A07104681%'
	or drug_source_value like '%A08504471%'
	or drug_source_value like '%A10004041%'
	or drug_source_value like '%A10703811%'
	or drug_source_value like '%A11103910%'
	or drug_source_value like '%A11204211%'
	or drug_source_value like '%A12202661%'
	or drug_source_value like '%A12603911%'
	or drug_source_value like '%A12703451%'
	or drug_source_value like '%A12804371%'
	or drug_source_value like '%A13302401%'
	or drug_source_value like '%A15204811%'
	or drug_source_value like '%A15603511%'
	or drug_source_value like '%A16604351%'
	or drug_source_value like '%A17001211%'
	or drug_source_value like '%A19203331%'
	or drug_source_value like '%A20602781%'
	or drug_source_value like '%A21404141%'
	or drug_source_value like '%A22401711%'
	or drug_source_value like '%A22607041%'
	or drug_source_value like '%A23503391%'
	or drug_source_value like '%A25005321%'
	or drug_source_value like '%A25803451%'
	or drug_source_value like '%A26401251%'
	or drug_source_value like '%A29503531%'
	or drug_source_value like '%A31805061%'
	or drug_source_value like '%A32202661%'
	or drug_source_value like '%A33203101%'
	or drug_source_value like '%A34003321%'
	or drug_source_value like '%A35104451%'
	or drug_source_value like '%A36705671%'
	or drug_source_value like '%A42900481%'
	or drug_source_value like '%A45900521%'
	or drug_source_value like '%A50703461%'
	or drug_source_value like '%A59500071%'
	or drug_source_value like '%A59500091%'
	or drug_source_value like '%A60300241%'
	or drug_source_value like '%A60603021%'
	or drug_source_value like '%A62755371%'
	or drug_source_value like '%A79100531%'
	or drug_source_value like '%A82800181%'
	or drug_source_value like '%E02670321%'
	or drug_source_value like '%622801280%'
	or drug_source_value like '%623006470%'
	or drug_source_value like '%625200160%'
	or drug_source_value like '%629701920%'
	or drug_source_value like '%640004850%'
	or drug_source_value like '%640005790%'
	or drug_source_value like '%640902430%'
	or drug_source_value like '%641606500%'
	or drug_source_value like '%641800230%'
	or drug_source_value like '%641904630%'
	or drug_source_value like '%642103120%'
	or drug_source_value like '%642103130%'
	or drug_source_value like '%652104500%'
	or drug_source_value like '%652100910%'
	or drug_source_value like '%642202580%'
	or drug_source_value like '%642303060%'
	or drug_source_value like '%642401990%'
	or drug_source_value like '%642505320%'
	or drug_source_value like '%642704290%'
	or drug_source_value like '%642801860%'
	or drug_source_value like '%642902870%'
	or drug_source_value like '%643105180%'
	or drug_source_value like '%643504110%'
	or drug_source_value like '%643902070%'
	or drug_source_value like '%644306200%'
	or drug_source_value like '%644702930%'
	or drug_source_value like '%644803240%'
	or drug_source_value like '%644912320%'
	or drug_source_value like '%645102850%'
	or drug_source_value like '%645207310%'
	or drug_source_value like '%645302340%'
	or drug_source_value like '%645402540%'
	or drug_source_value like '%645602340%'
	or drug_source_value like '%645701450%'
	or drug_source_value like '%645903560%'
	or drug_source_value like '%646002650%'
	or drug_source_value like '%646202490%'
	or drug_source_value like '%648102550%'
	or drug_source_value like '%648203400%'
	or drug_source_value like '%648504750%'
	or drug_source_value like '%648602160%'
	or drug_source_value like '%649001490%'
	or drug_source_value like '%649101530%'
	or drug_source_value like '%649702500%'
	or drug_source_value like '%649804340%'
	or drug_source_value like '%650202660%'
	or drug_source_value like '%650301780%'
	or drug_source_value like '%651600810%'
	or drug_source_value like '%651804950%'
	or drug_source_value like '%651903370%'
	or drug_source_value like '%652601680%'
	or drug_source_value like '%653004400%'
	or drug_source_value like '%653102070%'
	or drug_source_value like '%653401610%'
	or drug_source_value like '%654303670%'
	or drug_source_value like '%655200060%'
	or drug_source_value like '%657201250%'
	or drug_source_value like '%657305310%'
	or drug_source_value like '%657803180%'
	or drug_source_value like '%658001880%'
	or drug_source_value like '%658602100%'
	or drug_source_value like '%660702540%'
	or drug_source_value like '%661602550%'
	or drug_source_value like '%661902130%'
	or drug_source_value like '%662502120%'
	or drug_source_value like '%668400320%'
	or drug_source_value like '%669802260%'
	or drug_source_value like '%670001120%'
	or drug_source_value like '%670302210%'
	or drug_source_value like '%670400980%'
	or drug_source_value like '%670604180%'
	or drug_source_value like '%671804160%'
	or drug_source_value like '%674400910%'
	or drug_source_value like '%684500260%'
	or drug_source_value like '%689001100%'
	or drug_source_value like '%690302340%'
	or drug_source_value like '%693201140%'
	or drug_source_value like '%693901000%'
	or drug_source_value like '%694204750%'
	or drug_source_value like '%697100030%'
	or drug_source_value like '%698000980%'
	or drug_source_value like '%A07705281%'
	or drug_source_value like '%A12804941%'
	or drug_source_value like '%A00358501%'
	or drug_source_value like '%A00702121%'
	or drug_source_value like '%A00803151%'
	or drug_source_value like '%A01208931%'
	or drug_source_value like '%A01307341%'
	or drug_source_value like '%A01403031%'
	or drug_source_value like '%A01559741%'
	or drug_source_value like '%A02108051%'
	or drug_source_value like '%A02352001%'
	or drug_source_value like '%A02507671%'
	or drug_source_value like '%A02706171%'
	or drug_source_value like '%A03405441%'
	or drug_source_value like '%A03603021%'
	or drug_source_value like '%A04204881%'
	or drug_source_value like '%A04507191%'
	or drug_source_value like '%A04705791%'
	or drug_source_value like '%A04804931%'
	or drug_source_value like '%A05003411%'
	or drug_source_value like '%A05303001%'
	or drug_source_value like '%A05405851%'
	or drug_source_value like '%A05706371%'
	or drug_source_value like '%A06104191%'
	or drug_source_value like '%A06907201%'
	or drug_source_value like '%A07208841%'
	or drug_source_value like '%A07705281%'
	or drug_source_value like '%A08505021%'
	or drug_source_value like '%A08603591%'
	or drug_source_value like '%A08803911%'
	or drug_source_value like '%A09306361%'
	or drug_source_value like '%A09704221%'
	or drug_source_value like '%A10004411%'
	or drug_source_value like '%A10704091%'
	or drug_source_value like '%A11656001%'
	or drug_source_value like '%A11801491%'
	or drug_source_value like '%A12605031%'
	or drug_source_value like '%A12804941%'
	or drug_source_value like '%A12951050%'
	or drug_source_value like '%A13153291%'
	or drug_source_value like '%A13306241%'
	or drug_source_value like '%A15205121%'
	or drug_source_value like '%A15301821%'
	or drug_source_value like '%A15603941%'
	or drug_source_value like '%A15902611%'
	or drug_source_value like '%A16205661%'
	or drug_source_value like '%A18953271%'
	or drug_source_value like '%A20403571%'
	or drug_source_value like '%A20756061%'
	or drug_source_value like '%A21405151%'
	or drug_source_value like '%A22607641%'
	or drug_source_value like '%A23403711%'
	or drug_source_value like '%A25005981%'
	or drug_source_value like '%A27804401%'
	or drug_source_value like '%A29506621%'
	or drug_source_value like '%A31804911%'
	or drug_source_value like '%A35104821%'
	or drug_source_value like '%A37804131%'
	or drug_source_value like '%A43903871%'
	or drug_source_value like '%A47404431%'
	or drug_source_value like '%A66303221%'
	or drug_source_value like '%A82300121%'
	or drug_source_value like '%B07404681%'
	or drug_source_value like '%625200540%'
	or drug_source_value like '%626900310%'
	or drug_source_value like '%628800520%'
	or drug_source_value like '%641704410%'
	or drug_source_value like '%642003080%'
	or drug_source_value like '%642904480%'
	or drug_source_value like '%643703610%'
	or drug_source_value like '%644308590%'
	or drug_source_value like '%644501860%'
	or drug_source_value like '%644602670%'
	or drug_source_value like '%645403680%'
	or drug_source_value like '%645604140%'
	or drug_source_value like '%645905420%'
	or drug_source_value like '%645000310%'
	or drug_source_value like '%646203670%'
	or drug_source_value like '%648506730%'
	or drug_source_value like '%648602510%'
	or drug_source_value like '%649506250%'
	or drug_source_value like '%650102490%'
	or drug_source_value like '%650204450%'
	or drug_source_value like '%650303090%'
	or drug_source_value like '%651203800%'
	or drug_source_value like '%652105740%'
	or drug_source_value like '%652301120%'
	or drug_source_value like '%654004120%'
	or drug_source_value like '%655403010%'
	or drug_source_value like '%656003260%'
	or drug_source_value like '%657202450%'
	or drug_source_value like '%657502780%'
	or drug_source_value like '%657804920%'
	or drug_source_value like '%664602070%'
	or drug_source_value like '%665001720%'
	or drug_source_value like '%669804200%'
	or drug_source_value like '%670103390%'
	or drug_source_value like '%670606180%'
	or drug_source_value like '%670701640%'
	or drug_source_value like '%671703940%'
	or drug_source_value like '%674401320%'
	or drug_source_value like '%697100370%'
	or drug_source_value like '%698502260%'
	or drug_source_value like '%E01840821%'
	or drug_source_value like '%53300070%'
	or drug_source_value like '%625200790%'
	or drug_source_value like '%628900630%'
	or drug_source_value like '%640005770%'
	or drug_source_value like '%642103870%'
	or drug_source_value like '%642403470%'
	or drug_source_value like '%642506210%'
	or drug_source_value like '%642705420%'
	or drug_source_value like '%643305680%'
	or drug_source_value like '%643505490%'
	or drug_source_value like '%644307910%'
	or drug_source_value like '%644913010%'
	or drug_source_value like '%645203450%'
	or drug_source_value like '%645207300%'
	or drug_source_value like '%645403390%'
	or drug_source_value like '%645702130%'
	or drug_source_value like '%648103450%'
	or drug_source_value like '%648203310%'
	or drug_source_value like '%648506260%'
	or drug_source_value like '%649102980%'
	or drug_source_value like '%649404730%'
	or drug_source_value like '%649702470%'
	or drug_source_value like '%649805050%'
	or drug_source_value like '%650302690%'
	or drug_source_value like '%651805570%'
	or drug_source_value like '%652602900%'
	or drug_source_value like '%652903120%'
	or drug_source_value like '%653102330%'
	or drug_source_value like '%653102330%'
	or drug_source_value like '%653401980%'
	or drug_source_value like '%653701880%'
	or drug_source_value like '%653804430%'
	or drug_source_value like '%655604220%'
	or drug_source_value like '%659900580%'
	or drug_source_value like '%665506010%'
	or drug_source_value like '%668901770%'
	or drug_source_value like '%669501060%'
	or drug_source_value like '%670401250%'
	or drug_source_value like '%670605380%'
	or drug_source_value like '%684500480%'
	or drug_source_value like '%694204930%'
	or drug_source_value like '%697100580%'
	or drug_source_value like '%652103880%'
	or drug_source_value like '%A33203581%'
	or drug_source_value like '%E01300571%'
)

	UNION ALL

	select de.PERSON_ID, DRUG_EXPOSURE_START_DATE,  COALESCE(DRUG_EXPOSURE_END_DATE, DATEADD(day,DAYS_SUPPLY,DRUG_EXPOSURE_START_DATE), DATEADD(day,1,DRUG_EXPOSURE_START_DATE)) as DRUG_EXPOSURE_END_DATE
	FROM @cdm_database_schema.DRUG_EXPOSURE de
	JOIN ctePersons p on de.person_id = p.person_id
	where (drug_source_value like '%644500230%'
	or drug_source_value like '%657200460%'
	or drug_source_value like '%655500710%'
	or drug_source_value like '%A02003951%'
	or drug_source_value like '%A09703641%'
	or drug_source_value like '%A50703171%'
	or drug_source_value like '%A78800531%'
	or drug_source_value like '%E09060741%'
	or drug_source_value like '%657200450%'
	or drug_source_value like '%644500240%'
	or drug_source_value like '%A02003941%'
	or drug_source_value like '%A09703631%'
	or drug_source_value like '%642103130%'
	or drug_source_value like '%643304750%'
	or drug_source_value like '%652100900%'
	or drug_source_value like '%B07404671%'
	or drug_source_value like '%643304010%'
	or drug_source_value like '%649804050%'
	or drug_source_value like '%647303660%'
	or drug_source_value like '%647500030%'
	or drug_source_value like '%661306120%'
	or drug_source_value like '%52700280%'
	or drug_source_value like '%654201280%'
	or drug_source_value like '%653403041%'
	or drug_source_value like '%643501790%'
	or drug_source_value like '%655500570%'
	or drug_source_value like '%668900380%'
	or drug_source_value like '%697100010%'
	or drug_source_value like '%53600910%'
	or drug_source_value like '%652604800%'
	or drug_source_value like '%644806040%'
	or drug_source_value like '%645902160%'
	or drug_source_value like '%693902310%'
	or drug_source_value like '%653404511%'
	or drug_source_value like '%670600660%'
	or drug_source_value like '%640900650%'
	or drug_source_value like '%653000980%'
	or drug_source_value like '%654701010%'
	or drug_source_value like '%679800310%'
	or drug_source_value like '%649402690%'
	or drug_source_value like '%661901370%'
	or drug_source_value like '%646800300%'
	or drug_source_value like '%A03805801%'
	or drug_source_value like '%645202710%'
	or drug_source_value like '%641600770%'
	or drug_source_value like '%650204530%'
	or drug_source_value like '%648504020%'
	or drug_source_value like '%643701650%'
	or drug_source_value like '%652300460%'
	or drug_source_value like '%649605220%'
	or drug_source_value like '%670101390%'
	or drug_source_value like '%683601450%'
	or drug_source_value like '%690300790%'
	or drug_source_value like '%641802230%'
	or drug_source_value like '%621800930%'
	or drug_source_value like '%649501910%'
	or drug_source_value like '%654301780%'
	or drug_source_value like '%694000310%'
	or drug_source_value like '%655402100%'
	or drug_source_value like '%689101020%'
	or drug_source_value like '%669604920%'
	or drug_source_value like '%643200650%'
	or drug_source_value like '%693201130%'
	or drug_source_value like '%623005820%'
	or drug_source_value like '%663602830%'
	or drug_source_value like '%671803390%'
	or drug_source_value like '%625800450%'
	or drug_source_value like '%669902660%'
	or drug_source_value like '%646200230%'
	or drug_source_value like '%647205470%'
	or drug_source_value like '%664601020%'
	or drug_source_value like '%647801610%'
	or drug_source_value like '%644000520%'
	or drug_source_value like '%648200960%'
	or drug_source_value like '%665000690%'
	or drug_source_value like '%643105030%'
	or drug_source_value like '%650301160%'
	or drug_source_value like '%699801590%'
	or drug_source_value like '%657501210%'
	or drug_source_value like '%658600720%'
	or drug_source_value like '%657302000%'
	or drug_source_value like '%658106500%'
	or drug_source_value like '%655601330%'
	or drug_source_value like '%A00306591%'
	or drug_source_value like '%A01207491%'
	or drug_source_value like '%A01306731%'
	or drug_source_value like '%A01402391%'
	or drug_source_value like '%A02704791%'
	or drug_source_value like '%A03006081%'
	or drug_source_value like '%A03505501%'
	or drug_source_value like '%A03805801%'
	or drug_source_value like '%A04304361%'
	or drug_source_value like '%A05404911%'
	or drug_source_value like '%A05607921%'
	or drug_source_value like '%A06502401%'
	or drug_source_value like '%A06653831%'
	or drug_source_value like '%A06703931%'
	or drug_source_value like '%A07104681%'
	or drug_source_value like '%A08504471%'
	or drug_source_value like '%A10004041%'
	or drug_source_value like '%A10703811%'
	or drug_source_value like '%A11103910%'
	or drug_source_value like '%A11204211%'
	or drug_source_value like '%A12202661%'
	or drug_source_value like '%A12603911%'
	or drug_source_value like '%A12703451%'
	or drug_source_value like '%A12804371%'
	or drug_source_value like '%A13302401%'
	or drug_source_value like '%A15204811%'
	or drug_source_value like '%A15603511%'
	or drug_source_value like '%A16604351%'
	or drug_source_value like '%A17001211%'
	or drug_source_value like '%A19203331%'
	or drug_source_value like '%A20602781%'
	or drug_source_value like '%A21404141%'
	or drug_source_value like '%A22401711%'
	or drug_source_value like '%A22607041%'
	or drug_source_value like '%A23503391%'
	or drug_source_value like '%A25005321%'
	or drug_source_value like '%A25803451%'
	or drug_source_value like '%A26401251%'
	or drug_source_value like '%A29503531%'
	or drug_source_value like '%A31805061%'
	or drug_source_value like '%A32202661%'
	or drug_source_value like '%A33203101%'
	or drug_source_value like '%A34003321%'
	or drug_source_value like '%A35104451%'
	or drug_source_value like '%A36705671%'
	or drug_source_value like '%A42900481%'
	or drug_source_value like '%A45900521%'
	or drug_source_value like '%A50703461%'
	or drug_source_value like '%A59500071%'
	or drug_source_value like '%A59500091%'
	or drug_source_value like '%A60300241%'
	or drug_source_value like '%A60603021%'
	or drug_source_value like '%A62755371%'
	or drug_source_value like '%A79100531%'
	or drug_source_value like '%A82800181%'
	or drug_source_value like '%E02670321%'
	or drug_source_value like '%622801280%'
	or drug_source_value like '%623006470%'
	or drug_source_value like '%625200160%'
	or drug_source_value like '%629701920%'
	or drug_source_value like '%640004850%'
	or drug_source_value like '%640005790%'
	or drug_source_value like '%640902430%'
	or drug_source_value like '%641606500%'
	or drug_source_value like '%641800230%'
	or drug_source_value like '%641904630%'
	or drug_source_value like '%642103120%'
	or drug_source_value like '%642103130%'
	or drug_source_value like '%652104500%'
	or drug_source_value like '%652100910%'
	or drug_source_value like '%642202580%'
	or drug_source_value like '%642303060%'
	or drug_source_value like '%642401990%'
	or drug_source_value like '%642505320%'
	or drug_source_value like '%642704290%'
	or drug_source_value like '%642801860%'
	or drug_source_value like '%642902870%'
	or drug_source_value like '%643105180%'
	or drug_source_value like '%643504110%'
	or drug_source_value like '%643902070%'
	or drug_source_value like '%644306200%'
	or drug_source_value like '%644702930%'
	or drug_source_value like '%644803240%'
	or drug_source_value like '%644912320%'
	or drug_source_value like '%645102850%'
	or drug_source_value like '%645207310%'
	or drug_source_value like '%645302340%'
	or drug_source_value like '%645402540%'
	or drug_source_value like '%645602340%'
	or drug_source_value like '%645701450%'
	or drug_source_value like '%645903560%'
	or drug_source_value like '%646002650%'
	or drug_source_value like '%646202490%'
	or drug_source_value like '%648102550%'
	or drug_source_value like '%648203400%'
	or drug_source_value like '%648504750%'
	or drug_source_value like '%648602160%'
	or drug_source_value like '%649001490%'
	or drug_source_value like '%649101530%'
	or drug_source_value like '%649702500%'
	or drug_source_value like '%649804340%'
	or drug_source_value like '%650202660%'
	or drug_source_value like '%650301780%'
	or drug_source_value like '%651600810%'
	or drug_source_value like '%651804950%'
	or drug_source_value like '%651903370%'
	or drug_source_value like '%652601680%'
	or drug_source_value like '%653004400%'
	or drug_source_value like '%653102070%'
	or drug_source_value like '%653401610%'
	or drug_source_value like '%654303670%'
	or drug_source_value like '%655200060%'
	or drug_source_value like '%657201250%'
	or drug_source_value like '%657305310%'
	or drug_source_value like '%657803180%'
	or drug_source_value like '%658001880%'
	or drug_source_value like '%658602100%'
	or drug_source_value like '%660702540%'
	or drug_source_value like '%661602550%'
	or drug_source_value like '%661902130%'
	or drug_source_value like '%662502120%'
	or drug_source_value like '%668400320%'
	or drug_source_value like '%669802260%'
	or drug_source_value like '%670001120%'
	or drug_source_value like '%670302210%'
	or drug_source_value like '%670400980%'
	or drug_source_value like '%670604180%'
	or drug_source_value like '%671804160%'
	or drug_source_value like '%674400910%'
	or drug_source_value like '%684500260%'
	or drug_source_value like '%689001100%'
	or drug_source_value like '%690302340%'
	or drug_source_value like '%693201140%'
	or drug_source_value like '%693901000%'
	or drug_source_value like '%694204750%'
	or drug_source_value like '%697100030%'
	or drug_source_value like '%698000980%'
	or drug_source_value like '%A07705281%'
	or drug_source_value like '%A12804941%'
	or drug_source_value like '%A00358501%'
	or drug_source_value like '%A00702121%'
	or drug_source_value like '%A00803151%'
	or drug_source_value like '%A01208931%'
	or drug_source_value like '%A01307341%'
	or drug_source_value like '%A01403031%'
	or drug_source_value like '%A01559741%'
	or drug_source_value like '%A02108051%'
	or drug_source_value like '%A02352001%'
	or drug_source_value like '%A02507671%'
	or drug_source_value like '%A02706171%'
	or drug_source_value like '%A03405441%'
	or drug_source_value like '%A03603021%'
	or drug_source_value like '%A04204881%'
	or drug_source_value like '%A04507191%'
	or drug_source_value like '%A04705791%'
	or drug_source_value like '%A04804931%'
	or drug_source_value like '%A05003411%'
	or drug_source_value like '%A05303001%'
	or drug_source_value like '%A05405851%'
	or drug_source_value like '%A05706371%'
	or drug_source_value like '%A06104191%'
	or drug_source_value like '%A06907201%'
	or drug_source_value like '%A07208841%'
	or drug_source_value like '%A07705281%'
	or drug_source_value like '%A08505021%'
	or drug_source_value like '%A08603591%'
	or drug_source_value like '%A08803911%'
	or drug_source_value like '%A09306361%'
	or drug_source_value like '%A09704221%'
	or drug_source_value like '%A10004411%'
	or drug_source_value like '%A10704091%'
	or drug_source_value like '%A11656001%'
	or drug_source_value like '%A11801491%'
	or drug_source_value like '%A12605031%'
	or drug_source_value like '%A12804941%'
	or drug_source_value like '%A12951050%'
	or drug_source_value like '%A13153291%'
	or drug_source_value like '%A13306241%'
	or drug_source_value like '%A15205121%'
	or drug_source_value like '%A15301821%'
	or drug_source_value like '%A15603941%'
	or drug_source_value like '%A15902611%'
	or drug_source_value like '%A16205661%'
	or drug_source_value like '%A18953271%'
	or drug_source_value like '%A20403571%'
	or drug_source_value like '%A20756061%'
	or drug_source_value like '%A21405151%'
	or drug_source_value like '%A22607641%'
	or drug_source_value like '%A23403711%'
	or drug_source_value like '%A25005981%'
	or drug_source_value like '%A27804401%'
	or drug_source_value like '%A29506621%'
	or drug_source_value like '%A31804911%'
	or drug_source_value like '%A35104821%'
	or drug_source_value like '%A37804131%'
	or drug_source_value like '%A43903871%'
	or drug_source_value like '%A47404431%'
	or drug_source_value like '%A66303221%'
	or drug_source_value like '%A82300121%'
	or drug_source_value like '%B07404681%'
	or drug_source_value like '%625200540%'
	or drug_source_value like '%626900310%'
	or drug_source_value like '%628800520%'
	or drug_source_value like '%641704410%'
	or drug_source_value like '%642003080%'
	or drug_source_value like '%642904480%'
	or drug_source_value like '%643703610%'
	or drug_source_value like '%644308590%'
	or drug_source_value like '%644501860%'
	or drug_source_value like '%644602670%'
	or drug_source_value like '%645403680%'
	or drug_source_value like '%645604140%'
	or drug_source_value like '%645905420%'
	or drug_source_value like '%645000310%'
	or drug_source_value like '%646203670%'
	or drug_source_value like '%648506730%'
	or drug_source_value like '%648602510%'
	or drug_source_value like '%649506250%'
	or drug_source_value like '%650102490%'
	or drug_source_value like '%650204450%'
	or drug_source_value like '%650303090%'
	or drug_source_value like '%651203800%'
	or drug_source_value like '%652105740%'
	or drug_source_value like '%652301120%'
	or drug_source_value like '%654004120%'
	or drug_source_value like '%655403010%'
	or drug_source_value like '%656003260%'
	or drug_source_value like '%657202450%'
	or drug_source_value like '%657502780%'
	or drug_source_value like '%657804920%'
	or drug_source_value like '%664602070%'
	or drug_source_value like '%665001720%'
	or drug_source_value like '%669804200%'
	or drug_source_value like '%670103390%'
	or drug_source_value like '%670606180%'
	or drug_source_value like '%670701640%'
	or drug_source_value like '%671703940%'
	or drug_source_value like '%674401320%'
	or drug_source_value like '%697100370%'
	or drug_source_value like '%698502260%'
	or drug_source_value like '%E01840821%'
	or drug_source_value like '%53300070%'
	or drug_source_value like '%625200790%'
	or drug_source_value like '%628900630%'
	or drug_source_value like '%640005770%'
	or drug_source_value like '%642103870%'
	or drug_source_value like '%642403470%'
	or drug_source_value like '%642506210%'
	or drug_source_value like '%642705420%'
	or drug_source_value like '%643305680%'
	or drug_source_value like '%643505490%'
	or drug_source_value like '%644307910%'
	or drug_source_value like '%644913010%'
	or drug_source_value like '%645203450%'
	or drug_source_value like '%645207300%'
	or drug_source_value like '%645403390%'
	or drug_source_value like '%645702130%'
	or drug_source_value like '%648103450%'
	or drug_source_value like '%648203310%'
	or drug_source_value like '%648506260%'
	or drug_source_value like '%649102980%'
	or drug_source_value like '%649404730%'
	or drug_source_value like '%649702470%'
	or drug_source_value like '%649805050%'
	or drug_source_value like '%650302690%'
	or drug_source_value like '%651805570%'
	or drug_source_value like '%652602900%'
	or drug_source_value like '%652903120%'
	or drug_source_value like '%653102330%'
	or drug_source_value like '%653102330%'
	or drug_source_value like '%653401980%'
	or drug_source_value like '%653701880%'
	or drug_source_value like '%653804430%'
	or drug_source_value like '%655604220%'
	or drug_source_value like '%659900580%'
	or drug_source_value like '%665506010%'
	or drug_source_value like '%668901770%'
	or drug_source_value like '%669501060%'
	or drug_source_value like '%670401250%'
	or drug_source_value like '%670605380%'
	or drug_source_value like '%684500480%'
	or drug_source_value like '%694204930%'
	or drug_source_value like '%697100580%'
	or drug_source_value like '%652103880%'
	or drug_source_value like '%A33203581%'
	or drug_source_value like '%E01300571%'
)
) E
;

select et.event_id, et.person_id, ERAS.era_end_date as end_date
INTO #strategy_ends
from #included_events et
JOIN
(
  select ENDS.person_id, min(drug_exposure_start_date) as era_start_date, DATEADD(day,0, ENDS.era_end_date) as era_end_date
  from
  (
    select de.person_id, de.drug_exposure_start_date, MIN(e.END_DATE) as era_end_date
    FROM #drugTarget DE
    JOIN
    (
      --cteEndDates
      select PERSON_ID, DATEADD(day,-1 * 180,EVENT_DATE) as END_DATE -- unpad the end date by 180
      FROM
      (
				select PERSON_ID, EVENT_DATE, EVENT_TYPE,
				MAX(START_ORDINAL) OVER (PARTITION BY PERSON_ID ORDER BY event_date, event_type ROWS UNBOUNDED PRECEDING) AS start_ordinal,
				ROW_NUMBER() OVER (PARTITION BY PERSON_ID ORDER BY EVENT_DATE, EVENT_TYPE) AS OVERALL_ORD -- this re-numbers the inner UNION so all rows are numbered ordered by the event date
				from
				(
					-- select the start dates, assigning a row number to each
					Select PERSON_ID, DRUG_EXPOSURE_START_DATE AS EVENT_DATE, 0 as EVENT_TYPE, ROW_NUMBER() OVER (PARTITION BY PERSON_ID ORDER BY DRUG_EXPOSURE_START_DATE) as START_ORDINAL
					from #drugTarget D

					UNION ALL

					-- add the end dates with NULL as the row number, padding the end dates by 180 to allow a grace period for overlapping ranges.
					select PERSON_ID, DATEADD(day,180,DRUG_EXPOSURE_END_DATE), 1 as EVENT_TYPE, NULL
					FROM #drugTarget D
				) RAWDATA
      ) E
      WHERE 2 * E.START_ORDINAL - E.OVERALL_ORD = 0
    ) E on DE.PERSON_ID = E.PERSON_ID and E.END_DATE >= DE.DRUG_EXPOSURE_START_DATE
    GROUP BY de.person_id, de.drug_exposure_start_date
  ) ENDS
  GROUP BY ENDS.person_id, ENDS.era_end_date
) ERAS on ERAS.person_id = et.person_id
WHERE et.start_date between ERAS.era_start_date and ERAS.era_end_date;

TRUNCATE TABLE #drugTarget;
DROP TABLE #drugTarget;


-- generate cohort periods into #final_cohort
with cohort_ends (event_id, person_id, end_date) as
(
	-- cohort exit dates
  -- By default, cohort exit at the event's op end date
select event_id, person_id, op_end_date as end_date from #included_events
UNION ALL
-- End Date Strategy
SELECT event_id, person_id, end_date from #strategy_ends

),
first_ends (person_id, start_date, end_date) as
(
	select F.person_id, F.start_date, F.end_date
	FROM (
	  select I.event_id, I.person_id, I.start_date, E.end_date, row_number() over (partition by I.person_id, I.event_id order by E.end_date) as ordinal
	  from #included_events I
	  join cohort_ends E on I.event_id = E.event_id and I.person_id = E.person_id and E.end_date >= I.start_date
	) F
	WHERE F.ordinal = 1
)
select person_id, start_date, end_date
INTO #cohort_rows
from first_ends;

with cteEndDates (person_id, end_date) AS -- the magic
(
	SELECT
		person_id
		, DATEADD(day,-1 * 0, event_date)  as end_date
	FROM
	(
		SELECT
			person_id
			, event_date
			, event_type
			, MAX(start_ordinal) OVER (PARTITION BY person_id ORDER BY event_date, event_type ROWS UNBOUNDED PRECEDING) AS start_ordinal
			, ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY event_date, event_type) AS overall_ord
		FROM
		(
			SELECT
				person_id
				, start_date AS event_date
				, -1 AS event_type
				, ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY start_date) AS start_ordinal
			FROM #cohort_rows

			UNION ALL


			SELECT
				person_id
				, DATEADD(day,0,end_date) as end_date
				, 1 AS event_type
				, NULL
			FROM #cohort_rows
		) RAWDATA
	) e
	WHERE (2 * e.start_ordinal) - e.overall_ord = 0
),
cteEnds (person_id, start_date, end_date) AS
(
	SELECT
		 c.person_id
		, c.start_date
		, MIN(e.end_date) AS end_date
	FROM #cohort_rows c
	JOIN cteEndDates e ON c.person_id = e.person_id AND e.end_date >= c.start_date
	GROUP BY c.person_id, c.start_date
)
select person_id, min(start_date) as start_date, end_date
into #final_cohort
from cteEnds
group by person_id, end_date
;

DELETE FROM @target_database_schema.@target_cohort_table where cohort_definition_id = @target_cohort_id;
INSERT INTO @target_database_schema.@target_cohort_table (cohort_definition_id, subject_id, cohort_start_date, cohort_end_date)
select @target_cohort_id as cohort_definition_id, person_id, start_date, end_date
FROM #final_cohort CO
;


TRUNCATE TABLE #strategy_ends;
DROP TABLE #strategy_ends;


TRUNCATE TABLE #cohort_rows;
DROP TABLE #cohort_rows;

TRUNCATE TABLE #final_cohort;
DROP TABLE #final_cohort;

TRUNCATE TABLE #inclusion_events;
DROP TABLE #inclusion_events;

TRUNCATE TABLE #qualified_events;
DROP TABLE #qualified_events;

TRUNCATE TABLE #included_events;
DROP TABLE #included_events;

TRUNCATE TABLE #Codesets;
DROP TABLE #Codesets;
