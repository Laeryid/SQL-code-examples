DROP TABLE IF EXISTS dwh.relations;
CREATE TABLE dwh.relations
(
	rel_id int default nextval('dwh.relations_seq')
	, rel_source_id int
	, rel_target_id int
	, rel_type varchar(50)
	, source varchar(60)
	, source_rel_source_id numeric(20, 0)
	, source_rel_target_id numeric(20, 0)
	, rel_source_type varchar(50)
	, rel_target_type varchar(50)
);

ALTER SEQUENCE dwh.relations_seq RESTART WITH 1;

SELECT source_node_id, node_type, node_id , source  
INTO TEMP TABLE nodes
FROM dwh.nodes
where source not in ('openownership', 'cyprus')
UNION ALL 
SELECT source_address_id, 'address', address_id, source FROM dwh.addresses
where source not in ('openownership', 'cyprus')
;

CREATE UNIQUE INDEX ON nodes
(source, source_node_id);

INSERT INTO dwh.relations
(rel_source_id, rel_target_id, rel_type, source, source_rel_source_id, source_rel_target_id
 	, rel_source_type, rel_target_type)
SELECT s.node_id rel_source_id
		, t.node_id rel_target_id
		, CASE 
			WHEN rel.rel_type like '%same%' or rel.rel_type like '%similar%' 
				THEN 'same'
			ELSE REPLACE(rel.rel_type, '_of', '') 
		END rel_type
		, rel.sourceid as source
		, rel.node_id_start as source_rel_source_id
		, rel.node_id_end as source_rel_target_id
		, s.node_type rel_source_type
		, t.node_type rel_target_type
FROM raw_data.icij_relationships rel
LEFT JOIN nodes s
	ON s.source_node_id = rel.node_id_start
	AND s.source = rel.sourceid
LEFT JOIN nodes t
	ON t.source_node_id = rel.node_id_end
	AND t.source = rel.sourceid
;

DROP TABLE nodes;

DELETE FROM dwh.relations where rel_type = 'city';
INSERT INTO dwh.relations
(rel_source_id, rel_target_id, rel_type, source, source_rel_source_id, source_rel_target_id
 	, rel_source_type, rel_target_type)
SELECT adr.address_id, ct.city_id, 'city', adr.source, adr.source_address_id, ct.city_id, 'address', 'city' 
FROM dwh.addresses adr
INNER JOIN dwh.cities ct
	ON ct.city_id = adr.city_id
;
DELETE FROM dwh.relations where rel_type = 'country';
INSERT INTO dwh.relations
(rel_source_id, rel_target_id, rel_type, source, source_rel_source_id, source_rel_target_id
 	, rel_source_type, rel_target_type)
SELECT adr.address_id, ct.country_id, 'country', adr.source, adr.source_address_id, ct.country_id, 'address', 'country' 
FROM dwh.addresses adr
INNER JOIN dwh.countries ct
	ON ct.country_id = adr.country_id
;

INSERT INTO dwh.relations
(rel_source_id, rel_target_id, rel_type, source, source_rel_source_id, source_rel_target_id
 	, rel_source_type, rel_target_type)
SELECT distinct nd.node_id, ct.country_id, 'jurisdiction', s.sourceid, s.node_id, ct.country_id, nd.node_type, 'country'
FROM raw_data.icij_nodes_entities s
INNER JOIN dwh.countries ct
	ON (s.jurisdiction like '%'||ct.country_code3||'%'
		OR s.jurisdiction_description like '%'||ct.country_name||'%')
INNER JOIN dwh.nodes nd
	ON nd.source_node_id = s.node_id
	AND nd.node_type = 'entity'
	AND nd.source = s.sourceid
;

INSERT INTO dwh.relations
(rel_source_id, rel_target_id, rel_type, source, source_rel_source_id, source_rel_target_id
 	, rel_source_type, rel_target_type)
SELECT distinct nd.node_id, ct.country_id, 'jurisdiction', s.sourceid, s.node_id, ct.country_id, nd.node_type, 'country'
FROM raw_data.icij_nodes_others s
INNER JOIN dwh.countries ct
	ON (s.jurisdiction like '%'||ct.country_code3||'%'
		OR s.jurisdiction_description like '%'||ct.country_name||'%')
INNER JOIN dwh.nodes nd
	ON nd.source_node_id = s.node_id
	AND nd.node_type = 'other'
	AND nd.source = s.sourceid
;


-- openownership
DELETE FROM dwh.relations where source = 'openownership'

SELECT node_id, source_node_id, node_type
into temp table nodes
FROM dwh.nodes
where source = 'openownership'
;

CREATE INDEX on nodes (source_node_id)
;

INSERT INTO dwh.relations
(rel_source_id, rel_target_id, rel_type, source, source_rel_source_id, source_rel_target_id
 	, rel_source_type, rel_target_type)
SELECT n_s.node_id, n_i.node_id
	, CASE WHEN n_s.node_type <> n_i.node_type THEN 'own/control' 
		ELSE 'other' END rel_type
	, 'openownership', rel.subject_statement_id, rel.interest_statement_id
	, n_s.node_type, n_i.node_type
FROM raw_data.openownership_relationships rel
INNER JOIN nodes n_s
	ON n_s.source_node_id = rel.subject_statement_id
INNER JOIN nodes n_i
	ON n_i.source_node_id = rel.interest_statement_id	
;

INSERT INTO dwh.relations
(rel_source_id, rel_target_id, rel_type, source, source_rel_source_id, source_rel_target_id
 	, rel_source_type, rel_target_type)
SELECT n_s.node_id, n_i.address_id
	, 'address' rel_type
	, 'openownership', n_s.source_node_id, n_i.source_address_id	
	, n_s.node_type, 'address'
FROM nodes n_s
INNER JOIN dwh.addresses n_i
	ON n_s.source_node_id = n_i.source_address_id	
;

INSERT INTO dwh.relations
(rel_source_id, rel_target_id, rel_type, source, source_rel_source_id, source_rel_target_id
 	, rel_source_type, rel_target_type)
SELECT n.node_id, ctr.country_id, 'jurisdiction', 'openownership', s.statementid, ctr.country_id
	, 'entity', 'country' 
FROM raw_data.openownership_entities s
INNER JOIN dwh.countries as ctr
	ON ctr.country_code = s.jurisdiction_code
INNER JOIN nodes n
	ON n.source_node_id = s.statementid
;

DROP TABLE nodes;

-- Cyprus
INSERT INTO dwh.relations
(rel_source_id, rel_target_id, rel_type, source, source_rel_source_id, source_rel_target_id
 	, rel_source_type, rel_target_type)
SELECT en.node_id, pr.node_id, 'owner/control', 'cyprus', cp.registration_no, cp.person_id
	, en.node_type, pr.node_type
FROM raw_data.cyprus_persons cp
INNER JOIN dwh.nodes en
	ON en.source_node_id = cp.registration_no
	AND en.node_type = 'entity'
INNER JOIN dwh.nodes pr
	ON pr.source_node_id = cp.person_id
	AND pr.node_type = 'officer'
;

INSERT INTO dwh.relations
(rel_source_id, rel_target_id, rel_type, source, source_rel_source_id, source_rel_target_id
 	, rel_source_type, rel_target_type)
SELECT en.node_id, ad.address_id, 'address', 'cyprus', og.registration_no, og.address_seq_no
	, en.node_type, 'address'
FROM raw_data.cyprus_organisations og
INNER JOIN dwh.nodes en
	ON en.source_node_id = og.registration_no
	AND en.node_type = 'entity'
INNER JOIN dwh.addresses ad
	ON ad.source_address_id = og.address_seq_no
;


INSERT INTO dwh.relations
(rel_source_id, rel_target_id, rel_type, source, source_rel_source_id, source_rel_target_id
 	, rel_source_type, rel_target_type)
SELECT en.node_id, ctr.country_id, 'jurisdiction', 'cyprus', og.registration_no, ctr.country_id
	, en.node_type, 'country'
FROM raw_data.cyprus_organisations og
INNER JOIN dwh.nodes en
	ON en.source_node_id = og.registration_no
	AND en.node_type = 'entity'
INNER JOIN dwh.countries ctr
	ON ctr.country_code = 'CY'
	