TRUNCATE TABLE dwh.addresses;

INSERT INTO dwh.addresses
(address, source, source_address_id, city_id, city
 , country_id, country_code, country_code3, country_name
)
SELECT s.address, s.sourceid, s.node_id, s.city_id, ct.city_name
		, ctr.country_id, ctr.country_code, ctr.country_code3, ctr.country_name
FROM (SELECT cta.alt_city_name
	  		, cta.country_code
	  		, cta.city_id
			, s.node_id
	  		, s.address
	  		, s.sourceid
			, row_number() over(partition by s.node_id order by cta.city_id, length(cta.alt_city_name) desc, cta.alt_city_name) as rn
	FROM raw_data.icij_nodes_addresses s
	inner join dwh.cities_alt_names as cta
		ON s.address like '%' || cta.alt_city_name || '%'
		AND s.country_codes = cta.country_code3
	--where s.node_id between 24000001 and 24001000
) as s
INNER JOIN dwh.cities ct
	ON ct.city_id = s.city_id
INNER JOIN dwh.countries ctr
	ON ctr.country_code = s.country_code
where rn = 1;

WITH RECURSIVE parse_address as (
	SELECT node_id
		, RIGHT(address, length(address) - length(SPLIT_PART(address, ',', 1)) - 1) address
		, SPLIT_PART(address, ',', 1) address_part
	FROM raw_data.icij_nodes_addresses
	UNION ALL
	SELECT node_id
		, CASE WHEN SPLIT_PART(address, ',', 1) = address
			THEN  ''
			ELSE RIGHT(address, length(address) - length(SPLIT_PART(address, ',', 1)) - 1)
			END address
		, SPLIT_PART(address, ',', 1) address_part
	FROM parse_address
	WHERE address <> '')
SELECT node_id, address_part
INTO TEMP TABLE address_parts
FROM parse_address
;


INSERT INTO dwh.addresses
(address, source, source_address_id, city_id, city
 , country_id, country_code, country_code3, country_name
)
SELECT s.address, s.sourceid, s.node_id, s.city_id, ct.city_name
		, ctr.country_id, ctr.country_code, ctr.country_code3, ctr.country_name
FROM (SELECT cta.alt_city_name
	  		, cta.country_code
	  		, cta.city_id
			, s.node_id
	  		, ina.address
	  		, ina.sourceid
			, row_number() over(partition by s.node_id order by cta.city_id, length(cta.alt_city_name) desc, cta.alt_city_name) as rn
	FROM address_parts s
	LEFT JOIN dwh.addresses exc
		ON s.node_id = exc.source_address_id
	inner join dwh.cities_alt_names as cta
		ON TRIM(BOTH FROM s.address_part) = cta.alt_city_name
	inner join raw_data.icij_nodes_addresses ina
	  	ON ina.node_id = s.node_id
	WHERE exc.source_address_id is null
) as s
INNER JOIN dwh.cities ct
	ON ct.city_id = s.city_id
INNER JOIN dwh.countries ctr
	ON ctr.country_code = s.country_code
where rn = 1;

DROP TABLE  address_parts;

INSERT INTO dwh.addresses
(address, source, source_address_id
 , country_id, country_code, country_code3, country_name
)
SELECT s.address, s.sourceid, s.node_id
		, ctr.country_id, ctr.country_code, ctr.country_code3, ctr.country_name
FROM raw_data.icij_nodes_addresses s
LEFT JOIN dwh.addresses exc
	ON s.node_id = exc.source_address_id
LEFT JOIN dwh.countries ctr
	ON ctr.country_code3 = s.country_codes
WHERE exc.source_address_id is null
;

-- openownership
WITH RECURSIVE parse_address as (
	SELECT address_id
		, RIGHT(address, length(address) - length(SPLIT_PART(address, ',', 1)) - 1) address
		, SPLIT_PART(address, ',', 1) address_part
		, s.country
	FROM raw_data.openownership_addresses s
	--WHERE not exists (SELECT 1 FROM dwh.addresses exc 
	--				 WHERE exc.source = 'openownership'
	--				 AND exc.source_address_id = s.statementid)
	UNION ALL
	SELECT address_id
		, CASE WHEN SPLIT_PART(address, ',', 1) = address
			THEN  ''
			ELSE RIGHT(address, length(address) - length(SPLIT_PART(address, ',', 1)) - 1)
			END address
		, SPLIT_PART(address, ',', 1) address_part
		, country
	FROM parse_address
	WHERE address <> '')
SELECT address_id, TRIM(BOTH FROM address_part) address_part, country
INTO TEMP TABLE o_address_parts
FROM parse_address
;

--CREATE INDEX ON o_address_parts (country, address_part) INCLUDE(address_id);

--CREATE INDEX ON dwh.cities_alt_names (country_code, alt_city_name) INCLUDE(city_id);

INSERT INTO dwh.addresses
(address, source, source_address_id, city_id, city
 , country_id, country_code, country_code3, country_name
)
SELECT s.address, 'openownership', s.statementid, s.city_id, ct.city_name
		, ctr.country_id, ctr.country_code, ctr.country_code3, ctr.country_name
FROM (SELECT cta.alt_city_name
	  		, cta.country_code
	  		, cta.city_id
			, opa.statementid
	  		, opa.address
			, row_number() over(partition by opa.statementid order by cta.city_id, length(cta.alt_city_name) desc, cta.alt_city_name) as rn
	FROM o_address_parts s
	inner join dwh.cities_alt_names as cta
		ON s.address_part = cta.alt_city_name
	  	AND s.country = cta.country_code
	inner join raw_data.openownership_addresses opa
	  	ON opa.address_id = s.address_id
	--LEFT JOIN dwh.addresses exc
	--	ON opa.statementid = exc.source_address_id
	--WHERE exc.source_address_id is null
) as s
INNER JOIN dwh.cities ct
	ON ct.city_id = s.city_id
INNER JOIN dwh.countries ctr
	ON ctr.country_code = s.country_code
where rn = 1;

/*
INSERT INTO dwh.addresses
(address, source, source_address_id, city_id, city
 , country_id, country_code, country_code3, country_name
)
SELECT s.address, 'openownership', s.statementid, s.city_id, ct.city_name
		, ctr.country_id, ctr.country_code, ctr.country_code3, ctr.country_name
FROM (SELECT cta.alt_city_name
	  		, cta.country_code
	  		, cta.city_id
			, opa.statementid
	  		, opa.address
			, row_number() over(partition by opa.statementid order by cta.city_id, length(cta.alt_city_name) desc, cta.alt_city_name) as rn
	FROM o_address_parts s
	inner join dwh.cities_alt_names as cta
		ON s.address_part like '%'||cta.alt_city_name||'%'
	  	AND s.country = cta.country_code
	inner join raw_data.openownership_addresses opa
	  	ON opa.address_id = s.address_id
	LEFT JOIN dwh.addresses exc
		ON opa.statementid = exc.source_address_id
		AND exc.source = 'openownership'
	WHERE exc.source_address_id is null
) as s
INNER JOIN dwh.cities ct
	ON ct.city_id = s.city_id
INNER JOIN dwh.countries ctr
	ON ctr.country_code = s.country_code
where rn = 1;
*/
DROP TABLE  o_address_parts;
 

SELECT REPLACE(s.address, ' ', ',') address, s.country, s.statementid
INTO TEMP TABLE left_openownership_addresses
FROM raw_data.openownership_addresses s
LEFT JOIN dwh.addresses exc
	ON s.statementid = exc.source_address_id
WHERE exc.source_address_id is null
;


WITH RECURSIVE parse_address as (
	SELECT statementid
		, RIGHT(address, length(address) - length(SPLIT_PART(address, ',', 1)) - 1) address
		, SPLIT_PART(address, ',', 1) address_part
		, s.country
	FROM left_openownership_addresses s
	--WHERE not exists (SELECT 1 FROM dwh.addresses exc 
	--				 WHERE exc.source = 'openownership'
	--				 AND exc.source_address_id = s.statementid)
	UNION ALL
	SELECT statementid
		, CASE WHEN SPLIT_PART(address, ',', 1) = address
			THEN  ''
			ELSE RIGHT(address, length(address) - length(SPLIT_PART(address, ',', 1)) - 1)
			END address
		, SPLIT_PART(address, ',', 1) address_part
		, country
	FROM parse_address
	WHERE address <> '')
SELECT statementid, TRIM(BOTH FROM address_part) address_part, country
INTO TEMP TABLE o_address_parts
FROM parse_address

;

SELECT city_id, country_code, alt_city_name, length(alt_city_name) name_len
	, SPLIT_PART(REPLACE(alt_city_name, ' ', ','), ',', 1) short_name
INTO TEMP TABLE cities_alt_names
FROM dwh.cities_alt_names
;

		INSERT INTO dwh.addresses
		(address, source, source_address_id, city_id, city
		 , country_id, country_code, country_code3, country_name
		)
		SELECT s.address, 'openownership', s.statementid, s.city_id, ct.city_name
				, ctr.country_id, ctr.country_code, ctr.country_code3, ctr.country_name
		FROM (SELECT cta.alt_city_name
					, cta.country_code
					, cta.city_id
					, adr.statementid
					, adr.address
					, row_number() over(partition by s.statementid order by cta.city_id, length(cta.alt_city_name) desc, cta.alt_city_name) as rn
			FROM o_address_parts as s
			inner join cities_alt_names as cta
				ON s.address_part = cta.short_name
				AND s.country = cta.country_code
			LEFT JOIN dwh.addresses exc
				ON s.statementid = exc.source_address_id
				AND exc.source = 'openownership'
			INNER JOIN raw_data.openownership_addresses adr
			  	ON adr.statementid = s.statementid
			    AND adr.address like '%'|| cta.alt_city_name ||'%'
			WHERE exc.source_address_id is null
			--where s.node_id between 24000001 and 24001000
		) as s
		INNER JOIN dwh.cities ct
			ON ct.city_id = s.city_id
		INNER JOIN dwh.countries ctr
			ON ctr.country_code = s.country_code
		where rn = 1;


INSERT INTO dwh.addresses
(address, source, source_address_id
 , country_id, country_code, country_code3, country_name
)
SELECT s.address, 'openownership', statementid
	, ct.country_id, ct.country_code, ct.country_code3, ct.country_name
FROM left_openownership_addresses as s
LEFT JOIN dwh.countries ct
	ON ct.country_code = s.country
LEFT JOIN dwh.addresses exc
	ON s.statementid = exc.source_address_id
	AND exc.source = 'openownership'
WHERE exc.source_address_id is null

;

DROP TABLE o_address_parts;
DROP TABLE left_openownership_addresses;

-- cyprus

WITH RECURSIVE parse_address as (
	SELECT address_seq_no
		, RIGHT(territory, length(territory) - length(SPLIT_PART(territory, ',', 1)) - 1) address
		, SPLIT_PART(territory, ',', 1) address_part
	FROM raw_data.cyprus_addresses s
	UNION ALL
	SELECT address_seq_no
		, CASE WHEN SPLIT_PART(address, ',', 1) = address
			THEN  ''
			ELSE RIGHT(address, length(address) - length(SPLIT_PART(address, ',', 1)) - 1)
			END address
		, SPLIT_PART(address, ',', 1) address_part
	FROM parse_address
	WHERE address <> '')
SELECT address_seq_no, TRIM(BOTH FROM address_part) address_part
INTO TEMP TABLE c_address_parts
FROM parse_address;



INSERT INTO dwh.addresses
(address, source, source_address_id, city_id, city
		 , country_id, country_code, country_code3, country_name
)
SELECT ca.territory || ', ' || ca.street || ', ' || ca.building
	, 'cyprus', ca.address_seq_no
	, ca.city_id, ct.city_name
	, ctr.country_id, ctr.country_code, ctr.country_code3, ctr.country_name
FROM (SELECT addres.territory, addres.street, addres.building, ca.address_seq_no
	  	, cn.city_id, cn.country_code
	  , row_number() over(partition by ca.address_seq_no order by cn.city_id, length(cn.alt_city_name) desc, cn.alt_city_name)  as rn
	  
	  FROM  c_address_parts ca
	  INNER JOIN dwh.cities_alt_names cn	
	  	ON ca.address_part = cn.alt_city_name
	  INNER JOIN raw_data.cyprus_addresses addres
	  	ON addres.address_seq_no = ca.address_seq_no
		) as ca
INNER JOIN dwh.cities ct
	ON ct.city_id = ca.city_id
INNER JOIN dwh.countries ctr
	ON ctr.country_code = ca.country_code
WHERE ca.rn = 1
;

INSERT INTO dwh.addresses
(address, source, source_address_id, city_id, city
		 , country_id, country_code, country_code3, country_name
)
SELECT ca.territory || ', ' || ca.street || ', ' || ca.building
	, 'cyprus', ca.address_seq_no
	, ca.city_id, ct.city_name
	, ctr.country_id, ctr.country_code, ctr.country_code3, ctr.country_name
FROM (SELECT ca.territory, ca.street, ca.building, ca.address_seq_no
	  	, cn.city_id, cn.country_code
	  , row_number() over(partition by ca.address_seq_no order by cn.city_id, length(cn.alt_city_name) desc, cn.alt_city_name)  as rn
	  
	  FROM  raw_data.cyprus_addresses ca
	  INNER JOIN dwh.cities_alt_names cn	
	  	ON ca.territory like '%'|| cn.alt_city_name ||'%'
	  LEFT JOIN dwh.addresses exc
	  	ON exc.source_address_id = ca.address_seq_no
		AND exc.source = 'cyprus'
	  WHERE exc.source_address_id is null
		) as ca
INNER JOIN dwh.cities ct
	ON ct.city_id = ca.city_id
INNER JOIN dwh.countries ctr
	ON ctr.country_code = ca.country_code
WHERE ca.rn = 1
;

INSERT INTO dwh.addresses
(address, source, source_address_id
		 , country_id, country_code, country_code3, country_name
)
SELECT ca.territory || ', ' || ca.street || ', ' || ca.building
	, 'cyprus', ca.address_seq_no
	, ctr.country_id, ctr.country_code, ctr.country_code3, ctr.country_name
FROM  raw_data.cyprus_addresses ca
LEFT JOIN dwh.addresses exc
	  	ON exc.source_address_id = ca.address_seq_no
		AND exc.source = 'cyprus'
INNER JOIN dwh.countries ctr
	ON ctr.country_code = 'CY'
WHERE exc.source_address_id is null
;

SELECT source, count(1) FROM dwh.addresses GROUP BY source;