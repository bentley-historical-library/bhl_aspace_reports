require 'db/migrations/utils'

#Converted all SQL functions to start with BHL_ to prevent conflicts with ArchivesSpace

Sequel.migration do
  up do
    if $db_type == :mysql

run "DROP FUNCTION IF EXISTS BHL_GetAccessionClassificationsUserDefined;"
run <<EOF
CREATE FUNCTION BHL_GetAccessionClassificationsUserDefined(f_accession_id INT)
	RETURNS VARCHAR(255)
	READS SQL DATA
BEGIN
	DECLARE f_value VARCHAR(255);

	SELECT 
		CONCAT_WS("; ", BHL_GetEnumValue(enum_1_id), BHL_GetEnumValue(enum_2_id), BHL_GetEnumValue(enum_3_id)) INTO f_value
	FROM
		user_defined
	WHERE
		accession_id = f_accession_id;

	RETURN f_value;
END
EOF

run "DROP FUNCTION IF EXISTS BHL_GetAccessionDonorNumbers;"
run <<EOF
CREATE FUNCTION BHL_GetAccessionDonorNumbers(f_accession_id INT)
	RETURNS VARCHAR(255)
	READS SQL DATA
BEGIN
	DECLARE f_value VARCHAR(255);

	SELECT
		GROUP_CONCAT(donor_number SEPARATOR "; ") into f_value
	FROM
		donor_detail
	JOIN
		linked_agents_rlshp ON linked_agents_rlshp.`accession_id` = f_accession_id
	WHERE
		(donor_detail.`agent_person_id` = linked_agents_rlshp.`agent_person_id` OR donor_detail.`agent_corporate_entity_id` = linked_agents_rlshp.`agent_corporate_entity_id` OR donor_detail.`agent_family_id` = linked_agents_rlshp.`agent_family_id`)
	AND 
		BINARY BHL_GetEnumValue(linked_agents_rlshp.`role_id`) = BINARY 'source';	

	RETURN f_value;
END
EOF

run "DROP  FUNCTION IF EXISTS BHL_GetAccessionExtentNumberType;"
run <<EOF
CREATE FUNCTION BHL_GetAccessionExtentNumberType(f_accession_id INT) 
    RETURNS VARCHAR(255)
    READS SQL DATA
BEGIN
    DECLARE f_value VARCHAR(255);

    SELECT 
    	GROUP_CONCAT(CONCAT(number, " ", BHL_GetEnumValue(extent_type_id)) SEPARATOR "; ") into f_value
    FROM
    	extent
	WHERE
		extent.`accession_id` = f_accession_id;
	RETURN f_value;   
END 
EOF

run "DROP FUNCTION IF EXISTS BHL_GetAccessionLocationUserDefined;"
run <<EOF
CREATE FUNCTION BHL_GetAccessionLocationUserDefined(f_accession_id INT)
	RETURNS VARCHAR(1024)
	READS SQL DATA
BEGIN
	DECLARE f_value VARCHAR(1024);
    
    SELECT
        text_1 into f_value
    FROM
        user_defined
    WHERE
        user_defined.`accession_id` = f_accession_id;
    
    RETURN f_value;
END
EOF

run "DROP FUNCTION IF EXISTS BHL_GetAccessionProcessingPriority;"
run <<EOF
CREATE FUNCTION BHL_GetAccessionProcessingPriority(f_accession_id INT)
	RETURNS VARCHAR(255)
	READS SQL DATA
BEGIN
	DECLARE f_value VARCHAR(255);

	SELECT
		value into f_value
	FROM
		enumeration_value
	JOIN
		collection_management ON collection_management.`accession_id` = f_accession_id
	WHERE
		enumeration_value.`id` = collection_management.`processing_priority_id`;

	RETURN f_value;
END
EOF

run "DROP FUNCTION IF EXISTS BHL_GetAccessionProcessingStatus;"
run <<EOF
CREATE FUNCTION BHL_GetAccessionProcessingStatus(f_accession_id INT)
	RETURNS VARCHAR(255)
	READS SQL DATA
BEGIN
	DECLARE f_value VARCHAR(255);

	SELECT
		value into f_value
	FROM
		enumeration_value
	JOIN
		collection_management ON collection_management.`accession_id` = f_accession_id
	WHERE
		enumeration_value.`id` = collection_management.`processing_status_id`;

	RETURN f_value;
END
EOF

run "DROP FUNCTION IF EXISTS BHL_GetAccessionSourceName;"
run <<EOF
CREATE FUNCTION BHL_GetAccessionSourceName(f_accession_id INT)
	RETURNS VARCHAR(1024)
	READS SQL DATA
BEGIN
	DECLARE f_value VARCHAR(1024);

		SELECT 
			GROUP_CONCAT(BHL_GetAgentSortName(T1.`agent_person_id`, T1.`agent_family_id`, T1.`agent_corporate_entity_id`) SEPARATOR '; ') INTO f_value
		FROM 
			`linked_agents_rlshp` T1
		WHERE
			T1.`accession_id` = f_accession_id
		AND
			BINARY BHL_GetEnumValue(T1.`role_id`) = BINARY 'source';
	RETURN f_value;
END
EOF

run "DROP FUNCTION IF EXISTS BHL_GetAgentAddress1;"
run <<EOF
CREATE FUNCTION BHL_GetAgentAddress1(f_person_id INT, f_family_id INT, f_corporate_id INT)
	RETURNS VARCHAR(255)
	READS SQL DATA
BEGIN
	DECLARE f_value VARCHAR(255);

	IF f_person_id IS NOT NULL THEN
		SELECT address_1 INTO f_value FROM agent_contact WHERE agent_person_id = f_person_id LIMIT 1;
	ELSEIF f_family_id IS NOT NULL THEN
		SELECT address_1 INTO f_value FROM agent_contact WHERE agent_family_id = f_family_id LIMIT 1;
	ELSEIF f_corporate_id IS NOT NULL THEN
		SELECT address_1 INTO f_value FROM agent_contact WHERE agent_corporate_entity_id = f_corporate_id LIMIT 1;
	ELSE
		SET f_value = '';
	END IF;

	RETURN f_value;
END
EOF

run "DROP FUNCTION IF EXISTS BHL_GetAgentAddress2;"
run <<EOF
CREATE FUNCTION BHL_GetAgentAddress2(f_person_id INT, f_family_id INT, f_corporate_id INT)
	RETURNS VARCHAR(255)
	READS SQL DATA
BEGIN
	DECLARE f_value VARCHAR(255);

	IF f_person_id IS NOT NULL THEN
		SELECT address_2 INTO f_value FROM agent_contact WHERE agent_person_id = f_person_id LIMIT 1;
	ELSEIF f_family_id IS NOT NULL THEN
		SELECT address_2 INTO f_value FROM agent_contact WHERE agent_family_id = f_family_id LIMIT 1;
	ELSEIF f_corporate_id IS NOT NULL THEN
		SELECT address_2 INTO f_value FROM agent_contact WHERE agent_corporate_entity_id = f_corporate_id LIMIT 1;
	ELSE
		SET f_value = '';
	END IF;

	RETURN f_value;
END
EOF

run "DROP FUNCTION IF EXISTS BHL_GetAgentCity;"
run <<EOF
CREATE FUNCTION BHL_GetAgentCity(f_person_id INT, f_family_id INT, f_corporate_id INT)
	RETURNS VARCHAR(255)
	READS SQL DATA
BEGIN
	DECLARE f_value VARCHAR(255);

	IF f_person_id IS NOT NULL THEN
		SELECT city INTO f_value FROM agent_contact WHERE agent_person_id = f_person_id LIMIT 1;
	ELSEIF f_family_id IS NOT NULL THEN
		SELECT city INTO f_value FROM agent_contact WHERE agent_family_id = f_family_id LIMIT 1;
	ELSEIF f_corporate_id IS NOT NULL THEN
		SELECT city INTO f_value FROM agent_contact WHERE agent_corporate_entity_id = f_corporate_id LIMIT 1;
	ELSE
		SET f_value = '';
	END IF;

	RETURN f_value;
END
EOF

run "DROP FUNCTION IF EXISTS BHL_GetAgentCountry;"
run <<EOF
CREATE FUNCTION BHL_GetAgentCountry(f_person_id INT, f_family_id INT, f_corporate_id INT)
	RETURNS VARCHAR(255)
	READS SQL DATA
BEGIN
	DECLARE f_value VARCHAR(255);

	IF f_person_id IS NOT NULL THEN
		SELECT country INTO f_value FROM agent_contact WHERE agent_person_id = f_person_id LIMIT 1;
	ELSEIF f_family_id IS NOT NULL THEN
		SELECT country INTO f_value FROM agent_contact WHERE agent_family_id = f_family_id LIMIT 1;
	ELSEIF f_corporate_id IS NOT NULL THEN
		SELECT country INTO f_value FROM agent_contact WHERE agent_corporate_entity_id = f_corporate_id LIMIT 1;
	ELSE
		SET f_value = '';
	END IF;

	RETURN f_value;
END
EOF

run "DROP FUNCTION IF EXISTS BHL_GetAgentDARTLID;"
run <<EOF
CREATE FUNCTION BHL_GetAgentDARTLID(f_person_id INT, f_family_id INT, f_corporate_id INT)
	RETURNS VARCHAR(255)
	READS SQL DATA
BEGIN
	DECLARE f_value VARCHAR(255);

	IF f_person_id IS NOT NULL THEN
		SELECT dart_id INTO f_value FROM donor_detail WHERE agent_person_id = f_person_id LIMIT 1;
	ELSEIF f_family_id IS NOT NULL THEN
		SELECT dart_id INTO f_value FROM donor_detail WHERE agent_family_id = f_family_id LIMIT 1;
	ELSEIF f_corporate_id IS NOT NULL THEN
		SELECT dart_id INTO f_value FROM donor_detail WHERE agent_corporate_entity_id = f_corporate_id LIMIT 1;
	ELSE
		SET f_value = '';
	END IF;

	RETURN f_value;
END
EOF

run "DROP FUNCTION IF EXISTS BHL_GetAgentDonorNumber;"
run <<EOF
CREATE FUNCTION BHL_GetAgentDonorNumber(f_person_id INT, f_family_id INT, f_corporate_id INT)
	RETURNS VARCHAR(255)
	READS SQL DATA
BEGIN
	DECLARE f_value VARCHAR(255);

	IF f_person_id IS NOT NULL THEN
		SELECT donor_number INTO f_value FROM donor_detail WHERE agent_person_id = f_person_id LIMIT 1;
	ELSEIF f_family_id IS NOT NULL THEN
		SELECT donor_number INTO f_value FROM donor_detail WHERE agent_family_id = f_family_id LIMIT 1;
	ELSEIF f_corporate_id IS NOT NULL THEN
		SELECT donor_number INTO f_value FROM donor_detail WHERE agent_corporate_entity_id = f_corporate_id LIMIT 1;
	ELSE
		SET f_value = '';
	END IF;

	RETURN f_value;
END
EOF

run "DROP FUNCTION IF EXISTS BHL_GetAgentLastName;"
run <<EOF
CREATE FUNCTION BHL_GetAgentLastName(f_person_id INT, f_family_id INT, f_corporate_id INT)
	RETURNS VARCHAR(255)
	READS SQL DATA
BEGIN
	DECLARE f_value VARCHAR(255);

	IF f_person_id IS NOT NULL THEN
		SELECT primary_name INTO f_value FROM name_person WHERE agent_person_id = f_person_id LIMIT 1;
	ELSE
		SET f_value = '';
	END IF;

	RETURN f_value;
END
EOF

run "DROP FUNCTION IF EXISTS BHL_GetAgentOrganizationOrUnit;"
run <<EOF
CREATE FUNCTION BHL_GetAgentOrganizationOrUnit(f_person_id INT, f_family_id INT, f_corporate_id INT)
	RETURNS VARCHAR(255)
	READS SQL DATA
BEGIN
	DECLARE f_value VARCHAR(255);

	IF f_corporate_id IS NOT NULL THEN
		SELECT sort_name INTO f_value FROM name_corporate_entity WHERE agent_corporate_entity_id = f_corporate_id LIMIT 1;
	ELSEIF f_family_id IS NOT NULL THEN
		SELECT sort_name INTO f_value FROM name_family WHERE agent_family_id = f_family_id LIMIT 1;
	ELSE
		SET f_value = '';
	END IF;

	RETURN f_value;
END
EOF

run "DROP FUNCTION IF EXISTS BHL_GetAgentRestOfName;"
run <<EOF
CREATE FUNCTION BHL_GetAgentRestOfName(f_person_id INT, f_family_id INT, f_corporate_id INT)
	RETURNS VARCHAR(255)
	READS SQL DATA
BEGIN
	DECLARE f_value VARCHAR(255);

	IF f_person_id IS NOT NULL THEN
		SELECT rest_of_name INTO f_value FROM name_person WHERE agent_person_id = f_person_id LIMIT 1;
	ELSE
		SET f_value = '';
	END IF;

	RETURN f_value;
END
EOF

run "DROP FUNCTION IF EXISTS BHL_GetAgentState;"
run <<EOF
CREATE FUNCTION BHL_GetAgentState(f_person_id INT, f_family_id INT, f_corporate_id INT)
	RETURNS VARCHAR(255)
	READS SQL DATA
BEGIN
	DECLARE f_value VARCHAR(255);

	IF f_person_id IS NOT NULL THEN
		SELECT region INTO f_value FROM agent_contact WHERE agent_person_id = f_person_id LIMIT 1;
	ELSEIF f_family_id IS NOT NULL THEN
		SELECT region INTO f_value FROM agent_contact WHERE agent_family_id = f_family_id LIMIT 1;
	ELSEIF f_corporate_id IS NOT NULL THEN
		SELECT region INTO f_value FROM agent_contact WHERE agent_corporate_entity_id = f_corporate_id LIMIT 1;
	ELSE
		SET f_value = '';
	END IF;

	RETURN f_value;
END
EOF

run "DROP FUNCTION IF EXISTS BHL_GetAgentSuffix;"
run <<EOF
CREATE FUNCTION BHL_GetAgentSuffix(f_person_id INT, f_family_id INT, f_corporate_id INT)
	RETURNS VARCHAR(255)
	READS SQL DATA
BEGIN
	DECLARE f_value VARCHAR(255);

	IF f_person_id IS NOT NULL THEN
		SELECT suffix INTO f_value FROM name_person WHERE agent_person_id = f_person_id LIMIT 1;
	ELSE
		SET f_value = '';
	END IF;

	RETURN f_value;
END
EOF

run "DROP FUNCTION IF EXISTS BHL_GetAgentTitle;"
run <<EOF
CREATE FUNCTION BHL_GetAgentTitle(f_person_id INT, f_family_id INT, f_corporate_id INT)
	RETURNS VARCHAR(255)
	READS SQL DATA
BEGIN
	DECLARE f_value VARCHAR(255);

	IF f_person_id IS NOT NULL THEN
		SELECT title INTO f_value FROM name_person WHERE agent_person_id = f_person_id LIMIT 1;
	ELSE
		SET f_value = '';
	END IF;

	RETURN f_value;
END
EOF

run "DROP FUNCTION IF EXISTS BHL_GetAgentZipCode;"
run <<EOF
CREATE FUNCTION BHL_GetAgentZipCode(f_person_id INT, f_family_id INT, f_corporate_id INT)
	RETURNS VARCHAR(255)
	READS SQL DATA
BEGIN
	DECLARE f_value VARCHAR(255);

	IF f_person_id IS NOT NULL THEN
		SELECT post_code INTO f_value FROM agent_contact WHERE agent_person_id = f_person_id LIMIT 1;
	ELSEIF f_family_id IS NOT NULL THEN
		SELECT post_code INTO f_value FROM agent_contact WHERE agent_family_id = f_family_id LIMIT 1;
	ELSEIF f_corporate_id IS NOT NULL THEN
		SELECT post_code INTO f_value FROM agent_contact WHERE agent_corporate_entity_id = f_corporate_id LIMIT 1;
	ELSE
		SET f_value = '';
	END IF;

	RETURN f_value;
END
EOF


run "DROP FUNCTION IF EXISTS BHL_GetTermType;"
run <<EOF
CREATE FUNCTION BHL_GetTermType(f_subject_id INT)
    RETURNS VARCHAR(255)
    READS SQL DATA
BEGIN
    DECLARE f_term_type VARCHAR(255) DEFAULT "";
    
    SELECT enumeration_value.`value` INTO f_term_type 
    FROM term
    INNER JOIN enumeration_value 
    ON term.`term_type_id` = enumeration_value.`id` 
    WHERE term.`id`  
    IN (SELECT subject_term.`term_id` 
        FROM subject_term 
        WHERE subject_term.`subject_id` = f_subject_id)  
    LIMIT 1;
    
    RETURN f_term_type;
END 
EOF

# Function to return the parent resource record id if 
# resource, or archival_object id is passed in.
run "DROP  FUNCTION IF EXISTS BHL_GetResourceId;"
run <<EOF
CREATE FUNCTION BHL_GetResourceId(f_resource_id INT, f_archival_object_id INT) 
    RETURNS INT
    READS SQL DATA
BEGIN
    DECLARE f_root_record_id INT;   
    
    IF f_resource_id IS NOT NULL THEN
        SET f_root_record_id = f_resource_id;
    ELSE
        SELECT archival_object.`root_record_id` INTO f_root_record_id 
        FROM archival_object 
        WHERE archival_object.`id` = f_archival_object_id;  
    END IF;
    
    RETURN f_root_record_id;
END 
EOF
# Function to return the parent digital object record id if 
# digital_object, or digital_object_component id is passed in.
run "DROP  FUNCTION IF EXISTS BHL_GetDigitalObjectId;"
run <<EOF
CREATE FUNCTION BHL_GetDigitalObjectId(f_digital_object_id INT, f_digital_component_id INT) 
    RETURNS INT
    READS SQL DATA
BEGIN
    DECLARE f_root_record_id INT;   
    
    IF f_digital_object_id IS NOT NULL THEN
        SET f_root_record_id = f_digital_object_id;
    ELSE
        SELECT digital_object_component.`root_record_id` INTO f_root_record_id 
        FROM digital_object_component 
        WHERE digital_object_component.`id` = f_digital_component_id;  
    END IF;
    
    RETURN f_root_record_id;
END 
EOF

# Function to return a coordinate string by concating the three 
# coordinate labels and indicators
run "DROP  FUNCTION IF EXISTS BHL_GetCoordinate;"
run <<EOF
CREATE FUNCTION BHL_GetCoordinate(f_location_id INT) 
    RETURNS VARCHAR(1020)
    READS SQL DATA
BEGIN
    DECLARE f_coordinate VARCHAR(1020); 
        DECLARE f_coordinate_1 VARCHAR(255);
        DECLARE f_coordinate_2 VARCHAR(255);
        DECLARE f_coordinate_3 VARCHAR(255);
        
        -- The three select statements can be combined into 1 query, but for clarity 
        -- are left separate
    SELECT CONCAT(location.`coordinate_1_label`, ' ', location.`coordinate_1_indicator`)  
                INTO f_coordinate_1 
        FROM location 
        WHERE location.`id` = f_location_id;
    
        SELECT CONCAT(location.`coordinate_2_label`, ' ', location.`coordinate_2_indicator`)  
                INTO f_coordinate_2 
        FROM location 
        WHERE location.`id` = f_location_id;

        SELECT CONCAT(location.`coordinate_3_label`, ' ', location.`coordinate_3_indicator`)  
                INTO f_coordinate_3 
        FROM location 
        WHERE location.`id` = f_location_id; 
        
        SET f_coordinate = CONCAT_WS('/', f_coordinate_1, f_coordinate_2, f_coordinate_3);
        
    RETURN f_coordinate;
END 
EOF

# Function to return enum value given an id
run "DROP  FUNCTION IF EXISTS BHL_GetEnumValue;"
run <<EOF
CREATE FUNCTION BHL_GetEnumValue(f_enum_id INT) 
    RETURNS VARCHAR(255)
    READS SQL DATA
BEGIN
    DECLARE f_value VARCHAR(255);   
    SELECT enumeration_value.`value`INTO f_value
    FROM enumeration_value
    WHERE enumeration_value.`id` = f_enum_id;
    RETURN f_value;
END 
EOF

# Function to return the enum value with the first letter capitalize
run "DROP  FUNCTION IF EXISTS BHL_GetEnumValueUF;"
run <<EOF
CREATE FUNCTION BHL_GetEnumValueUF(f_enum_id INT) 
    RETURNS VARCHAR(255)
    READS SQL DATA
BEGIN
    DECLARE f_value VARCHAR(255);   
    DECLARE f_ovalue VARCHAR(255);        
        SET f_ovalue = BHL_GetEnumValue(f_enum_id);
    SET f_value = CONCAT(UCASE(LEFT(f_ovalue, 1)), SUBSTRING(f_ovalue, 2));
    RETURN f_value;
END 
EOF

# Function to return the number of resources for a particular repository
run "DROP  FUNCTION IF EXISTS BHL_GetTotalResources;"
run <<EOF
CREATE FUNCTION BHL_GetTotalResources(f_repo_id INT) 
    RETURNS INT
    READS SQL DATA
BEGIN
    DECLARE f_total INT;    
    
    SELECT COUNT(id) INTO f_total 
    FROM resource 
    WHERE resource.`repo_id` = f_repo_id;
        
    RETURN f_total;
END 
EOF

# Function to return the number of resources with level = item for a 
# particular repository
run "DROP  FUNCTION IF EXISTS BHL_GetTotalResourcesItems;"
run <<EOF
CREATE FUNCTION BHL_GetTotalResourcesItems(f_repo_id INT) 
    RETURNS INT
    READS SQL DATA
BEGIN
    DECLARE f_total INT;    
    
    SELECT COUNT(id) INTO f_total 
    FROM resource 
    WHERE (resource.`repo_id` = f_repo_id
    AND 
    BINARY BHL_GetEnumValue(resource.`level_id`) = BINARY 'item');
        
    RETURN f_total;
END 
EOF

# Function to return the number of resources with restrictions for a 
# particular repository
run "DROP  FUNCTION IF EXISTS BHL_GetResourcesWithRestrictions;"
run <<EOF
CREATE FUNCTION BHL_GetResourcesWithRestrictions(f_repo_id INT) 
    RETURNS INT
    READS SQL DATA
BEGIN
    DECLARE f_total INT;    
    
    SELECT COUNT(id) INTO f_total 
    FROM resource 
    WHERE (resource.`repo_id` = f_repo_id
    AND 
    resource.`restrictions` = 1);
        
    RETURN f_total;
END 
EOF

# Function to return the number of resources with finding aids for a 
# particular repository
run "DROP  FUNCTION IF EXISTS BHL_GetResourcesWithFindingAids;"
run <<EOF
CREATE FUNCTION BHL_GetResourcesWithFindingAids(f_repo_id INT) 
    RETURNS INT
    READS SQL DATA
BEGIN
    DECLARE f_total INT;    
    
    SELECT COUNT(id) INTO f_total 
    FROM resource 
    WHERE (resource.`repo_id` = f_repo_id
    AND 
    resource.`ead_id` IS NOT NULL);
        
    RETURN f_total;
END 
EOF

# Function to return the number of accessions for a particular repository
run "DROP  FUNCTION IF EXISTS BHL_GetTotalAccessions;"
run <<EOF
CREATE FUNCTION BHL_GetTotalAccessions(f_repo_id INT) 
    RETURNS INT
    READS SQL DATA
BEGIN
    DECLARE f_total INT;    
    
    SELECT COUNT(id) INTO f_total 
    FROM accession 
    WHERE accession.`repo_id` = f_repo_id;
        
    RETURN f_total;
END 
EOF

# Function to return the number of accessions that are processed for
# a particular repository
run "DROP  FUNCTION IF EXISTS BHL_GetAccessionsProcessed;"
run <<EOF
CREATE FUNCTION BHL_GetAccessionsProcessed(f_repo_id INT) 
    RETURNS INT
    READS SQL DATA
BEGIN
    DECLARE f_total INT;    
    
    SELECT count(T1.id) INTO f_total  
    FROM 
            event_link_rlshp T1
    INNER JOIN 
            event T2 ON T1.event_id = T2.id 
    WHERE (
            T2.repo_id = f_repo_id
        AND
            T1.accession_id IS NOT NULL
    AND 
            BINARY BHL_GetEnumValue(T2.event_type_id) = BINARY 'processed');
        
    RETURN f_total;
END 
EOF

# Function to return if an accessions has been processed
run "DROP  FUNCTION IF EXISTS BHL_GetAccessionProcessed;"
run <<EOF
CREATE FUNCTION BHL_GetAccessionProcessed(f_accession_id INT) 
    RETURNS VARCHAR(255)
    READS SQL DATA
BEGIN
    DECLARE f_value VARCHAR(255);   
    
    SELECT T1.event_id INTO f_value  
    FROM 
            event_link_rlshp T1 
    INNER JOIN 
            event T2 ON T1.event_id = T2.id 
    WHERE 
            (T1.accession_id = f_accession_id  
    AND 
            BINARY BHL_GetEnumValue(T2.event_type_id) = BINARY 'processed')
        LIMIT 1;
        
    RETURN BHL_GetBoolean(f_value);
END 
EOF

# Function to return the process date for a particular accession
run "DROP  FUNCTION IF EXISTS BHL_GetAccessionProcessedDate;"
run <<EOF
CREATE FUNCTION BHL_GetAccessionProcessedDate(f_accession_id INT) 
    RETURNS VARCHAR(255)
    READS SQL DATA
BEGIN
    DECLARE f_value VARCHAR(255);   
    
    SELECT BHL_GetEventDateExpression(T1.event_id) INTO f_value  
    FROM 
            event_link_rlshp T1 
    INNER JOIN 
            event T2 ON T1.event_id = T2.id 
    WHERE 
            (T1.accession_id = f_accession_id  
    AND 
            BINARY BHL_GetEnumValue(T2.event_type_id) = BINARY 'processed')
        LIMIT 1;
        
    RETURN f_value;
END 
EOF

# Function to return the number of accessions that are cataloged for
# a particular repository
run "DROP  FUNCTION IF EXISTS BHL_GetAccessionsCataloged;"
run <<EOF
CREATE FUNCTION BHL_GetAccessionsCataloged(f_repo_id INT) 
    RETURNS INT
    READS SQL DATA
BEGIN
    DECLARE f_total INT;    
    
    SELECT count(T2.accession_id) INTO f_total  
    FROM 
            event T1 
    INNER JOIN 
            event_link_rlshp T2 ON T1.id = T2.event_id 
    WHERE (
            T1.repo_id = f_repo_id  
    AND 
            T2.accession_id IS NOT NULL 
    AND 
            BINARY BHL_GetEnumValue(T1.event_type_id) = BINARY 'cataloged')
        LIMIT 1;
        
    RETURN f_total;
END 
EOF

# Function to return the if an accessions has been cataloged
run "DROP  FUNCTION IF EXISTS BHL_GetAccessionCataloged;"
run <<EOF
CREATE FUNCTION BHL_GetAccessionCataloged(f_accession_id INT) 
    RETURNS INT
    READS SQL DATA
BEGIN
    DECLARE f_value INT;    
    
    SELECT T1.id INTO f_value 
    FROM 
            event T1 
    INNER JOIN 
            event_link_rlshp T2 ON T1.id = T2.event_id 
    WHERE (
            T2.accession_id = f_accession_id 
    AND 
            BINARY BHL_GetEnumValue(T1.event_type_id) = BINARY 'cataloged')
        LIMIT 1;

    RETURN BHL_GetBoolean(f_value);
END 
EOF

# Function to return the process date for a particular accession
run "DROP  FUNCTION IF EXISTS BHL_GetAccessionCatalogedDate;"
run <<EOF
CREATE FUNCTION BHL_GetAccessionCatalogedDate(f_accession_id INT) 
    RETURNS VARCHAR(255)
    READS SQL DATA
BEGIN
    DECLARE f_value VARCHAR(255);   
    
    SELECT BHL_GetEventDateExpression(T1.event_id) INTO f_value  
    FROM 
            event_link_rlshp T1 
    INNER JOIN 
            event T2 ON T1.event_id = T2.id 
    WHERE 
            (T1.accession_id = f_accession_id  
    AND 
            BINARY BHL_GetEnumValue(T2.event_type_id) = BINARY 'cataloged')
        LIMIT 1;
        
    RETURN f_value;
END 
EOF

# Function to return the number of accessions with restrictions for a 
# particular repository
run "DROP  FUNCTION IF EXISTS BHL_GetAccessionsWithRestrictions;"
run <<EOF
CREATE FUNCTION BHL_GetAccessionsWithRestrictions(f_repo_id INT) 
    RETURNS INT
    READS SQL DATA
BEGIN
    DECLARE f_total INT;    
    
    SELECT COUNT(id) INTO f_total 
    FROM accession 
    WHERE (accession.`repo_id` = f_repo_id
    AND 
    accession.`use_restrictions` = 1);
        
    RETURN f_total;
END 
EOF

# Function to return the number of accessions that have had rights transferred
# for a particular repository
run "DROP  FUNCTION IF EXISTS BHL_GetAccessionsWithRightsTransferred;"
run <<EOF
CREATE FUNCTION BHL_GetAccessionsWithRightsTransferred(f_repo_id INT) 
    RETURNS INT
    READS SQL DATA
BEGIN
    DECLARE f_total INT;    
    
    SELECT count(T2.accession_id) INTO f_total  
    FROM 
            event T1 
    INNER JOIN 
            event_link_rlshp T2 ON T1.id = T2.event_id 
    WHERE ( 
            T1.repo_id = f_repo_id  
    AND 
            T2.accession_id IS NOT NULL 
    AND 
            BINARY BHL_GetEnumValue(T1.event_type_id) = BINARY 'rights_transferred');
        
    RETURN f_total;
END 
EOF

# Function to return if an accession has had it's rights transferred
run "DROP  FUNCTION IF EXISTS BHL_GetAccessionRightsTransferred;"
run <<EOF
CREATE FUNCTION BHL_GetAccessionRightsTransferred(f_accession_id INT) 
    RETURNS INT
    READS SQL DATA
BEGIN
    DECLARE f_value INT;    
    
    SELECT T1.id INTO f_value  
    FROM 
            event T1 
    INNER JOIN 
            event_link_rlshp T2 ON T1.id = T2.event_id 
    WHERE 
            T2.accession_id = f_accession_id 
    AND 
            BINARY BHL_GetEnumValue(T1.event_type_id) = BINARY 'rights_transferred';
        
    RETURN BHL_GetBoolean(f_value);
END 
EOF

# Function to return if  acknowlegement has been set for accession
run "DROP  FUNCTION IF EXISTS BHL_GetAccessionAcknowledgementSent;"
run <<EOF
CREATE FUNCTION BHL_GetAccessionAcknowledgementSent(f_accession_id INT) 
    RETURNS INT
    READS SQL DATA
BEGIN
    DECLARE f_value INT;    
    
    SELECT T1.id INTO f_value  
    FROM 
            event T1 
    INNER JOIN 
            event_link_rlshp T2 ON T1.id = T2.event_id 
    WHERE 
            T2.accession_id = f_accession_id 
    AND 
            BINARY BHL_GetEnumValue(T1.event_type_id) = BINARY 'acknowledgement_sent';
        
    RETURN BHL_GetBoolean(f_value);
END 
EOF

# Function to return if an accession has had it's rights transferred
run "DROP  FUNCTION IF EXISTS BHL_GetAccessionRightsTransferredNote;"
run <<EOF
CREATE FUNCTION BHL_GetAccessionRightsTransferredNote(f_accession_id INT) 
    RETURNS VARCHAR(255)
    READS SQL DATA
BEGIN
    DECLARE f_value VARCHAR(255);   
    
    SELECT T1.outcome_note INTO f_value  
    FROM 
            event T1 
    INNER JOIN 
            event_link_rlshp T2 ON T1.id = T2.event_id 
    WHERE 
            T2.accession_id = f_accession_id 
    AND 
            BINARY BHL_GetEnumValue(T1.event_type_id) = BINARY 'rights_transferred';

    RETURN f_value;
END 
EOF

# Function to return the date expression for an accession record
run "DROP  FUNCTION IF EXISTS BHL_GetEventDateExpression;"
run <<EOF
CREATE FUNCTION BHL_GetEventDateExpression(f_record_id INT) 
    RETURNS VARCHAR(255)
    READS SQL DATA
BEGIN
    DECLARE f_value VARCHAR(255);
        DECLARE f_date VARCHAR(255);
        DECLARE f_expression VARCHAR(255);
        DECLARE f_begin VARCHAR(255);
        DECLARE f_end VARCHAR(255);
    
    SELECT date.`expression`, date.`begin`, date.`end` 
        INTO f_expression, f_begin, f_end 
    FROM 
            date 
    WHERE date.`event_id` = f_record_id 
        LIMIT 1;
    
        -- If the expression is null return the concat of begin and end
        SET f_date = CONCAT(f_begin, '-', f_end);
        
        IF f_expression IS NULL THEN
            SET f_value = f_date;
        ELSEIF f_date IS NOT NULL THEN
            SET f_value = CONCAT(f_expression, ' , ', f_date);
        ELSE
            SET f_value = f_expression;
        END IF;
    
    RETURN f_value;
END 
EOF

# Function to return the number of personal agent records
run "DROP  FUNCTION IF EXISTS BHL_GetAgentsPersonal;"
run <<EOF
CREATE FUNCTION BHL_GetAgentsPersonal(f_repo_id INT) 
    RETURNS INT
    READS SQL DATA
BEGIN
    DECLARE f_total INT;    
    
    SELECT COUNT(id) INTO f_total 
    FROM agent_person
    WHERE agent_person.`id` NOT IN (
        SELECT user.`agent_record_id` 
        FROM
        user WHERE 
        user.`agent_record_id` IS NOT NULL);
        
    RETURN f_total;
END 
EOF

# Function to return the number of corporate agent records
run "DROP  FUNCTION IF EXISTS BHL_GetAgentsCorporate;"
run <<EOF
CREATE FUNCTION BHL_GetAgentsCorporate(f_repo_id INT) 
    RETURNS INT
    READS SQL DATA
BEGIN
    DECLARE f_total INT;    
    
    SELECT COUNT(id) INTO f_total 
    FROM agent_corporate_entity 
    WHERE agent_corporate_entity.`publish` IS NOT NULL;
        
    RETURN f_total;
END 
EOF

# Function to return the number of family agent records
run "DROP  FUNCTION IF EXISTS BHL_GetAgentsFamily;"
run <<EOF
CREATE FUNCTION BHL_GetAgentsFamily(f_repo_id INT) 
    RETURNS INT
    READS SQL DATA
BEGIN
    DECLARE f_total INT;    
    
    SELECT COUNT(id) INTO f_total 
    FROM agent_family;
        
    RETURN f_total;
END 
EOF

# Function to return the number of software agent records
run "DROP  FUNCTION IF EXISTS BHL_GetAgentsSoftware;"
run <<EOF
CREATE FUNCTION BHL_GetAgentsSoftware(f_repo_id INT) 
    RETURNS INT
    READS SQL DATA
BEGIN
    DECLARE f_total INT;    
    
    SELECT COUNT(id) INTO f_total 
    FROM agent_software
    WHERE agent_software.`system_role` = 'none';
        
    RETURN f_total;
END 
EOF

# Function to return the agent type i.e. Person, Family, Corporate, Software
# when those ids found in the linked_agents_rlshp are passed in as parameters
run "DROP  FUNCTION IF EXISTS BHL_GetAgentMatch;"
run <<EOF
CREATE FUNCTION BHL_GetAgentMatch(f_agent_type VARCHAR(10), f_agent_id INT, 
                              f_person_id INT, f_family_id INT, f_corporate_id INT, f_software_id INT) 
    RETURNS INT
    READS SQL DATA
BEGIN
    DECLARE f_agent_match INT;  
    
    IF f_agent_type = 'Person' AND f_person_id = f_agent_id THEN
            SET f_agent_match = 1;
        ELSEIF f_agent_type = 'Family' AND f_family_id = f_agent_id THEN
            SET f_agent_match = 1;
        ELSEIF f_agent_type = 'Corporate' AND f_corporate_id = f_agent_id THEN
            SET f_agent_match = 1;
        ELSEIF f_agent_type = 'Software' AND f_software_id = f_agent_id THEN
            SET f_agent_match = 1;
        ELSE 
            SET f_agent_match = 0;
        END IF;

    RETURN f_agent_match;
END 
EOF

# Function to return the sortname given a Person, Family, or Corporate
# when those ids found in the linked_agents_rlshp are passed in as parameters
run "DROP  FUNCTION IF EXISTS BHL_GetAgentSortName;"
run <<EOF
CREATE FUNCTION BHL_GetAgentSortName(f_person_id INT, f_family_id INT, f_corporate_id INT) 
    RETURNS VARCHAR(255)
    READS SQL DATA
BEGIN
    DECLARE f_value VARCHAR(255);   
    
    IF f_person_id IS NOT NULL THEN
            SELECT sort_name INTO f_value FROM name_person WHERE agent_person_id = f_person_id LIMIT 1;
        ELSEIF f_family_id IS NOT NULL THEN
            SELECT sort_name INTO f_value FROM name_family WHERE agent_family_id = f_family_id LIMIT 1;
        ELSEIF f_corporate_id IS NOT NULL THEN
            SELECT sort_name INTO f_value FROM name_corporate_entity WHERE agent_corporate_entity_id = f_corporate_id LIMIT 1;
        ELSE 
            SET f_value = 'Unknown';
        END IF;

    RETURN f_value;
END 
EOF

# Function to return the sortname given a Person, Family, or Corporate + the role Id
# when those ids found in the linked_agents_rlshp are passed in as parameters
run "DROP  FUNCTION IF EXISTS BHL_GetAgentUniqueName;"
run <<EOF
CREATE FUNCTION BHL_GetAgentUniqueName(f_person_id INT, f_family_id INT, f_corporate_id INT, f_role_id INT) 
    RETURNS VARCHAR(255)
    READS SQL DATA
BEGIN
    DECLARE f_value VARCHAR(255);   
    
    IF f_person_id IS NOT NULL THEN
            SELECT sort_name INTO f_value FROM name_person WHERE agent_person_id = f_person_id LIMIT 1;
        ELSEIF f_family_id IS NOT NULL THEN
            SELECT sort_name INTO f_value FROM name_family WHERE agent_family_id = f_family_id LIMIT 1;
        ELSEIF f_corporate_id IS NOT NULL THEN
            SELECT sort_name INTO f_value FROM name_corporate_entity WHERE agent_corporate_entity_id = f_corporate_id LIMIT 1;
        ELSE 
            SET f_value = 'Unknown';
        END IF;

    RETURN CONCAT_WS('-',f_value, f_role_id);
END 
EOF

# Function to return if a resource record has any agents linked to it has
# Creators
run "DROP  FUNCTION IF EXISTS BHL_GetResourceHasCreator;"
run <<EOF
CREATE FUNCTION BHL_GetResourceHasCreator(f_record_id INT) 
    RETURNS INT
    READS SQL DATA
BEGIN
    DECLARE f_value INT;    
        
        SELECT
            T1.`id` INTO f_value
        FROM
            `linked_agents_rlshp` T1
        WHERE
            BHL_GetResourceId(T1.`resource_id`, T1.`archival_object_id`) = f_record_id
        AND
            BINARY BHL_GetEnumValue(T1.`role_id`) = BINARY 'creator'
        LIMIT 1;

    RETURN BHL_GetBoolean(f_value);
END 
EOF

# Function to any agents sort_name linked to the resource has
# Creators
run "DROP  FUNCTION IF EXISTS BHL_GetResourceCreator;"
run <<EOF
CREATE FUNCTION BHL_GetResourceCreator(f_record_id INT) 
    RETURNS VARCHAR(1024)
    READS SQL DATA
BEGIN
    DECLARE f_value VARCHAR(1024);  
        
        SELECT
            GROUP_CONCAT(BHL_GetAgentSortName(T1.`agent_person_id`, T1.`agent_family_id`, T1.`agent_corporate_entity_id`) SEPARATOR '; ') INTO f_value
        FROM
            `linked_agents_rlshp` T1
        WHERE
            BHL_GetResourceId(T1.`resource_id`, T1.`archival_object_id`) = f_record_id
        AND
            BINARY BHL_GetEnumValue(T1.`role_id`) = BINARY 'creator';

    RETURN f_value;
END 
EOF

# Function to return if a resource record has any agents linked to it has
# Source
run "DROP  FUNCTION IF EXISTS BHL_GetResourceHasSource;"
run <<EOF
CREATE FUNCTION BHL_GetResourceHasSource(f_record_id INT) 
    RETURNS INT
    READS SQL DATA
BEGIN
    DECLARE f_value INT;    
        
        SELECT
            T1.`id` INTO f_value
        FROM
            `linked_agents_rlshp` T1
        WHERE
            BHL_GetResourceId(T1.`resource_id`, T1.`archival_object_id`) = f_record_id
        AND
            BINARY BHL_GetEnumValue(T1.`role_id`) = BINARY 'source' 
        LIMIT 1;

    RETURN BHL_GetBoolean(f_value);
END 
EOF

# Function to return if a resource record has any agents linked to it has
# Creators
run "DROP  FUNCTION IF EXISTS BHL_GetResourceHasDeaccession;"
run <<EOF
CREATE FUNCTION BHL_GetResourceHasDeaccession(f_record_id INT) 
    RETURNS INT
    READS SQL DATA
BEGIN
    DECLARE f_value INT;    
        
        SELECT
            T1.`id` INTO f_value
        FROM
            `deaccession` T1
        WHERE
            T1.`resource_id` = f_record_id
        LIMIT 1;

    RETURN BHL_GetBoolean(f_value);
END 
EOF

# Function to return the number of subject records
run "DROP  FUNCTION IF EXISTS BHL_GetTotalSubjects;"
run <<EOF
CREATE FUNCTION BHL_GetTotalSubjects(f_repo_id INT) 
    RETURNS INT
    READS SQL DATA
BEGIN
    DECLARE f_total INT;    
    
    SELECT COUNT(id) INTO f_total 
    FROM subject;
        
    RETURN f_total;
END 
EOF

# Function to return the number of resource records for a particular finding
# aid status
run "DROP  FUNCTION IF EXISTS BHL_GetStatusCount;"
run <<EOF
CREATE FUNCTION BHL_GetStatusCount(f_repo_id INT, f_status_id INT) 
    RETURNS INT
    READS SQL DATA
BEGIN
    DECLARE f_total INT;    
    
    SELECT COUNT(id) INTO f_total 
    FROM 
        resource
    WHERE 
        resource.`finding_aid_status_id` = f_status_id
        AND
        resource.`repo_id` = f_repo_id;
        
    RETURN f_total;
END 
EOF

# Function to return the number of resource records for a particular language
# code
run "DROP  FUNCTION IF EXISTS BHL_GetLanguageCount;"
run <<EOF
CREATE FUNCTION BHL_GetLanguageCount(f_repo_id INT, f_language_id INT) 
    RETURNS INT
    READS SQL DATA
BEGIN
    DECLARE f_total INT;    
    
    SELECT COUNT(id) INTO f_total 
    FROM 
        resource
    WHERE 
        resource.`language_id` = f_language_id
        AND
        resource.`repo_id` = f_repo_id;
        
    RETURN f_total;
END 
EOF

# Function to return the number of instances for a particular instance type 
# in a repository. I couldn't find a simpler way to do this counting
run "DROP  FUNCTION IF EXISTS BHL_GetInstanceCount;"
run <<EOF
CREATE FUNCTION BHL_GetInstanceCount(f_repo_id INT, f_instance_type_id INT) 
    RETURNS INT
    READS SQL DATA
BEGIN
    DECLARE f_total INT DEFAULT 0;
    DECLARE f_id INT;   
    DECLARE done INT DEFAULT 0;
    
    DECLARE cur CURSOR FOR SELECT T1.`id`  
    FROM 
            resource T1
    INNER JOIN
            instance T2 ON BHL_GetResourceId(T2.`resource_id`, T2.`archival_object_id`) = T1.`id`
        WHERE 
            T1.`repo_id` = f_repo_id
    AND
            T2.`instance_type_id` = f_instance_type_id 
    GROUP BY
            T1.`id`;    
    
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;
    
    OPEN cur;
    
    count_resource: LOOP
            FETCH cur INTO f_id;
    
            IF done = 1 THEN
        LEAVE count_resource;
            END IF;
        
            SET f_total = f_total + 1;
    
    END LOOP count_resource;
    
    CLOSE cur;
        
    RETURN f_total;
END 
EOF

# Function to return the total extent of unprocessed accessions that
# for a particular repository
run "DROP  FUNCTION IF EXISTS BHL_GetAccessionsExtent;"
run <<EOF
CREATE FUNCTION BHL_GetAccessionsExtent(f_repo_id INT, f_extent_type_id INT) 
    RETURNS DECIMAL(10,2)
    READS SQL DATA
BEGIN
    DECLARE f_total DECIMAL(10,2);  
    
    SELECT SUM(T1.number) INTO f_total  
    FROM extent T1 
    INNER JOIN 
        accession T2 ON T1.accession_id = T2.id 
    WHERE (T2.repo_id = f_repo_id   
        AND BHL_GetAccessionCataloged(T2.id) = 0
        AND T1.extent_type_id = f_extent_type_id);
    
    -- Check for null then set it to zero
    IF f_total IS NULL THEN
        SET f_total = 0;
    END IF;
    
    RETURN f_total;
END 
EOF

# Function to return the total extent for an accession record
run "DROP  FUNCTION IF EXISTS BHL_GetAccessionExtent;"
run <<EOF
CREATE FUNCTION BHL_GetAccessionExtent(f_accession_id INT) 
    RETURNS DECIMAL(10,2)
    READS SQL DATA
BEGIN
    DECLARE f_total DECIMAL(10,2);  
    
    SELECT 
            SUM(T1.number) INTO f_total  
    FROM 
            extent T1
    WHERE 
            T1.accession_id = f_accession_id;
    
    -- Check for null then set it to zero
    IF f_total IS NULL THEN
            SET f_total = 0;
    END IF;
    
    RETURN f_total;
END 
EOF

# Function to return the accession extent type
run "DROP  FUNCTION IF EXISTS BHL_GetAccessionExtentType;"
run <<EOF
CREATE FUNCTION BHL_GetAccessionExtentType(f_accession_id INT) 
    RETURNS VARCHAR(255)
    READS SQL DATA
BEGIN
    DECLARE f_value VARCHAR(255);   
    
    SELECT 
            BHL_GetEnumValueUF(T1.extent_type_id) INTO f_value  
    FROM 
            extent T1 
    WHERE 
            T1.accession_id = f_accession_id
        LIMIT 1;
    
    RETURN f_value;
END 
EOF

# Function to return the accession container summary
run "DROP  FUNCTION IF EXISTS BHL_GetAccessionContainerSummary;"
run <<EOF
CREATE FUNCTION BHL_GetAccessionContainerSummary(f_accession_id INT) 
    RETURNS TEXT
    READS SQL DATA
BEGIN
    DECLARE f_value TEXT;   
    
    SELECT T1.container_summary INTO f_value  
    FROM extent T1 
    WHERE T1.accession_id = f_accession_id
        LIMIT 1;
    
    RETURN f_value;
END 
EOF

# Function to return the accession id for a given instance
run "DROP  FUNCTION IF EXISTS BHL_GetAccessionIdForInstance;"
run <<EOF
CREATE FUNCTION BHL_GetAccessionIdForInstance(f_record_id INT) 
    RETURNS INT
    READS SQL DATA
BEGIN
    DECLARE f_value INT;
        
        -- get the resource id 
    SELECT T1.`accession_id` INTO f_value  
    FROM 
            instance T1
    WHERE T1.`id` = f_record_id; 
    
    RETURN f_value;
END 
EOF

# Function to return the total extent of resources and its' archival objects
# for a particular repository
run "DROP  FUNCTION IF EXISTS BHL_GetResourcesExtent;"
run <<EOF
CREATE FUNCTION BHL_GetResourcesExtent(f_repo_id INT, f_extent_type_id INT) 
    RETURNS DECIMAL(10,2)
    READS SQL DATA
BEGIN
    DECLARE f_total DECIMAL(10,2);  
    
    SELECT 
            SUM(T1.number) INTO f_total  
    FROM 
            extent T1 
    INNER JOIN 
            resource T2 ON BHL_GetResourceId(T1.resource_id, T1.archival_object_id) = T2.id 
    WHERE 
            (T2.repo_id = f_repo_id AND T1.extent_type_id = f_extent_type_id);
    
    -- Check for null then set it to zero
    IF f_total IS NULL THEN
        SET f_total = 0;
    END IF;
    
    RETURN f_total;
END 
EOF

# Function to return the total extent of a resource record excluding the
# archival objects
run "DROP  FUNCTION IF EXISTS BHL_GetResourceExtent;"
run <<EOF
CREATE FUNCTION BHL_GetResourceExtent(f_resource_id INT) 
    RETURNS DECIMAL(10,2)
    READS SQL DATA
BEGIN
    DECLARE f_total DECIMAL(10,2);  
    
    SELECT 
            SUM(T1.number) INTO f_total  
    FROM 
            extent T1 
    WHERE 
            T1.resource_id = f_resource_id;
    
    -- Check for null then set it to zero
    IF f_total IS NULL THEN
            SET f_total = 0;
    END IF;
    
    RETURN f_total;
END 
EOF


# Function to return the resource extent type of a resource record excluding the
# archival objects
run "DROP  FUNCTION IF EXISTS BHL_GetResourceExtentType;"
run <<EOF
CREATE FUNCTION BHL_GetResourceExtentType(f_resource_id INT) 
    RETURNS VARCHAR(255)
    READS SQL DATA
BEGIN
    DECLARE f_value VARCHAR(255);   
    
    SELECT BHL_GetEnumValueUF(T1.extent_type_id) INTO f_value  
    FROM extent T1 
    WHERE T1.resource_id = f_resource_id
        LIMIT 1;
    
    RETURN f_value;
END 
EOF

# Function to return the resource extent type of a resource record excluding the
# archival objects
run "DROP  FUNCTION IF EXISTS BHL_GetResourceContainerSummary;"
run <<EOF
CREATE FUNCTION BHL_GetResourceContainerSummary(f_resource_id INT) 
    RETURNS TEXT
    READS SQL DATA
BEGIN
    DECLARE f_value TEXT;   
    
    SELECT T1.container_summary INTO f_value  
    FROM extent T1 
    WHERE T1.resource_id = f_resource_id
        LIMIT 1;
    
    RETURN f_value;
END 
EOF

# Function to return the total extent of a resource record excluding the
# archival objects
run "DROP  FUNCTION IF EXISTS BHL_GetResourceDeaccessionExtent;"
run <<EOF
CREATE FUNCTION BHL_GetResourceDeaccessionExtent(f_resource_id INT) 
    RETURNS DECIMAL(10,2)
    READS SQL DATA
BEGIN
    DECLARE f_total DECIMAL(10,2);  
    
    SELECT 
            SUM(T2.number) INTO f_total  
    FROM 
            deaccession T1
        INNER JOIN 
            extent T2 ON T1.id = T2.deaccession_id 
    WHERE 
            T1.resource_id = f_resource_id;
    
    -- Check for null then set it to zero
    IF f_total IS NULL THEN
            SET f_total = 0;
    END IF;
    
    RETURN f_total;
END 
EOF

# Function to return the number of subject records with a certain term type
run "DROP  FUNCTION IF EXISTS BHL_GetTermTypeCount;"
run <<EOF
CREATE FUNCTION BHL_GetTermTypeCount(f_term_type_id INT) 
    RETURNS INT
    READS SQL DATA
BEGIN
    DECLARE f_total INT DEFAULT 0;  
    
        SELECT COUNT(*) INTO f_total
        FROM (
            SELECT T1.`id`
            FROM 
                term T1
            INNER JOIN
                subject_term T2 ON T1.`id` = T2.`term_id`
            WHERE
        T1.`term_type_id` = f_term_type_id
            GROUP BY 
                T2.`subject_id`
        ) AS t;
    
    RETURN f_total;
END 
EOF

# Function to return the date expression for an accession record
run "DROP  FUNCTION IF EXISTS BHL_GetAccessionDateExpression;"
run <<EOF
CREATE FUNCTION BHL_GetAccessionDateExpression(f_record_id INT) 
    RETURNS VARCHAR(255)
    READS SQL DATA
BEGIN
    DECLARE f_value VARCHAR(255);
        DECLARE f_date VARCHAR(255);
        DECLARE f_expression VARCHAR(255);
        DECLARE f_begin VARCHAR(255);
        DECLARE f_end VARCHAR(255);
    
    SELECT date.`expression`, date.`begin`, date.`end` 
        INTO f_expression, f_begin, f_end 
    FROM 
            date 
    WHERE date.`accession_id` = f_record_id 
        LIMIT 1;
    
        -- If the expression is null return the concat of begin and end
        SET f_date = CONCAT(f_begin, '-', f_end);
        
        IF f_expression IS NULL THEN
            SET f_value = f_date;
        ELSEIF f_date IS NOT NULL THEN
            SET f_value = CONCAT(f_expression, ' , ', f_date);
        ELSE
            SET f_value = f_expression;
        END IF;
    
    RETURN f_value;
END 
EOF

# Function to return a particula part of a date record for an accession record
# f_part = 0 return date expression
# f_part = 1 return date begin
# f_part = 2 return date end
run "DROP  FUNCTION IF EXISTS BHL_GetAccessionDatePart;"
run <<EOF
CREATE FUNCTION BHL_GetAccessionDatePart(f_record_id INT, f_date_type VARCHAR(255), f_part INT) 
    RETURNS VARCHAR(255)
    READS SQL DATA
BEGIN
    DECLARE f_value VARCHAR(255);
        DECLARE f_expression VARCHAR(255);
        DECLARE f_begin VARCHAR(255);
        DECLARE f_end VARCHAR(255);
    
    SELECT 
            date.`expression`, date.`begin`, date.`end` 
        INTO 
            f_expression, f_begin, f_end 
    FROM 
            date 
    WHERE (
            date.`accession_id` = f_record_id
            AND
            BHL_GetEnumValue(date.`date_type_id`) = f_date_type)
        LIMIT 1;
    
        -- return the part we need
        IF f_part = 0 THEN
            SET f_value = f_expression;
        ELSEIF f_part = 1 THEN
            SET f_value = f_begin;
        ELSE
            SET f_value = f_end;
        END IF;
    
    RETURN f_value;
END 
EOF

# Function to return the date expression for a digital object
run "DROP  FUNCTION IF EXISTS BHL_GetDigitalObjectDateExpression;"
run <<EOF
CREATE FUNCTION BHL_GetDigitalObjectDateExpression(f_record_id INT) 
    RETURNS VARCHAR(255)
    READS SQL DATA
BEGIN
    DECLARE f_value VARCHAR(255);
        DECLARE f_date VARCHAR(255);
        DECLARE f_expression VARCHAR(255);
        DECLARE f_begin VARCHAR(255);
        DECLARE f_end VARCHAR(255);
    
    SELECT date.`expression`, date.`begin`, date.`end` 
        INTO f_expression, f_begin, f_end 
    FROM 
            date 
    WHERE date.`digital_object_id` = f_record_id
        LIMIT 1;
    
        -- If the expression is null return the concat of begin and end
        SET f_date = CONCAT(f_begin, '-', f_end);
        
        IF f_expression IS NULL THEN
            SET f_value = f_date;
        ELSEIF f_date IS NOT NULL THEN
            SET f_value = CONCAT(f_expression, ' , ', f_date);
        ELSE
            SET f_value = f_expression;
        END IF;
    
    RETURN f_value;
END 
EOF

# Function to return the date expression for a resource record
run "DROP  FUNCTION IF EXISTS BHL_GetResourceDateExpression;"
run <<EOF
CREATE FUNCTION BHL_GetResourceDateExpression(f_record_id INT) 
    RETURNS VARCHAR(255)
    READS SQL DATA
BEGIN
    DECLARE f_value VARCHAR(255);
        DECLARE f_date VARCHAR(255);
        DECLARE f_expression VARCHAR(255);
        DECLARE f_begin VARCHAR(255);
        DECLARE f_end VARCHAR(255);
    
    SELECT date.`expression`, date.`begin`, date.`end` 
        INTO f_expression, f_begin, f_end 
    FROM 
            date 
    WHERE date.`resource_id` = f_record_id 
        LIMIT 1;
    
        -- If the expression is null return the concat of begin and end
        SET f_date = CONCAT(f_begin, '-', f_end);
        
        IF f_expression IS NULL THEN
            SET f_value = f_date;
        ELSEIF f_date IS NOT NULL THEN
            SET f_value = CONCAT(f_expression, ' , ', f_date);
        ELSE
            SET f_value = f_expression;
        END IF;
    
    RETURN f_value;
END 
EOF

# Function to return the resource id for a given instance
run "DROP  FUNCTION IF EXISTS BHL_GetResourceIdForInstance;"
run <<EOF
CREATE FUNCTION BHL_GetResourceIdForInstance(f_record_id INT) 
    RETURNS INT
    READS SQL DATA
BEGIN
    DECLARE f_value INT;  
    -- get the resource id 
    SELECT BHL_GetResourceID(T1.`resource_id`, T1.`archival_object_id`) INTO f_value  
    FROM 
            instance T1
    WHERE T1.`id` = f_record_id; 
    
    RETURN f_value;
END 
EOF

# Function to return the resource identifier for a given instance
run "DROP  FUNCTION IF EXISTS BHL_GetResourceIdentiferForInstance;"
run <<EOF
CREATE FUNCTION BHL_GetResourceIdentiferForInstance(f_record_id INT) 
    RETURNS VARCHAR(255)
    READS SQL DATA
BEGIN
    DECLARE f_value VARCHAR(255);
        
        -- get the resource id 
    SELECT T2.`identifier` INTO f_value  
    FROM 
            instance T1
        INNER JOIN
            resource T2 ON BHL_GetResourceID(T1.`resource_id`, T1.`archival_object_id`) = T2.`id`
    WHERE T1.`id` = f_record_id; 
    
    RETURN f_value;
END 
EOF

# Function to return the resource id (PK) for a given instance
run "DROP  FUNCTION IF EXISTS BHL_GetResourceIdForInstance;"
run <<EOF
CREATE FUNCTION BHL_GetResourceIdForInstance(f_record_id INT) 
    RETURNS INT
    READS SQL DATA
BEGIN
    DECLARE f_value INT;
        
        -- get the resource id 
    SELECT 
            T2.`id` INTO f_value  
    FROM 
            instance T1
        INNER JOIN
            resource T2 ON BHL_GetResourceID(T1.`resource_id`, T1.`archival_object_id`) = T2.`id`
    WHERE 
            T1.`id` = f_record_id; 
    
    RETURN f_value;
END 
EOF

# Function to return the resource identifier for a given instance
run "DROP  FUNCTION IF EXISTS BHL_GetResourceTitleForInstance;"
run <<EOF
CREATE FUNCTION BHL_GetResourceTitleForInstance(f_record_id INT) 
    RETURNS VARCHAR(255)
    READS SQL DATA
BEGIN
    DECLARE f_value VARCHAR(255);
        
        -- get the resource id 
    SELECT 
            T2.`title` INTO f_value  
    FROM 
            instance T1
        INNER JOIN
            resource T2 ON BHL_GetResourceID(T1.`resource_id`, T1.`archival_object_id`) = T2.`id`
    WHERE 
            T1.`id` = f_record_id; 
    
    RETURN f_value;
END 
EOF

# function to return a 0 or 1 to represent a boolean value to the report
run "DROP  FUNCTION IF EXISTS BHL_GetBoolean;"
run <<EOF
CREATE FUNCTION BHL_GetBoolean(f_value INT) 
    RETURNS INT
    READS SQL DATA
BEGIN
    DECLARE f_boolean INT;
        
    IF f_value IS NULL THEN
        SET f_boolean = 0;
    ELSE 
        SET f_boolean = 1;
    END IF;

    RETURN f_boolean;
END 
EOF

# function to return the name of a repository given the id
run "DROP  FUNCTION IF EXISTS BHL_GetRepositoryName;"
run <<EOF
CREATE FUNCTION BHL_GetRepositoryName(f_record_id INT) 
    RETURNS VARCHAR(255)
    READS SQL DATA
BEGIN
    DECLARE f_value VARCHAR(255);

    SELECT 
        `name` INTO f_value  
    FROM 
        repository 
    WHERE 
        `id` = f_record_id; 
    
    RETURN f_value;
END 
EOF

# Function to return the date expression for an accession record
run "DROP  FUNCTION IF EXISTS BHL_GetDeaccessionDate;"
run <<EOF
CREATE FUNCTION BHL_GetDeaccessionDate(f_record_id INT) 
    RETURNS VARCHAR(255)
    READS SQL DATA
BEGIN
    DECLARE f_value VARCHAR(255);
        DECLARE f_expression VARCHAR(255);
        DECLARE f_begin VARCHAR(255);
    
    SELECT date.`expression`, date.`begin`
        INTO f_expression, f_begin
    FROM 
            date 
    WHERE date.`deaccession_id` = f_record_id 
        LIMIT 1;
    
        # Just return the date begin       
        SET f_value = f_begin;
    
    RETURN f_value;
END 
EOF

# Function to return the total extent for an deaccession record
run "DROP  FUNCTION IF EXISTS BHL_GetDeaccessionExtent;"
run <<EOF
CREATE FUNCTION BHL_GetDeaccessionExtent(f_deaccession_id INT) 
    RETURNS DECIMAL(10,2)
    READS SQL DATA
BEGIN
    DECLARE f_total DECIMAL(10,2);  
    
    SELECT 
            SUM(T1.number) INTO f_total  
    FROM 
            extent T1 
    WHERE 
            T1.deaccession_id = f_deaccession_id;
    
    -- Check for null then set it to zero
    IF f_total IS NULL THEN
            SET f_total = 0;
    END IF;
    
    RETURN f_total;
END 
EOF

# Function to return the deaccession extent type
run "DROP  FUNCTION IF EXISTS BHL_GetDeaccessionExtentType;"
run <<EOF
CREATE FUNCTION BHL_GetDeaccessionExtentType(f_deaccession_id INT) 
    RETURNS VARCHAR(255)
    READS SQL DATA
BEGIN
    DECLARE f_value VARCHAR(255);   
    
    SELECT 
            BHL_GetEnumValueUF(T1.extent_type_id) INTO f_value  
    FROM 
            extent T1 
    WHERE 
            T1.deaccession_id = f_deaccession_id
        LIMIT 1;
    
    RETURN f_value;
END 
EOF

# Function to return phone number given an agent contact id
run "DROP  FUNCTION IF EXISTS BHL_GetPhoneNumber;"
run <<EOF
CREATE FUNCTION BHL_GetPhoneNumber(f_agent_contact_id INT) 
    RETURNS VARCHAR(255)
    READS SQL DATA
BEGIN
    DECLARE f_value VARCHAR(255);   
    
    SELECT 
            telephone.`number`INTO f_value
    FROM 
            telephone
    WHERE 
            telephone.`agent_contact_id` = f_agent_contact_id
            AND
            BINARY BHL_GetEnumValue(telephone.`number_type_id`) != BINARY 'fax'
        LIMIT 1;
        
    RETURN f_value;
END 
EOF

# Function to return fax number given an agent contact id
run "DROP  FUNCTION IF EXISTS BHL_GetFaxNumber;"
run <<EOF
CREATE FUNCTION BHL_GetFaxNumber(f_agent_contact_id INT) 
    RETURNS VARCHAR(255)
    READS SQL DATA
BEGIN
    DECLARE f_value VARCHAR(255);   
    
    SELECT 
            telephone.`number`INTO f_value
    FROM 
            telephone
    WHERE 
            telephone.`agent_contact_id` = f_agent_contact_id
            AND
            BINARY BHL_GetEnumValue(telephone.`number_type_id`) = BINARY 'fax'
        LIMIT 1;
        
    RETURN f_value;
END 
EOF

run "DROP FUNCTION IF EXISTS BHL_GetAgentBEALContactID;"
run <<EOF
CREATE FUNCTION BHL_GetAgentBEALContactID(f_person_id INT, f_family_id INT, f_corporate_id INT)
	RETURNS VARCHAR(255)
	READS SQL DATA
BEGIN
	DECLARE f_value VARCHAR(255);

	IF f_person_id IS NOT NULL THEN
		SELECT beal_contact_id INTO f_value FROM donor_detail WHERE agent_person_id = f_person_id LIMIT 1;
	ELSEIF f_family_id IS NOT NULL THEN
		SELECT beal_contact_id INTO f_value FROM donor_detail WHERE agent_family_id = f_family_id LIMIT 1;
	ELSEIF f_corporate_id IS NOT NULL THEN
		SELECT beal_contact_id INTO f_value FROM donor_detail WHERE agent_corporate_entity_id = f_corporate_id LIMIT 1;
	ELSE
		SET f_value = '';
	END IF;

	RETURN f_value;
END
EOF



run "DROP FUNCTION IF EXISTS BHL_GetResourceExtentNumberType;"
run <<EOF
CREATE FUNCTION BHL_GetResourceExtentNumberType(f_resource_id INT) 
    RETURNS VARCHAR(1020)
    READS SQL DATA
BEGIN
    DECLARE f_value VARCHAR(1020);

    SELECT 
    	GROUP_CONCAT(CONCAT(number, " ", BHL_GetEnumValue(extent_type_id)) SEPARATOR "; ") into f_value
    FROM
    	extent
	WHERE
		extent.`resource_id` = f_resource_id;
	RETURN f_value;   
END 
EOF

run "DROP FUNCTION IF EXISTS BHL_GetAccessrestrictContent;"
run <<EOF
CREATE FUNCTION BHL_GetAccessrestrictContent(f_note_id INT) 
    RETURNS VARCHAR(8000)
    READS SQL DATA
BEGIN
    DECLARE f_value VARCHAR(8000);

    SELECT
    	JSON_EXTRACT(CONVERT(notes using utf8), '$.subnotes[0].content') into f_value
    FROM
    	note
	WHERE
		note.`id` = f_note_id;

	RETURN f_value;   
END 
EOF

run "DROP FUNCTION IF EXISTS BHL_CountAccessrestrictsForResource;"
run <<EOF
CREATE FUNCTION BHL_CountAccessrestrictsForResource(f_resource_id INT) 
    RETURNS INT
    READS SQL DATA
BEGIN
    DECLARE f_total INT;

    SELECT
    	COUNT(*) into f_total
    FROM
    	note
	JOIN
		archival_object ON archival_object.id = note.archival_object_id
	JOIN
		resource ON resource.id = archival_object.root_record_id
	WHERE
		note.`archival_object_id` IS NOT NULL AND resource.`id` = f_resource_id AND note.`notes` LIKE "%accessrestrict%";

	RETURN f_total;
END 
EOF

run "DROP FUNCTION IF EXISTS BHL_CountAccessrestrictByType;"
run <<EOF
CREATE FUNCTION BHL_CountAccessrestrictByType(f_resource_id INT, f_accessrestrict_type CHAR(2)) 
    RETURNS INT
    READS SQL DATA
BEGIN
    DECLARE f_total INT;

    SELECT
    	COUNT(*) into f_total
    FROM
    	note
	JOIN
		archival_object ON archival_object.id = note.archival_object_id
	JOIN
		resource ON resource.id = archival_object.root_record_id
	WHERE
		note.`archival_object_id` IS NOT NULL AND resource.`id` = f_resource_id AND note.`notes` LIKE "%accessrestrict%" AND LOWER(JSON_EXTRACT(CONVERT(notes using utf8), '$.subnotes[0].content')) LIKE CONCAT('"', f_accessrestrict_type, ' %');

	RETURN f_total;
END 
EOF

run "DROP FUNCTION IF EXISTS BHL_CountRestrictedContainersForResource;"
run <<EOF
CREATE FUNCTION BHL_CountRestrictedContainersForResource(f_resource_id INT) 
    RETURNS INT
    READS SQL DATA
BEGIN
    DECLARE f_total INT;

    SELECT
        COUNT(DISTINCT top_container.`id`) into f_total
    FROM
        note
    JOIN
        archival_object ON archival_object.id = note.archival_object_id
    JOIN
        resource ON resource.id = archival_object.root_record_id
    JOIN
        instance ON instance.archival_object_id = archival_object.id
    JOIN
        sub_container ON sub_container.instance_id = instance.id
    JOIN
        top_container_link_rlshp ON top_container_link_rlshp.sub_container_id = sub_container.id
    JOIN
        top_container ON top_container.id = top_container_link_rlshp.top_container_id
    WHERE
        resource.`id` = f_resource_id AND note.`archival_object_id` IS NOT NULL AND note.`notes` LIKE "%accessrestrict%";

    RETURN f_total;
END 
EOF

run "DROP FUNCTION IF EXISTS BHL_CountContainersForResource;"
run <<EOF
CREATE FUNCTION BHL_CountContainersForResource(f_resource_id INT) 
    RETURNS INT
    READS SQL DATA
BEGIN
    DECLARE f_total INT;

    SELECT
        COUNT(DISTINCT top_container.`id`) into f_total
    FROM
        archival_object
    JOIN
        resource ON resource.id = archival_object.root_record_id
    JOIN
        instance ON instance.archival_object_id = archival_object.id
    JOIN
        sub_container ON sub_container.instance_id = instance.id
    JOIN
        top_container_link_rlshp ON top_container_link_rlshp.sub_container_id = sub_container.id
    JOIN
        top_container ON top_container.id = top_container_link_rlshp.top_container_id
    WHERE
        resource.`id` = f_resource_id;

    RETURN f_total;
END 
EOF

run "DROP FUNCTION IF EXISTS BHL_CountDigitalRestrictionsForResource;"
run <<EOF
CREATE FUNCTION BHL_CountDigitalRestrictionsForResource(f_resource_id INT) 
    RETURNS INT
    READS SQL DATA
BEGIN
    DECLARE f_total INT;

    SELECT
        COUNT(DISTINCT instance.`id`) into f_total
    FROM
        note
    JOIN
        archival_object ON archival_object.id = note.archival_object_id
    JOIN
        resource ON resource.id = archival_object.root_record_id
    JOIN
        instance ON instance.archival_object_id = archival_object.id
    WHERE
        resource.`id` = f_resource_id AND note.`archival_object_id` IS NOT NULL 
    AND 
        note.`notes` LIKE "%accessrestrict%" AND BINARY BHL_GetEnumValue(instance.`instance_type_id`) = BINARY "digital_object";

    RETURN f_total;
END 
EOF

run "DROP FUNCTION IF EXISTS BHL_CountReadingRoomRestrictions;"
run <<EOF
CREATE FUNCTION BHL_CountReadingRoomRestrictions(f_resource_id INT) 
    RETURNS INT
    READS SQL DATA
BEGIN
    DECLARE f_total INT;

    SELECT
        COUNT(*) into f_total
    FROM
        note
    JOIN
        archival_object ON archival_object.id = note.archival_object_id
    JOIN
        resource ON resource.id = archival_object.root_record_id
    WHERE
        note.`archival_object_id` IS NOT NULL AND resource.`id` = f_resource_id 
    AND 
        note.`notes` LIKE "%accessrestrict%" 
    AND 
        LOWER(JSON_EXTRACT(CONVERT(notes using utf8), '$.subnotes[0].content')) LIKE "%reading room%";

    RETURN f_total;
END 
EOF


run "DROP FUNCTION IF EXISTS BHL_GetAccessionFieldArchivists;"
run <<EOF
CREATE FUNCTION BHL_GetAccessionFieldArchivists(f_accession_id INT)
	RETURNS VARCHAR(255)
	READS SQL DATA
BEGIN
	DECLARE f_value VARCHAR(255);

	SELECT 
		GROUP_CONCAT(name_person.sort_name SEPARATOR "; ") into f_value
	FROM
		name_person
	JOIN
		linked_agents_rlshp ON name_person.`agent_person_id`=linked_agents_rlshp.`agent_person_id`
	JOIN
		event ON event.`id`=linked_agents_rlshp.`event_id`
	JOIN
		event_link_rlshp ON event_link_rlshp.`event_id`=event.`id`
	WHERE
		event_link_rlshp.`accession_id`=f_accession_id 
	AND
		BINARY BHL_GetEnumValue(linked_agents_rlshp.`role_id`) = BINARY 'field_archivist'
	AND
		BINARY BHL_GetEnumValue(event.`event_type_id`) = BINARY 'custody_transfer';

	RETURN f_value;
END
EOF

run "DROP FUNCTION IF EXISTS BHL_GetArchivalObjectNoteByType;"
run <<EOF
CREATE FUNCTION BHL_GetArchivalObjectNoteByType(f_archival_object_id INT, f_note_type VARCHAR(20)) 
    RETURNS VARCHAR(8000)
    READS SQL DATA
BEGIN
    DECLARE f_value MEDIUMBLOB;

    SELECT
        notes into f_value
    FROM
    	note
	WHERE
		note.`archival_object_id` = f_archival_object_id AND note.`notes` LIKE CONCAT('%', f_note_type, '%') LIMIT 1;

	RETURN f_value;
END 
EOF

run "DROP  FUNCTION IF EXISTS BHL_GetArchivalObjectExtent;"
run <<EOF
CREATE FUNCTION BHL_GetArchivalObjectExtent(f_archival_object_id INT) 
    RETURNS VARCHAR(1024)
    READS SQL DATA
BEGIN
    DECLARE f_value VARCHAR(1024);

    SELECT 
        GROUP_CONCAT(CONCAT(number, " ", BHL_GetEnumValue(extent_type_id), if(GetExtentPhysicalDetails(id) IS NOT NULL, CONCAT(" ", BHL_GetExtentPhysicalDetails(id)), '')) SEPARATOR "; ") into f_value
    FROM
        extent
    WHERE
        extent.`archival_object_id` = f_archival_object_id;
    RETURN f_value; 
END 
EOF

run "DROP  FUNCTION IF EXISTS BHL_GetExtentPhysicalDetails;"
run <<EOF
CREATE FUNCTION BHL_GetExtentPhysicalDetails(f_extent_id INT) 
    RETURNS VARCHAR(1024)
    READS SQL DATA
BEGIN
    DECLARE f_value VARCHAR(1024);

    SELECT
        CONCAT("(", COALESCE(physical_details, ''), if(physical_details IS NULL OR dimensions IS NULL, '', ', '), COALESCE(dimensions, ''), ")") into f_value 
    FROM
        extent
    WHERE
        extent.`id` = f_extent_id AND (extent.`physical_details` IS NOT NULL OR extent.`dimensions` IS NOT NULL);

    RETURN f_value;
END 
EOF

run "DROP FUNCTION IF EXISTS BHL_GetArchivalObjectContainers;"
run <<EOF
CREATE FUNCTION BHL_GetArchivalObjectContainers(f_archival_object_id INT) 
    RETURNS VARCHAR(1024)
    READS SQL DATA
BEGIN
    DECLARE f_value VARCHAR(1024);

    SELECT
        GROUP_CONCAT(CONCAT(GetEnumValue(top_container.`type_id`), " ", top_container.`indicator`)) INTO f_value
    FROM
        top_container
    JOIN
        top_container_link_rlshp ON top_container_link_rlshp.top_container_id = top_container.id
    JOIN
        sub_container ON sub_container.id = top_container_link_rlshp.sub_container_id
    JOIN
        instance ON instance.id = sub_container.instance_id
    WHERE
        instance.archival_object_id = f_archival_object_id;

    RETURN f_value;
END 
EOF

run "DROP FUNCTION IF EXISTS BHL_GetArchivalObjectBreadcrumb;"
run <<EOF
CREATE FUNCTION BHL_GetArchivalObjectBreadcrumb(f_archival_object_id INT) 
    RETURNS VARCHAR(8000)
    READS SQL DATA
BEGIN
    DECLARE f_value VARCHAR(8000);

    DECLARE f_parent_id INT;
    SELECT parent_id INTO f_parent_id FROM archival_object WHERE id = f_archival_object_id;

    WHILE f_parent_id IS NOT NULL DO
        SELECT if(f_value IS NOT NULL, CONCAT(display_string, " > ", f_value), display_string) INTO f_value FROM archival_object WHERE id = f_parent_id;
        SELECT parent_id INTO f_parent_id FROM archival_object WHERE id = f_parent_id;
    END WHILE;


    RETURN f_value;
END 
EOF

run "DROP FUNCTION IF EXISTS BHL_GetResourceCallNumber;"
run <<EOF
CREATE FUNCTION BHL_GetResourceCallNumber(f_resource_id INT) 
    RETURNS VARCHAR(255)
    READS SQL DATA
BEGIN
    DECLARE f_value VARCHAR(255);


    SELECT
        identifier INTO f_value
    FROM
        resource
    WHERE
        resource.id = f_resource_id;

    RETURN f_value;
END 
EOF


run "DROP FUNCTION IF EXISTS BHL_GetArchivalObjectDigitalObject;"
run <<EOF
CREATE FUNCTION BHL_GetArchivalObjectDigitalObject(f_archival_object_id INT) 
    RETURNS INT
    READS SQL DATA
BEGIN
    DECLARE f_value INT;

    SELECT
        digital_object.`id` INTO f_value
    FROM
        digital_object
    JOIN
        instance_do_link_rlshp ON instance_do_link_rlshp.`digital_object_id` = digital_object.`id`
    JOIN
        instance ON instance.`id` = instance_do_link_rlshp.`instance_id`
    WHERE
        instance.`archival_object_id` = f_archival_object_id LIMIT 1;

    RETURN f_value;
END 
EOF

    end
  end
end