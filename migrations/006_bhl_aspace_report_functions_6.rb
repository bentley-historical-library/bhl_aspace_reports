require 'db/migrations/utils'

Sequel.migration do
  up do
    if $db_type == :mysql

# last name, first name, middle name, suffix, title, organization or unit
run "DROP FUNCTION IF EXISTS GetAgentLastName;"
run <<EOF
CREATE FUNCTION GetAgentLastName(f_person_id INT, f_family_id INT, f_corporate_id INT)
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

run "DROP FUNCTION IF EXISTS GetAgentRestOfName;"
run <<EOF
CREATE FUNCTION GetAgentRestOfName(f_person_id INT, f_family_id INT, f_corporate_id INT)
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

run "DROP FUNCTION IF EXISTS GetAgentSuffix;"
run <<EOF
CREATE FUNCTION GetAgentSuffix(f_person_id INT, f_family_id INT, f_corporate_id INT)
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

run "DROP FUNCTION IF EXISTS GetAgentTitle;"
run <<EOF
CREATE FUNCTION GetAgentTitle(f_person_id INT, f_family_id INT, f_corporate_id INT)
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

run "DROP FUNCTION IF EXISTS GetAgentOrganizationOrUnit;"
run <<EOF
CREATE FUNCTION GetAgentOrganizationOrUnit(f_person_id INT, f_family_id INT, f_corporate_id INT)
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

run "DROP FUNCTION IF EXISTS GetAgentAddress2;"
run <<EOF
CREATE FUNCTION GetAgentAddress2(f_person_id INT, f_family_id INT, f_corporate_id INT)
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


	end
  end
end