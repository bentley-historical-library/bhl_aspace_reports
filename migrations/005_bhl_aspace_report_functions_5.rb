require 'db/migrations/utils'

Sequel.migration do
  up do
    if $db_type == :mysql

run "DROP FUNCTION IF EXISTS GetAgentContactName;"
run <<EOF
CREATE FUNCTION GetAgentContactName(f_person_id INT, f_family_id INT, f_corporate_id INT)
	RETURNS VARCHAR(255)
	READS SQL DATA
BEGIN
	DECLARE f_value VARCHAR(255);

	IF f_person_id IS NOT NULL THEN
		SELECT name INTO f_value FROM agent_contact WHERE agent_person_id = f_person_id LIMIT 1;
	ELSEIF f_family_id IS NOT NULL THEN
		SELECT name INTO f_value FROM agent_contact WHERE agent_family_id = f_family_id LIMIT 1;
	ELSEIF f_corporate_id IS NOT NULL THEN
		SELECT name INTO f_value FROM agent_contact WHERE agent_corporate_entity_id = f_corporate_id LIMIT 1;
	ELSE
		SET f_value = '';
	END IF;

	RETURN f_value;
END
EOF

run "DROP FUNCTION IF EXISTS GetAgentAddress;"
run <<EOF
CREATE FUNCTION GetAgentAddress(f_person_id INT, f_family_id INT, f_corporate_id INT)
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

run "DROP FUNCTION IF EXISTS GetAgentCity;"
run <<EOF
CREATE FUNCTION GetAgentCity(f_person_id INT, f_family_id INT, f_corporate_id INT)
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

run "DROP FUNCTION IF EXISTS GetAgentState;"
run <<EOF
CREATE FUNCTION GetAgentState(f_person_id INT, f_family_id INT, f_corporate_id INT)
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

run "DROP FUNCTION IF EXISTS GetAgentCountry;"
run <<EOF
CREATE FUNCTION GetAgentCountry(f_person_id INT, f_family_id INT, f_corporate_id INT)
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

run "DROP FUNCTION IF EXISTS GetAgentZipCode;"
run <<EOF
CREATE FUNCTION GetAgentZipCode(f_person_id INT, f_family_id INT, f_corporate_id INT)
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

	end
  end
end