--
-- trigger function to route inserts from a parent table to the correct child
-- partition
--
CREATE OR REPLACE FUNCTION zbx_route_insert_by_clock()
	RETURNS TRIGGER AS $$
	DECLARE
		schema_name TEXT;
		table_name	TEXT;
		time_format	TEXT;
	BEGIN
		-- trigger arguments
		schema_name	:= TG_ARGV[0];
		time_format	:= TG_ARGV[1];

		-- compute destination partition by appending NEW.clock to the original
		-- table name, formatted using time_format
		table_name := schema_name || '.' || TG_TABLE_NAME || '_'
			|| TO_CHAR(TO_TIMESTAMP(NEW.clock), time_format);

		EXECUTE 'INSERT INTO ' || table_name || ' SELECT ($1).*;' USING NEW;
		RETURN NULL;
	END;
$$ LANGUAGE plpgsql;

--
-- function to provision partition tables in advance
--
-- table_name:		table to create child partitions for (e.g. history)
-- partition_by:	period per partition [day|month|year] (default: month)
-- count:			number of partitions to provision, starting from NOW() 
-- 					(default: 12)
-- schema_name:		target schema where partitions are stored
-- 					(default: partitions)
--
CREATE OR REPLACE FUNCTION zbx_provision_partitions(
	table_name		TEXT
	, partition_by	TEXT	DEFAULT 'month'
	, count			BIGINT	DEFAULT 12
	, schema_name	TEXT	DEFAULT 'partitions'
) RETURNS VOID AS $$
	DECLARE
		time_format			TEXT;
		time_interval		INTERVAL;
		start_date			INTEGER;
		end_date			INTEGER;
		new_table_name		TEXT;
		new_constraint_name	TEXT;
		new_trigger_name	TEXT;
	BEGIN
		-- set time_format, used to format the partition suffix
		CASE partition_by
			WHEN 'day'		THEN time_format := 'YYYY_MM_DD';
			WHEN 'month'	THEN time_format := 'YYYY_MM';
			WHEN 'year'		THEN time_format := 'YYYY';
			ELSE RAISE 'Unsupported partition_by value: %', partition_by;
		END CASE;

		-- compute time interval for partition period
		time_interval := '1 ' || partition_by;

		-- create <count> new partitions
		FOR i IN 0..(count - 1) LOOP
			start_date			:= EXTRACT(EPOCH FROM DATE_TRUNC(partition_by, NOW() + (time_interval * i)));
			end_date			:= EXTRACT(EPOCH FROM DATE_TRUNC(partition_by, NOW() + (time_interval * (i + 1))));
			new_table_name		:= table_name || '_' || TO_CHAR(NOW() + (time_interval * i), time_format);
			new_constraint_name	:= new_table_name || '_clock';
			new_trigger_name	:= table_name || '_insert';

			-- check if table exists
			BEGIN
				PERFORM (schema_name || '.' || new_table_name)::regclass;
				RAISE NOTICE 'partition already exist: %.%', schema_name, new_table_name; 

			EXCEPTION WHEN undefined_table THEN
				-- create missing table, copying schema from parent table
				EXECUTE 'CREATE TABLE ' || schema_name || '.' || new_table_name || ' (
					LIKE ' || table_name || '
						INCLUDING DEFAULTS
						INCLUDING CONSTRAINTS
						INCLUDING INDEXES
				) INHERITS (' || table_name || ');';

				-- add clock column constraint
				EXECUTE 'ALTER TABLE ' || schema_name || '.' || new_table_name
					|| ' ADD CONSTRAINT ' || new_constraint_name
					|| ' CHECK ( clock >= ' || start_date || ' AND clock < ' || end_date || ' );';				
			END;
		END LOOP;
		
		-- trigger the routing function on insert to the parent table
		-- TODO: is there a race condition here if a row is inserted BEFORE the
		-- trigger is recreated? Rows could leak into the parent table.
		EXECUTE 'DROP TRIGGER IF EXISTS ' || QUOTE_IDENT(new_trigger_name) || ' ON ' || QUOTE_IDENT(table_name) || ';';
		EXECUTE 'CREATE TRIGGER ' || QUOTE_IDENT(new_trigger_name) || '
			BEFORE INSERT ON ' || QUOTE_IDENT(table_name) || '
			FOR EACH ROW EXECUTE PROCEDURE zbx_route_insert_by_clock(' || QUOTE_IDENT(schema_name) || ', ' || QUOTE_LITERAL(time_format) || ');';
	END
$$ LANGUAGE plpgsql;

--
-- function to 'unpartition' a table by copying data from all child partitions
-- into the parent table, deleting the partitions and removing the partitioning
-- triggers.
-- 
-- WARNING: all insert triggers must be removed from the parent table to ensure
-- copied rows are inserted into the parent; not back into the child partitions.
--
-- You should probably stop the Zabbix server while running this function.
-- Otherwise new value are inserted into the parent table BEFORE the data is
-- copied from child tables. Data are then no longer sequential.
-- 
-- All child tables are dropped!
--
-- table_name:			parent table name
-- trigger_name:		name of the trigger to be dropped from the parent table
-- 						(default: {table_name}_insert)
-- schema_name:			parent table schema name (default: public)
--
CREATE OR REPLACE FUNCTION zbx_deprovision_partitions(
	table_name		TEXT
	, trigger_name	TEXT	DEFAULT ''
	, schema_name	TEXT	DEFAULT 'public'
) RETURNS VOID AS $$
	DECLARE
		child		RECORD;
		ins_count	INTEGER DEFAULT 0;
	BEGIN
		-- default trigger name
		IF trigger_name = '' THEN 
			trigger_name = table_name || '_insert';
		END IF;

		-- delete the insert trigger on the parent table
		EXECUTE 'DROP TRIGGER ' || trigger_name || ' ON ' || schema_name || '.' || table_name || ' CASCADE;';

		-- loop through child tables
		FOR child IN (
			SELECT 
				n.nspname	AS schema_name
				, c.relname	AS table_name
			FROM pg_inherits i
			JOIN pg_class c ON i.inhrelid = c.oid
			JOIN pg_namespace n ON c.relnamespace = n.oid
			WHERE i.inhparent = table_name::regclass
			ORDER BY c.relname ASC --sort by name so we insert oldest first
		) LOOP
			-- copy content into parent table
			EXECUTE 'INSERT INTO ' || schema_name || '.' || table_name || ' SELECT * FROM ONLY ' || child.schema_name || '.' || child.table_name;
			GET DIAGNOSTICS ins_count := ROW_COUNT;
			
			-- drop partition
			EXECUTE 'DROP TABLE ' || child.schema_name || '.' || child.table_name || ';';

			-- notify
			RAISE NOTICE 'Copied % rows from %.%', ins_count, child.schema_name, child.table_name;
		END LOOP;

		-- update stats for parent table
		EXECUTE 'ANALYZE ' || schema_name || '.' || table_name || ';';
	END
$$ LANGUAGE plpgsql;

--
-- function to constrain a child partition table by the minimum and maximum id
--
-- stops a partition being scanned for ids that are out of range.
-- 
-- WARNING: do no apply to a table that will still be appended to
--
-- table_name:	child table to constrain
-- column_name:	numeric ID column to constrain by
-- schema_name:	schema where child table exists (default: partitions)
--
CREATE OR REPLACE FUNCTION zbx_constrain_partition(
	table_name		TEXT
	, column_name	TEXT
	, schema_name	TEXT	DEFAULT 'partitions'
) RETURNS VOID AS $$
	DECLARE
		min_id				BIGINT DEFAULT 0;
		max_id				BIGINT DEFAULT 0;
		new_constraint_name	TEXT;
	BEGIN
		new_constraint_name := table_name || '_' || column_name;

		-- find minimum and maximum id
		EXECUTE 'SELECT MIN(' || column_name || '), MAX(' || column_name || ') FROM ONLY ' || schema_name || '.' || table_name || ';' INTO min_id, max_id;

		-- remove existing constraint
		EXECUTE 'ALTER TABLE ' || schema_name || '.' || table_name
			|| ' DROP CONSTRAINT IF EXISTS ' || new_constraint_name || ';';

		-- add constraint
		EXECUTE 'ALTER TABLE ' || schema_name || '.' || table_name
			|| ' ADD CONSTRAINT ' || new_constraint_name
			|| ' CHECK ( ' || column_name || ' >= ' || min_id || ' AND ' || column_name || ' <= ' || max_id || ' );';

		RAISE NOTICE 'Added constraint % ( % >= % <= % )', new_constraint_name, min_id, column_name, max_id;
	END
$$ LANGUAGE plpgsql;
