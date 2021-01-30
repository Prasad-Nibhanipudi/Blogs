-- Sample Table for Audit demonstration
DROP TABLE if exists tablename;
CREATE TABLE tablename(counter bigint , pkey varchar(50) primary key, col1_audit float, col2_audit varchar(50),col3_audit timestamp,transacted timestamp, col4 varchar(2048), col5 varchar(2048),col6 varchar(2048),col7 varchar(2048),col8 varchar(2048),col9 varchar(100),col10 varchar(2048), CREATEd_by varchar(50),modified_by varchar(50) );

-- Create Audit Table for sample Table

DROP TABLE if exists audit__tablename;
DROP TRIGGER if exists audit__tablename on tablename;

CREATE TABLE audit__tablename(id bigserial primary key, pkey varchar(50) not null, col1_audit float, col2_audit varchar(50),audit_operation varchar(1),audit_timestamp timestamp,audit_author varchar(50));
CREATE index audit__tablename__pkey on 	audit__tablename(pkey);
CREATE index audit__tablename__timestamp on 	audit__tablename(audit_timestamp);

-- extension dependencies
CREATE EXTENSION IF NOT EXISTS HSTORE; 

-- Generic Audit Trigger assuming fixed Audit columns 
CREATE OR REPLACE FUNCTION audit__tablename() RETURNS TRIGGER AS $BODY$ 
	DECLARE 
			table_pkey character varying(50); 
			result_hstore hstore; 
			new_hstore hstore; 
			old_hstore hstore; 
			audit_operation character varying(1); 											
	BEGIN 
			IF (TG_OP = 'UPDATE') then 
				table_pkey = NEW.pkey; 
				new_hstore = slice(hstore(NEW), TG_ARGV); 
				old_hstore = slice(hstore(OLD), TG_ARGV); 
				result_hstore = new_hstore-old_hstore;
				result_hstore = result_hstore || hstore('audit_author'::text,NEW.modified_by);  
				audit_operation = 'U'; 
			ELSIF (TG_OP = 'INSERT') then 
				table_pkey = NEW.pkey; 
				result_hstore = slice(hstore(NEW), TG_ARGV); 
				audit_operation = 'I'; 
				result_hstore = result_hstore || hstore('audit_author'::text,NEW.CREATED_BY);
			ELSIF (TG_OP = 'DELETE') then 
				table_pkey = OLD.pkey; 
				result_hstore = slice(hstore(OLD), TG_ARGV); 
				audit_operation = 'D'; 
				result_hstore = result_hstore || hstore('audit_author'::text,OLD.MODIFIED_BY); 
			END IF; 
			if (array_length(akeys(result_hstore),1) > 0) then
			 result_hstore = result_hstore || hstore('pkey'::text,table_pkey::text);
			 result_hstore = result_hstore || hstore('id'::text,nextval('audit__tablename_id_seq'::regclass)::text);
			 result_hstore = result_hstore || hstore('audit_operation'::text,audit_operation::text);
			 result_hstore = result_hstore || hstore('audit_timestamp'::text,now()::text);
			--RAISE NOTICE 'Audit Record=%', result_hstore;
			 INSERT INTO audit__tablename SELECT  * FROM populate_record(NULL::audit__tablename, result_hstore);	
			END if; 
			RETURN NULL; 
	END; 
$BODY$ LANGUAGE PLPGSQL COST 75;

-- Associate Audit Trigger to sample Table on specific columns
CREATE TRIGGER audit__tablename 
  AFTER INSERT OR UPDATE OR DELETE 
  ON tablename 
  FOR EACH ROW 
  EXECUTE PROCEDURE audit__tablename('col1_audit','col2_audit','col3_audit');

-- ------- Test --------------------------------------
-- Helper Random functions
DROP function if exists random_string(length integer,range integer);
CREATE OR REPLACE FUNCTION random_string(length integer,range integer)
RETURNS varchar AS $$
  SELECT array_to_string(ARRAY(
           SELECT substr(upper('abcdefghijklmnopqrstuvxyz0123456789'),trunc(random()*$2+1)::int,1)       
             FROM generate_series(1,$1)),'')
$$ LANGUAGE sql VOLATILE;

-- Helper insert statement
insert into tablename(counter,pkey,col1_audit,col2_audit,col3_audit,CREATEd_by,modified_by,col4,col5,col6,col7,col8,col9,col10) 
	select i,random_string(36,36),100+i,random_string(36,36),now()-'1 hour'::interval,'CREATEd_by'||i,'modified_by'||i, random_string(36,36),random_string(36,36),random_string(50,25) , random_string(50,25),random_string(50,25),random_string(50,25),random_string(50,25) 
	from generate_series(1,100000) i;


-- Test cases
update tablename set col1_audit=133.4, col3_audit=now() where counter%100=0;

delete from tablename  where counter%100=0;

-- Audit Read Queries from UI
select
  pkey,
  case
  when col1_audit is null then (
    select col1_audit from audit__tablename a
    where pkey = a.pkey
      and audit_timestamp <= a.audit_timestamp
      and col1_audit is not null
    limit 1)
  else col1_audit
  END as col1_audit,
  case
  when col2_audit is null then (
    select col2_audit from audit__tablename a
    where pkey = a.pkey
      and audit_timestamp <= a.audit_timestamp
      and col2_audit is not null
    limit 1)
  else col2_audit
  END as col2_audit,
  audit_operation,
	audit_author,  
	audit_timestamp
from 
audit__tablename
order by audit_timestamp desc
limit 100;