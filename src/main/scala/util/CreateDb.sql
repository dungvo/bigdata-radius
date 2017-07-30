CREATE DATABASE dwh_noc;






CREATE TABLE IF NOT EXISTS dwh_kibana (
date_time	timestamp,
bras_id	varchar(20),
error_name	varchar(50),
error_level	varchar(30)
);

CREATE TABLE IF NOT EXISTS dwh_opsview (
date_time	timestamp,
bras_id	varchar(20),
service_name	varchar(50),
service_status	varchar(30)
);

CREATE TABLE IF NOT EXISTS dwh_temp_bras (
bras_id	varchar(20),
host	varchar(30),
module	varchar(3),
index	varchar(3)
);

CREATE TABLE IF NOT EXISTS dwh_temp_inf (
date_time	timestamp,
host	varchar(30),
module	varchar(3),
index	varchar(3),
cpe_error	int,
lostip_error	int
);

CREATE TABLE IF NOT EXISTS dwh_inf_index (
date_time	timestamp,
bras_id	varchar(20),
host_endpoint varchar(30),
host varchar(30),
module_ol varchar(3),
index varchar(3),
cpe_error int,
lostip_error int
);

CREATE TABLE IF NOT EXISTS dwh_inf_module (
date_time	timestamp,
bras_id	varchar(20),
host	varchar(30),
module	varchar(3),
cpe_error	int,
lostip_error	int
);

CREATE TABLE IF NOT EXISTS dwh_inf_host (
date_time	timestamp,
bras_id	varchar(20),
host	varchar(30),
cpe_error	int,
lostip_error	int
);


CREATE TABLE IF NOT EXISTS brashostmapping(
bras_id varchar(30),
olt varchar(20),
portPON varchar(10),
host_endpoint varchar(30),
PRIMARY KEY(host_endpoint)
);



CREATE TABLE IF NOT EXISTS bras_count_by_port (
bras_id	varchar(20),
line_ol	varchar(3),
card_ol	varchar(3),
port_ol	varchar(3),
port varchar(30),
time	timestamp,
signin_total_count_by_port	int,
logoff_total_count_by_port	int,
signin_distinct_count_by_port	int,
logoff_distinct_count_by_port	int
);

CREATE TABLE IF NOT EXISTS bras_count_by_card (
bras_id	varchar(20),
line_ol	varchar(3),
card_ol	varchar(3),
card varchar(30),
time	timestamp,
signin_total_count_by_card	int,
logoff_total_count_by_card	int,
signin_distinct_count_by_card	int,
logoff_distinct_count_by_card	int
);

CREATE TABLE IF NOT EXISTS bras_count_by_line (
bras_id	varchar(20),
line_ol varchar(3),
line	varchar(30),
time	timestamp,
signin_total_count_by_line	int,
logoff_total_count_by_line	int,
signin_distinct_count_by_line	int,
logoff_distinct_count_by_line	int
);

CREATE TABLE IF NOT EXISTS bras_count (
bras_id	varchar(20),
time	timestamp,
signin_total_count	int,
logoff_total_count	int,
signin_distinct_count	int,
logoff_distinct_count	int
);

CREATE TABLE IF NOT EXISTS dwh_radius_bras_detail (
date_time	timestamp,
bras_id	varchar(20),
active_user	int,
signin	int,
logoff	int,
cpe_error	int,
lostip_error	int,
crit_kibana	int,
info_kibana	int,
crit_opsview	int,
ok_opsview	int,
warn_opsview	int,
unknow_opsview	int,
);
CREATE TABLE IF NOT EXISTS inf_error(
	log_type varchar(30),
	host varchar(20),
	module_ol varchar(8),
	times_stamp timestamp,
	module varchar(30)
);

CREATE TABLE IF NOT EXISTS inserttest(
bras_id varchar(30),
olt varchar(30),
portPON varchar(30),
time timestamp
);

CREATE TABLE IF NOT EXISTS result_inf_tmp(
	date_time timestamp,
	host varchar(20),
	host_endpoint varchar(30),
	index varchar(10),
	cpe_error int,
	lostip_error int,
	module_ol varchar(10)

);
	
insert into test_table1 (col2,..., coln) select col2,...,coln from table1;


insert into inserttest(bras_id,olt,portpon,time) 
	select b.bras_id, bh.olt, bh.portPON,b.time 
	from bras_count b join brashostmapping bh on b.bras_id = bh.bras_id ;

HCM-MP04-1-NEW/11/1/0


val logType: String,
                           val hostName: String,
                           val date: String,
                           val time: String,
                           val module: String



                            host_endpoint    | text                        |           | extended |              | 
 time             | timestamp without time zone |           | plain    |              | 
 module/cpe error | integer                     |           | plain    |              | 
 lostip_error     | integer                     |           | plain    |              | 
 host             | text                        |           | extended |              | 
 module_ol        | text                        |           | extended |              | 
 index_ol         | text                        |           | extended |              | 