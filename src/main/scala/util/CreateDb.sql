CREATE  DATABASE  dwh_noc;

CREATE  TABLE  IF  NOT  EXISTS  dwh_kibana  (
date_time        timestamp  ,
bras_id  varchar(30)  ,
error_name        varchar(50)  ,
error_level        varchar(30),
	PRIMARY  KEY  (bras_id,date_time)
);


CREATE  TABLE  IF  NOT  EXISTS  dwh_kibana_agg(
        date_time  timestamp,
        bras_id  varchar(30),
        total_critical_count  int,
        total_info_count  int,
        PRIMARY  KEY  (bras_id,date_time)
);


CREATE  TABLE  IF  NOT  EXISTS  dwh_opsview  (
date_time        timestamp,
bras_id           varchar(50),
service_name        varchar(50),
service_status        varchar(30),
message  text
);

CREATE  TABLE  IF  NOT  EXISTS  dwh_opsview_status  (
date_time        timestamp,
bras_id           varchar(50),
warn_opsview        int,
unknown_opsview        int,
ok_opsview        int,
crit_opsview        int
);



CREATE  TABLE  IF  NOT  EXISTS  dwh_temp_inf  (
date_time        timestamp,
host        varchar(30),
module        varchar(3),
index        varchar(3),
cpe_error        int,
lostip_error        int
);

CREATE  TABLE  IF  NOT  EXISTS  dwh_inf_index  (
date_time        timestamp,
bras_id           varchar(20),
host_endpoint  varchar(30),
host  varchar(30),
module_ol  varchar(3),
index  varchar(3),
cpe_error  int,
lostip_error  int
);

CREATE  TABLE  IF  NOT  EXISTS  dwh_inf_module  (
date_time        timestamp,
bras_id           varchar(20),
host        varchar(30),
module        varchar(3),
cpe_error        int,
lostip_error        int
);

CREATE  TABLE  IF  NOT  EXISTS  dwh_inf_host  (
date_time        timestamp,
bras_id           varchar(20),
host        varchar(30),
cpe_error        int,
lostip_error        int
);


CREATE  TABLE  IF  NOT  EXISTS  brashostmapping(
bras_id  varchar(30),
olt  varchar(20),
portPON  varchar(10),
host_endpoint  varchar(30),
PRIMARY  KEY(host_endpoint)
);



CREATE  TABLE  IF  NOT  EXISTS  bras_by_port  (
bras_id           varchar(20),
line_ol        varchar(3),
card_ol        varchar(3),
port_ol        varchar(3),
port  varchar(30),
time        timestamp,
signin_total_count_by_port        int,
logoff_total_count_by_port        int,
signin_distinct_count_by_port        int,
logoff_distinct_count_by_port        int
);

CREATE  TABLE  IF  NOT  EXISTS  bras_by_card  (
bras_id           varchar(20),
line_ol        varchar(3),
card_ol        varchar(3),
card  varchar(30),
time        timestamp,
signin_total_count_by_card           int,
logoff_total_count_by_card           int,
signin_distinct_count_by_card           int,
logoff_distinct_count_by_card           int
);

CREATE  TABLE  IF  NOT  EXISTS  bras_by_line  (
bras_id           varchar(20),
line_ol  varchar(3),
line        varchar(30),
time        timestamp,
signin_total_count_by_line        int,
logoff_total_count_by_line        int,
signin_distinct_count_by_line        int,
logoff_distinct_count_by_line        int
);

CREATE  TABLE  IF  NOT  EXISTS  bras  (
bras_id           varchar(20),
time        timestamp,
signin_total_count        int,
logoff_total_count        int,
signin_distinct_count        int,
logoff_distinct_count        int
);




CREATE  TABLE  IF  NOT  EXISTS  dwh_radius_bras_detail (
date_time        timestamp,
bras_id           varchar(20),
active_user        int,
signin_total_count        int,
logoff_total_count        int,
signin_distinct_count     int,
logoff_distinct_count     int,
cpe_error        int,
lostip_error        int,
crit_kibana        int,
info_kibana        int,
crit_opsview        int,
ok_opsview        int,
warn_opsview        int,
unknown_opsview        int,
label  varchar(20),
PRIMARY  KEY(date_time,bras_id)
);

CREATE  TABLE  IF  NOT  EXISTS  inf_error(
        log_type  varchar(30),
        host  varchar(20),
        module_ol  varchar(8),
        times_stamp  timestamp,
        module  varchar(30)
);

CREATE  TABLE  IF  NOT  EXISTS  result_inf_tmp(
        date_time  timestamp,
        host  varchar(20),
        host_endpoint  varchar(30),
        index  varchar(10),
        cpe_error  int,
        lostip_error  int,
        module_ol  varchar(10)

);

CREATE TABLE IF NOT EXISTS threshold(
		bras_id varchar(30),
		threshold_signin int,
		threshold_logoff int,
		PRIMARY KEY(bras_id)
);


CREATE  TABLE  IF  NOT  EXISTS  inserttest(
bras_id  varchar(30),
olt  varchar(30),
portPON  varchar(30),
time  timestamp
);
CREATE TABLE active_user (bras_id varchar(30) PRIMARY KEY NOT NULL, active_users int) ;

CREATE TABLE IF NOT EXISTS verify(bras_id varchar(30),date_time timestamp,feedback int,PRIMARY KEY(bras_id,date_time));

CREATE TABLE IF NOT EXISTS bras_name_ip_mapping(
    bras_id varchar(30),
    bras_ip varchar(15),
    PRIMARY KEY(bras_id)
);
CREATE TABLE IF NOT EXISTS logoff_users(event_key varchar(50),bras_id varchar(30),time timestamp,user_list text);

        
insert  into  test_table1  (col2,...,  coln)  select  col2,...,coln  from  table1;


insert  into  inserttest(bras_id,olt,portpon,time)  
        select  b.bras_id,  bh.olt,  bh.portPON,b.time  
        from  bras  b  join  brashostmapping  bh  on  b.bras_id  =  bh.bras_id  ;



sudo docker network create -d bridge --subnet 172.30.41.0/24 --gateway 171.30.41.1 docke
FROM php:7.0-apache
RUN mkdir -p /var/www/html/bigdata_noc
COPY . /var/www/html/bigdata_noc/
WORKDIR /var/www/html/bigdata_noc
