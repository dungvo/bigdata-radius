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




CREATE  TABLE  IF  NOT  EXISTS  dwh_radius_bras_detail  (
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
        
insert  into  test_table1  (col2,...,  coln)  select  col2,...,coln  from  table1;


insert  into  inserttest(bras_id,olt,portpon,time)  
        select  b.bras_id,  bh.olt,  bh.portPON,b.time  
        from  bras  b  join  brashostmapping  bh  on  b.bras_id  =  bh.bras_id  ;

HCM-MP04-1-NEW/11/1/0


val  logType:  String,
                                                      val  hostName:  String,
                                                      val  date:  String,
                                                      val  time:  String,
                                                      val  module:  String



                                                        host_endpoint        |  text                                                |                      |  extended  |                            |  
  time                          |  timestamp  without  time  zone  |                      |  plain        |                            |  
  module/cpe  error  |  integer                                          |                      |  plain        |                            |  
  lostip_error          |  integer                                          |                      |  plain        |                            |  
  host                          |  text                                                |                      |  extended  |                            |  
  module_ol                |  text                                                |                      |  extended  |                            |  
  index_ol                  |  text                                                |                      |  extended  |                            |  




  insert  into  dwh_inf_index(bras_id,host_endpoint,host,module_ol,index,cpe_error,lostip_error,date_time)  select  bh.bras_id,i.host_endpoint,i.host,i.module_ol,i.index_ol,i.cpe_error,i.lostip_error,i.date_time  from  result_inf_tmp  i  join  (SELECT  *  FROM  brashostmapping  WHERE  host_endpoint  in  ('HNIP10603GC57/0/2/36','BGGP03101GC57/0/8/19','HNIP29904GC57/0/3/28','HCMP27103GC57/0/1/83','HNIP44001GC57/0/2/57','HNIP35303GC57/0/2/13','HNIP41201GC57/0/1/77','HCMP09805GC57/0/1/64','HNIP29904GC57/0/3/92','NTGP05702GC57/0/2/14','NTGP05301GC57/0/2/6','HCMP00901GC57/0/2/8','DNGP10802GC57/0/3/40','HCMP21806GC57/0/7/70','HCMP09303GC57/0/3/26','HCMP25901GC57/0/7/49','HCMP50703GC57/0/1/87','HNIP14301GC57/0/2/68','HNIP29904GC57/0/3/67','HNIP45503GC57/0/5/16','HYNP05401GC57/0/7/11','HCMP59802GC57/0/3/11','HCMP58201GC57/0/2/7','HNIP58102GC57/0/8/49','HNIP04802GC57/0/2/26','HCMP21806GC57/0/7/29','PTOP04201GC57/0/5/24','HDGP06201GC57/0/2/88','HNIP20603GC57/0/8/69','HCMP21806GC57/0/7/125','BRUP05701GC57/0/1/110','BDGP06903GC57/0/1/54','DNGP13301GC57/0/4/61','HNIP29904GC57/0/3/35','HNIP22003GC57/0/5/30','HCMP50803GC57/0/5/31','HCMP18101GC57/0/2/68','HPGP01402GC57/0/4/6','HNIP49703GC57/0/5/30','HCMP20102GC57/0/3/28','DNIP01603GC57/0/7/19','HNIP56801GC57/0/8/10','HNIP51301GC57/0/8/13','HNIP57202GC57/0/1/3','HCMP21806GC57/0/7/100','HCMP40007GC57/0/7/57','HNIP29904GC57/0/3/89','TNNP03301GC57/0/3/40','HCMP37101GC57/0/4/14','HNIP29904GC57/0/3/31','DNGP10302GC57/0/5/3','HCMP21806GC57/0/7/111','HNIP22401GC57/0/4/92','HNIP30902GC57/0/1/2','NTGP05901GC57/0/6/16','HPGP11302GC57/0/1/23','HDGP04301GC57/0/6/52','HNIP03602GC57/0/3/34','HNIP15401GC57/0/7/34','HPGP06104GC57/0/8/45','AGGP04501GC57/0/5/93','HNIP29904GC57/0/3/120','HCMP01610GC57/0/3/26','HCMP49303GC57/0/7/5','HCMP09302GC57/0/2/11','HCMP21806GC57/0/7/107','HNIP40302GC57/0/7/4','HUEP08901GC57/0/3/31','VPCP02201GC57/0/8/1','HNIP48402GC57/0/7/67','HNIP07202GC57/0/4/94','NTGP06302GC57/0/6/101','HNIP29904GC57/0/3/47','PTOP04101GC57/0/2/31','HDGP04301GC57/0/7/19','HCMP21806GC57/0/7/113','HCMP47404GC57/0/3/31')  )  bh  on  i.host_endpoint  =  bh.host_endpoint



  INSERT  INTO  result_inf_tmp  (host_endpoint,  date_time,  cpe_error,  lostip_error,  host,  module_ol,  index_ol)  VALUES    ('HNIP61101GC57/0/3/76','2017-06-13  20:03:23',22,12,'HNIP61101GC57','3','76');

HNIP61101GC57/0/3/76


insert  into  dwh_inf_index(bras_id,host_endpoint,host,module_ol,index,cpe_error,lostip_error,date_time)  select  bh.bras_id,i.host_endpoint,i.host,i.module_ol,i.index_ol,i.cpe_error,i.lostip_error,i.date_time  from  result_inf_tmp  i  join  (SELECT  *  FROM  brashostmapping  WHERE  host_endpoint  in  ('HNIP10603GC57/0/2/36','BGGP03101GC57/0/8/19','HNIP29904GC57/0/3/28','HCMP27103GC57/0/1/83','HNIP44001GC57/0/2/57','HNIP41201GC57/0/1/77','HNIP61101GC57/0/3/76','HCMP09805GC57/0/1/64','HNIP29904GC57/0/3/92','NTGP05702GC57/0/2/14','NTGP05301GC57/0/2/6','HCMP00901GC57/0/2/8','DNGP10802GC57/0/3/40','HCMP21806GC57/0/7/70','HCMP09303GC57/0/3/26','HCMP25901GC57/0/7/49','HCMP50703GC57/0/1/87','HNIP14301GC57/0/2/68','HNIP29904GC57/0/3/67','HNIP45503GC57/0/5/16','HYNP05401GC57/0/7/11','HCMP59802GC57/0/3/11','HCMP58201GC57/0/2/7','HNIP58102GC57/0/8/49','HNIP04802GC57/0/2/26','HCMP21806GC57/0/7/29','PTOP04201GC57/0/5/24','HDGP06201GC57/0/2/88','HNIP20603GC57/0/8/69','HCMP21806GC57/0/7/125','BRUP05701GC57/0/1/110','BDGP06903GC57/0/1/54','DNGP13301GC57/0/4/61','HNIP29904GC57/0/3/35','HNIP22003GC57/0/5/30','HCMP50803GC57/0/5/31','HCMP18101GC57/0/2/68','HPGP01402GC57/0/4/6','HNIP49703GC57/0/5/30','HCMP20102GC57/0/3/28','DNIP01603GC57/0/7/19','HNIP56801GC57/0/8/10','HNIP51301GC57/0/8/13','HNIP57202GC57/0/1/3','HCMP21806GC57/0/7/100','HCMP40007GC57/0/7/57','HNIP29904GC57/0/3/89','TNNP03301GC57/0/3/40','HCMP37101GC57/0/4/14','HNIP29904GC57/0/3/31','DNGP10302GC57/0/5/3','HCMP21806GC57/0/7/111','HNIP22401GC57/0/4/92','HNIP30902GC57/0/1/2','NTGP05901GC57/0/6/16','HPGP11302GC57/0/1/23','HDGP04301GC57/0/6/52','HNIP03602GC57/0/3/34','HNIP15401GC57/0/7/34','HPGP06104GC57/0/8/45','AGGP04501GC57/0/5/93','HNIP29904GC57/0/3/120','HCMP01610GC57/0/3/26','HCMP49303GC57/0/7/5','HCMP09302GC57/0/2/11','HCMP21806GC57/0/7/107','HNIP40302GC57/0/7/4','HUEP08901GC57/0/3/31','VPCP02201GC57/0/8/1','HNIP48402GC57/0/7/67','HNIP07202GC57/0/4/94','NTGP06302GC57/0/6/101','HNIP29904GC57/0/3/47','PTOP04101GC57/0/2/31','HDGP04301GC57/0/7/19','HCMP21806GC57/0/7/113','HCMP47404GC57/0/3/31')  )  bh  on  i.host_endpoint  =  bh.host_endpoint

        insert  into  dwh_inf_host(bras_id,host,cpe_error,lostip_error,date_time)  
                select  bh.bras_id,i.host,SUM(i.cpe_error),SUM(i.lostip_error),i.date_time  
                from  result_inf_tmp  i  join  
                        (SELECT  *  FROM  brashostmapping  
                                WHERE  host_endpoint  in  
                                ('HNIP10603GC57/0/2/36','BGGP03101GC57/0/8/19','HNIP29904GC57/0/3/28','HCMP27103GC57/0/1/83','HNIP44001GC57/0/2/57','HNIP41201GC57/0/1/77','HNIP61101GC57/0/3/76','HCMP09805GC57/0/1/64','HNIP29904GC57/0/3/92','NTGP05702GC57/0/2/14','NTGP05301GC57/0/2/6','HCMP00901GC57/0/2/8','DNGP10802GC57/0/3/40','HCMP21806GC57/0/7/70','HCMP09303GC57/0/3/26','HCMP25901GC57/0/7/49','HCMP50703GC57/0/1/87','HNIP14301GC57/0/2/68','HNIP29904GC57/0/3/67','HNIP45503GC57/0/5/16','HYNP05401GC57/0/7/11','HCMP59802GC57/0/3/11','HCMP58201GC57/0/2/7','HNIP58102GC57/0/8/49','HNIP04802GC57/0/2/26','HCMP21806GC57/0/7/29','PTOP04201GC57/0/5/24','HDGP06201GC57/0/2/88','HNIP20603GC57/0/8/69','HCMP21806GC57/0/7/125','BRUP05701GC57/0/1/110','BDGP06903GC57/0/1/54','DNGP13301GC57/0/4/61','HNIP29904GC57/0/3/35','HNIP22003GC57/0/5/30','HCMP50803GC57/0/5/31','HCMP18101GC57/0/2/68','HPGP01402GC57/0/4/6','HNIP49703GC57/0/5/30','HCMP20102GC57/0/3/28','DNIP01603GC57/0/7/19','HNIP56801GC57/0/8/10','HNIP51301GC57/0/8/13','HNIP57202GC57/0/1/3','HCMP21806GC57/0/7/100','HCMP40007GC57/0/7/57','HNIP29904GC57/0/3/89','TNNP03301GC57/0/3/40','HCMP37101GC57/0/4/14','HNIP29904GC57/0/3/31','DNGP10302GC57/0/5/3','HCMP21806GC57/0/7/111','HNIP22401GC57/0/4/92','HNIP30902GC57/0/1/2','NTGP05901GC57/0/6/16','HPGP11302GC57/0/1/23','HDGP04301GC57/0/6/52','HNIP03602GC57/0/3/34','HNIP15401GC57/0/7/34','HPGP06104GC57/0/8/45','AGGP04501GC57/0/5/93','HNIP29904GC57/0/3/120','HCMP01610GC57/0/3/26','HCMP49303GC57/0/7/5','HCMP09302GC57/0/2/11','HCMP21806GC57/0/7/107','HNIP40302GC57/0/7/4','HUEP08901GC57/0/3/31','VPCP02201GC57/0/8/1','HNIP48402GC57/0/7/67','HNIP07202GC57/0/4/94','NTGP06302GC57/0/6/101','HNIP29904GC57/0/3/47','PTOP04101GC57/0/2/31','HDGP04301GC57/0/7/19','HCMP21806GC57/0/7/113','HCMP47404GC57/0/3/31')  )
                          bh  on  i.host_endpoint  =  bh.host_endpoint
                GROUP  BY  i.host,bh.bras_id,i.date_time  ;

        insert  into  dwh_inf_module(bras_id,host,module,cpe_error,lostip_error,date_time)  
                select  bh.bras_id,i.host,i.module_ol,SUM(i.cpe_error),SUM(i.lostip_error),i.date_time  
                from  result_inf_tmp  i  join  
                        (SELECT  *  FROM  brashostmapping  
                                WHERE  host_endpoint  in  
                                ('HNIP10603GC57/0/2/36','BGGP03101GC57/0/8/19','HNIP29904GC57/0/3/28','HCMP27103GC57/0/1/83','HNIP44001GC57/0/2/57','HNIP41201GC57/0/1/77','HNIP61101GC57/0/3/76','HCMP09805GC57/0/1/64','HNIP29904GC57/0/3/92','NTGP05702GC57/0/2/14','NTGP05301GC57/0/2/6','HCMP00901GC57/0/2/8','DNGP10802GC57/0/3/40','HCMP21806GC57/0/7/70','HCMP09303GC57/0/3/26','HCMP25901GC57/0/7/49','HCMP50703GC57/0/1/87','HNIP14301GC57/0/2/68','HNIP29904GC57/0/3/67','HNIP45503GC57/0/5/16','HYNP05401GC57/0/7/11','HCMP59802GC57/0/3/11','HCMP58201GC57/0/2/7','HNIP58102GC57/0/8/49','HNIP04802GC57/0/2/26','HCMP21806GC57/0/7/29','PTOP04201GC57/0/5/24','HDGP06201GC57/0/2/88','HNIP20603GC57/0/8/69','HCMP21806GC57/0/7/125','BRUP05701GC57/0/1/110','BDGP06903GC57/0/1/54','DNGP13301GC57/0/4/61','HNIP29904GC57/0/3/35','HNIP22003GC57/0/5/30','HCMP50803GC57/0/5/31','HCMP18101GC57/0/2/68','HPGP01402GC57/0/4/6','HNIP49703GC57/0/5/30','HCMP20102GC57/0/3/28','DNIP01603GC57/0/7/19','HNIP56801GC57/0/8/10','HNIP51301GC57/0/8/13','HNIP57202GC57/0/1/3','HCMP21806GC57/0/7/100','HCMP40007GC57/0/7/57','HNIP29904GC57/0/3/89','TNNP03301GC57/0/3/40','HCMP37101GC57/0/4/14','HNIP29904GC57/0/3/31','DNGP10302GC57/0/5/3','HCMP21806GC57/0/7/111','HNIP22401GC57/0/4/92','HNIP30902GC57/0/1/2','NTGP05901GC57/0/6/16','HPGP11302GC57/0/1/23','HDGP04301GC57/0/6/52','HNIP03602GC57/0/3/34','HNIP15401GC57/0/7/34','HPGP06104GC57/0/8/45','AGGP04501GC57/0/5/93','HNIP29904GC57/0/3/120','HCMP01610GC57/0/3/26','HCMP49303GC57/0/7/5','HCMP09302GC57/0/2/11','HCMP21806GC57/0/7/107','HNIP40302GC57/0/7/4','HUEP08901GC57/0/3/31','VPCP02201GC57/0/8/1','HNIP48402GC57/0/7/67','HNIP07202GC57/0/4/94','NTGP06302GC57/0/6/101','HNIP29904GC57/0/3/47','PTOP04101GC57/0/2/31','HDGP04301GC57/0/7/19','HCMP21806GC57/0/7/113','HCMP47404GC57/0/3/31')  )
                          bh  on  i.host_endpoint  =  bh.host_endpoint
                GROUP  BY  i.host,bh.bras_id,i.date_time,i.module_ol  ;          



          val  query  =  "insert  into  dwh_inf_index(bras_id,host_endpoint,host,module_ol,index,cpe_error,lostip_error,date_time)"  +
                        "  select  bh.bras_id,i.host_endpoint,i.host,i.module_ol,i.index_ol,i.cpe_error,i.lostip_error,i.date_time  from  result_inf_tmp  i  join  "  +
                        "(SELECT  *  FROM  brashostmapping  WHERE  host_endpoint  in  "+host_endpoint_IdsString+  "  )  bh  on  i.host_endpoint  =  bh.host_endpoint  "
                    println(query  )

        val  insertINFModuleQuery  =  "insert  into  dwh_inf_module(bras_id,host,module,cpe_error,lostip_error,date_time)  
                select  bh.bras_id,i.host,i.module_ol,SUM(i.cpe_error),SUM(i.lostip_error),i.date_time  
                from  result_inf_tmp  i  join  
                        (SELECT  *  FROM  brashostmapping  
                                WHERE  host_endpoint  in  "  +  host_endpoint_IdsString  +
                          "  bh  on  i.host_endpoint  =  bh.host_endpoint
                GROUP  BY  i.host,bh.bras_id,i.date_time,i.module_ol"      

        val  insertINFHostQuery  =  "insert  into  dwh_inf_host(bras_id,host,cpe_error,lostip_error,date_time)  
                select  bh.bras_id,i.host,SUM(i.cpe_error),SUM(i.lostip_error),i.date_time  
                from  result_inf_tmp  i  join  
                        (SELECT  *  FROM  brashostmapping  
                                WHERE  host_endpoint  in  "  +  host_endpoint_IdsString  +
                          "  bh  on  i.host_endpoint  =  bh.host_endpoint
                GROUP  BY  i.host,bh.bras_id,i.date_time  ;"                      INF-HDG-HDGP03601GC56-10.10.227.54
                INF-HPG-HPGP02302GC57-10.10.219.124


SELECT  *
FROM  (SELECT  *  FROM  bras  WHERE  time  >  '$point_of_time')  bras  
        left  join
          (SELECT  bras_id,SUM(total_critical_count)  as  crit_kibana,SUM(total_info_count)  as  info_kibana  FROM  dwh_kibana_agg
                  WHERE  date_time  >  '$point_of_time'    GROUP  BY  bras_id)  kibana  
                on  bras.bras_id  =  kibana.bras_id
        left  join  
            (SELECT  bras_id,SUM(unknown_opsview)  as  unknown_opsview,  SUM(warn_opsview)  as  warn_opsview,  SUM(ok_opsview)  as  ok_opsview,SUM(crit_opsview)  as  crit_opsview  FROM  dwh_opsview_status
                    WHERE  date_time  >  '$point_of_time'  GROUP  BY  bras_id)  opsview
                    on  bras.bras_id  =  opsview.bras_id    
        left  join  
            (SELECT  bras_id,SUM(cpe_error)  as  cpe_error,  SUM(lostip_error)  as  lostip_error  FROM  dwh_inf_host
                    WHERE  date_time  >  '$point_of_time'  GROUP  BY  bras_id)  inf
                    on  bras.bras_id  =  inf.bras_id    ;            




  val getBrasDetailQuery = s"( SELECT * FROM " +
          s" (SELECT bras.bras_id,bras.time, bras.signin_total_count,bras.logoff_total_count," +
          s" bras.signin_distinct_count, bras.logoff_distinct_count,kibana.crit_kibana , kibana.info_kibana , opsview.unknown_opsview , opsview.warn_opsview , " +
          s" opsview.ok_opsview , opsview.crit_opsview , inf.cpe_error , inf.lostip_error  " +
          s" FROM" +
          s" (SELECT * FROM bras_count WHERE time > '$timestamp_last30mins') bras left join " +
          s" (SELECT bras_id,SUM(total_critical_count) as crit_kibana,SUM(total_info_count) as info_kibana " +
          s" FROM dwh_kibana_agg WHERE date_time > '$point_of_time'  " +
          s" GROUP BY bras_id) kibana on bras.bras_id = kibana.bras_id left join   " +
          s" (SELECT bras_id,SUM(unknown_opsview) as unknown_opsview, SUM(warn_opsview) as warn_opsview, SUM(ok_opsview) as ok_opsview,SUM(crit_opsview) as crit_opsview " +
          s" FROM dwh_opsview_status  WHERE date_time > '$point_of_time' GROUP BY bras_id) opsview  on bras.bras_id = opsview.bras_id  left join  " +
          s" (SELECT bras_id,SUM(cpe_error) as cpe_error, SUM(lostip_error) as lostip_error " +
          s" FROM dwh_inf_host  WHERE date_time > '$point_of_time' GROUP BY bras_id) inf  on bras.bras_id = inf.bras_id ) as m ) n  ;"
                   




                   BDG-MP-01-01,2017-08-07 16:55:43.948,16,49,12.000000000000000000,30.000000000000000000,null,null,null,null,2,0




DELETE FROM dwh_radius_bras_detail
WHERE id IN (SELECT id
              FROM (SELECT id,
                             ROW_NUMBER() OVER (partition BY column1, column2, column3 ORDER BY id) AS rnum
                     FROM tablename) t
              WHERE t.rnum > 1);


?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?


