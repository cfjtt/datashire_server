--  第三方链接squid  add yi.zhou 2014/11/10
drop table if  exists DS_WEBSERVICE_CONNECTION;
create table DS_WEBSERVICE_CONNECTION
(
	id 					int not null,
    is_restful			varchar(1),
	address				varchar(200),
	primary key(id)
);
drop table if  exists DS_HTTP_CONNECTION;
create table DS_HTTP_CONNECTION
(
   id 					int not null,
   host 				varchar(30) null,
   port 				int null,
   primary key(id)
);


drop table if  exists DS_THIRDPARTY_PARAMS;
create table DS_THIRDPARTY_PARAMS
(
	id                  int not null AUTO_INCREMENT,
	squid_id            int,
	source_table_id     int,
	name                varchar(200),
	params_type         int,
	value_type          int,
	val                 varchar(200),
	column_id           int,
	variable_id         int,
	ref_squid_id        int,
	primary key(id)
);
-- alter table DS_THIRDPARTY_PARAMS add constraint Fk_Thrid_party_source_table_id foreign key (SOURCE_TABLE_ID)
-- 	  references DS_SOURCE_TABLE(id);
alter table DS_THIRDPARTY_PARAMS add constraint FK_thrid_party_id foreign key (squid_id)
      references DS_SQUID (id);

--  added by yi.zhou  添加变量 (variable)
drop table if  exists DS_VARIABLE;
create table DS_VARIABLE
(
	id                  int not null AUTO_INCREMENT,
	project_id			int,
	squid_flow_id		int,
	squid_id 			int,
	variable_scope 		int, -- 0:project、1:squidflow、2:squidModelBase
	variable_name       varchar(200),
	variable_type		int,
	variable_length		int,
	variable_precision	int,
	variable_scale		int,
	variable_value		varchar(500),
	add_date 			timestamp not null,
	primary key (id)
);

 -- update by yi.zhou 2015-1-4 支持引用transformation  更新
alter table ds_transformation modify column term_index varchar(100);
alter table ds_transformation modify column replica_count varchar(100);
alter table ds_transformation modify column Length varchar(100);
alter table ds_transformation modify column start_position varchar(100);
alter table ds_transformation modify column power varchar(100);
alter table ds_transformation modify column Modulus varchar(100);

--  added by yi.zhou DESTWS(SquidModelBase)
drop table if exists DS_DEST_WEBSERVICE;
create table DS_DEST_WEBSERVICE
(
	id                   int not null,
	service_name 		 varchar(100),
	wsdl                 varchar(200),
	is_realtime 		 varchar(1),
	callback_url		 varchar(200),
	allowed_services     varchar(200),
	primary key(id)
);

alter table DS_THIRDPARTY_PARAMS drop column variable_id;

create table DS_START_SQUID_FLOW_LOG
(
	id                  int not null AUTO_INCREMENT,
	task_id				varchar(200),
	call_back_url		int,
	create_date			datetime,
	call_back_date		datetime,
	primary key(id)
);

--  added by yi.zhou  添加报表历史版本 (report_version)
drop table if  exists DS_REPORT_VERSION;
create table DS_REPORT_VERSION
(
	id                int not null AUTO_INCREMENT,
	squid_id 		  int not null,
	version 		  int,
	template      	  blob ,
	add_date 		  timestamp not null,
	primary key (id)
);

-- added by yi.zhou column
alter table DS_COLUMN add column sort_level int; 
alter table DS_COLUMN add column sort_type int;
-- added by yi.zhou column
alter table DS_TRANSFORMATION modify column delimiter varchar(2000);
alter table DS_TRANSFORMATION add column split_type int;

-- added by akachi 2015-04-24
insert into DS_SYS_USER (`KEY`,USER_NAME,PASSWORD,FULL_NAME,EMAIL_ADDRESS,IS_ACTIVE) values ('58610ba8-5225-4300-81a2-b73dd5bdc168','superuser','7066a4f427769cc43347aa96b72931a','superuser','www.eurlanda.com','N')