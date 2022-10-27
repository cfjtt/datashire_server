drop table if exists DS_SQUID_LINK;
drop table if exists DS_URL;
drop table if exists DS_WEB_CONNECTION;
drop table if exists DS_WEIBO_CONNECTION;
drop table if exists DS_FILEFOLDER_CONNECTION;
drop table if exists DS_FTP_CONNECTION;
drop table if exists DS_HDFS_CONNECTION;
drop table if exists DS_SQL_CONNECTION;
drop table if exists DS_NO_SQL_CONNECTION;
drop table if exists DS_WEBSERVICE_CONNECTION;
drop table if exists DS_HTTP_CONNECTION;
drop table if exists DS_DATA_SQUID;
drop table if exists DS_REPORT_SQUID;
drop table if exists DS_GIS_MAP_SQUID;
drop table if exists DS_THIRDPARTY_PARAMS;
drop table if exists DS_TRANSFORMATION_LINK;
drop table if exists DS_TRAN_INPUT_DEFINITION;
drop table if exists DS_TRAN_INPUTS;
drop table if exists DS_TRANSFORMATION;
drop table if exists DS_TRANSFORMATION_TYPE;
drop table if exists DS_INDEXES;
drop table if exists DS_REFERENCE_COLUMN;
drop table if exists DS_REFERENCE_COLUMN_GROUP;
drop table if exists DS_JOIN;
drop table if exists DS_JOIN_TYPE;
drop table if exists DS_COLUMN;
drop table if exists DS_DOC_EXTRACT;
drop table if exists DS_SOURCE_COLUMN;
drop table if exists DS_SOURCE_TABLE;
drop table if exists DS_DM_SQUID;
drop table if exists DS_SQUID;
drop table if exists DS_SQUID_TYPE;
drop table if exists DS_SQUID_FLOW;
drop table if exists DS_PROJECT;

create table  DS_COLUMN
(
   id                   int not null AUTO_INCREMENT,
   `key`                  varchar(50),
   relative_order       smallint ,
   squid_id             int not null,
   `name`                 varchar(128) not null ,
   data_type            int not null ,
   `collation`            int,
   nullable             varchar(1),
   length               int,
   `precision`            int,
   scale				int,
   is_groupby           varchar(1),
   aggregation_type     int,
   description          varchar(500),
   isunique             varchar(1),
   ispk                 varchar(1),
   cdc               	int,
   is_business_key 		int,
   primary key (id),
   unique(squid_id, name)
);

create table  DS_DATA_SQUID
(
   id                   int not null,
   is_incremental       varchar(1),
   incremental_expression varchar(4000) ,
   is_persisted         varchar(1)  ,
   table_name           varchar(100) ,
   destination_squid_id int,
   is_indexed           varchar(1),
   top_n                int,
   truncate_existing_data_flag int,
   process_mode         int,
   cdc                  int ,
   exception_handling_flag int  ,
   log_format           int  ,
   post_process         int ,
   xsd_dtd_file         varchar(100) ,
   xsd_dtd_path         varchar(200) ,
   source_table_id      int ,
   union_all_flag       int,
   is_distinct 			int,
   ref_squid_id   		int,
   primary key (id)
);

create table  DS_DOC_EXTRACT
(
   id                   int not null,
   is_persisted     	varchar(1),
   table_name           varchar(100) ,
   destination_squid_id int,
   is_indexed        	varchar(1),
   top_n                int,
   truncate_existing_data_flag int,
   process_mode         int,
   cdc                  int,
   exception_handling_flag int,
   doc_format           int ,
   row_format           int ,
   delimiter            varchar(10),
   field_length         int,
   header_row_no        int,
   first_data_row_no    int,
   row_delimiter        varchar(500),
   row_delimiter_position int,
   skip_rows            int,
   post_process         int,
   source_table_id      int,
   union_all_flag		int,
   primary key (id)
);

create table  DS_FILEFOLDER_CONNECTION
(
   id                   int not null,
   host                 varchar(15) ,
   port                 varchar(10) ,
   user_name            varchar(100) ,
   password             varchar(100) ,
   file_path            varchar(500) ,
   including_subfolders_flag int,
   unionall_flag        int,
   primary key (id)
);


create table  DS_FTP_CONNECTION
(
   id                   int not null,
   host                 varchar(15) ,
   port                 varchar(10) ,
   user_name            varchar(100) ,
   password             varchar(100) ,
   file_path            varchar(500) ,
   including_subfolders_flag int ,
   unionall_flag        int,
   postprocess          int,
   protocol             int,
   encryption           int,
   allowanonymous_flag  int,
   maxconnections       int,
   transfermode_flag    int ,
   primary key (id)
);


create table  DS_HDFS_CONNECTION
(
   id                   int not null,
   host                 varchar(15) ,
   port                 varchar(10) ,
   user_name            varchar(100),
   password             varchar(100) ,
   file_path            varchar(500) ,
   unionall_flag        int,
   primary key (id)
);

create table DS_WEBSERVICE_CONNECTION
(
	id 					int not null,
    is_restful			varchar(1),
	address				varchar(200),
	primary key(id)
);

create table DS_HTTP_CONNECTION
(
   id 					int not null,
   host 				varchar(30) not null,
   port 				int not null,
   primary key(id)
);

create table DS_THIRDPARTY_PARAMS
(
	id                  int not null AUTO_INCREMENT,
	squid_id            int,
	name                varchar(200),
	params_type         int,
	value_type          int,
	val                 varchar(200),
	column_id           int,
	-- variable_id         int,
	ref_squid_id        int,
	primary key(id)
);

create table  DS_INDEXES
(
   id                   int not null AUTO_INCREMENT,
   squid_id             int not null ,
   index_name           varchar(100) not null ,
   index_type           int,
   column_id10          int,
   column_id9           int,
   column_id8           int,
   column_id7           int,
   column_id6           int,
   column_id5           int,
   column_id4           int,
   column_id3           int,
   column_id2           int,
   column_id1           int,
   primary key (id),
   unique (index_name, squid_id)
);


create table  DS_JOIN
(
   id                   int not null AUTO_INCREMENT,
   `key`                  varchar(50) not null,
   target_squid_id      int not null,
   joined_squid_id      int not null,
   prior_join_id        int not null,
   join_type_id         int not null,
   join_condition       varchar(500),
   primary key (id)
);


create table  DS_JOIN_TYPE
(
   id                   int not null,
   code                 varchar(50),
   description          varchar(500),
   primary key (id)
);


create table  DS_PROJECT
(
   id                   int not null AUTO_INCREMENT,
   `key`                  varchar(50),
   `name`                 varchar(50),
   repository_id		int not null,
   description          varchar(200),
   creation_date        datetime,
   modification_date    datetime,
   creator              varchar(50),
   primary key (id),
   unique  (name, repository_id)
);


create table  DS_REFERENCE_COLUMN
(
   column_id            int not null ,
   relative_order       smallint ,
   reference_squid_id   int not null ,
   `name`                 varchar(100) ,
   data_type            int ,
   `collation`            int,
   nullable             varchar(1),
   length               int,
   `precision`            int,
   scale				int,
   description          varchar(500),
   isunique             varchar(1),
   ispk                 varchar(1),
   cdc               	int,
   is_business_key 		int,
   host_squid_id        int,
   is_referenced        varchar(1),
   group_id             int
);



create table  DS_REFERENCE_COLUMN_GROUP
(
   id                   int not null AUTO_INCREMENT,
   `key`                  varchar(50) not null,
   reference_squid_id   int not null ,
   relative_order       smallint,
   primary key (id)
);


create table  DS_REPORT_SQUID
(
   id                   int not null,
   report_name          varchar(50) ,
   report_template      BLOB ,
   is_support_history   boolean ,
   max_history_count    int,
   is_send_email        boolean ,
   email_receivers      varchar(500) ,
   email_title          varchar(200) ,
   email_report_format  varchar(10) ,
   creation_date        datetime ,
   is_real_time         boolean,
   folder_id            int,
   is_packed            boolean,
   is_compressed        boolean,
   is_encrypted         boolean,
   password             varchar(10),
   primary key (id)
);

create table  DS_GIS_MAP_SQUID
(
   id                   int not null,
   map_name          varchar(50) ,
   map_template      BLOB ,
   is_support_history   boolean ,
   max_history_count    int,
   is_send_email        boolean ,
   email_receivers      varchar(500) ,
   email_title          varchar(200) ,
   email_report_format  varchar(10) ,
   creation_date        datetime ,
   is_real_time         boolean,
   folder_id            int,
   is_packed            boolean,
   is_compressed        boolean,
   is_encrypted         boolean,
   password             varchar(10),
   primary key (id)
);


create table  DS_SOURCE_COLUMN
(
   id                   int not null AUTO_INCREMENT,
   source_table_id      int not null,
   `name`                 varchar(128) not null,
   data_type            int not null,
   nullable             varchar(1),
   length               int,
   `precision`            int,
   scale				int,
   relative_order       smallint,
   isunique             varchar(1),
   ispk                 varchar(1),
   `collation`            int,
   primary key (id),
   unique  (source_table_id, name)
);


create table  DS_SOURCE_TABLE
(
   id                   int not null AUTO_INCREMENT,
   table_name           varchar(255) not null ,
   source_squid_id      int not null ,
   is_extracted         varchar(1),
   relative_order 		smallint,
   url 					varchar(300),
   url_params           varchar(300),
   header_params        varchar(3000),	
   method               int,
   primary key (id),
   unique  (table_name, source_squid_id)
);


create table  DS_SQL_CONNECTION
(
   id                   int not null,
   db_type_id   int not null ,
   host                 varchar(100) ,
   port                 varchar(10) ,
   user_name            varchar(100) ,
   password             varchar(100) ,
   database_name        varchar(100) ,
   primary key (id)
);

create table  DS_NO_SQL_CONNECTION
(
   id                   int not null,
   db_type_id   		int not null ,
   host                 varchar(100) ,
   port                 varchar(10) ,
   user_name            varchar(100) ,
   password             varchar(100) ,
   database_name        varchar(100) ,
   primary key (id)
);

create table  DS_SQUID
(
   id                   int not null AUTO_INCREMENT,
   `key`                  varchar(50) not null ,
   squid_flow_id        int not null,
   `name`                 varchar(200) not null ,
   description          varchar(500),
   squid_type_id        int not null ,
   location_x           int not null,
   location_y           int not null,
   squid_height         int not null ,
   squid_width          int not null,
   table_name           varchar(500),
   is_show_all          varchar(1) ,
   source_is_show_all   varchar(1) ,
   column_group_x       int ,
   column_group_y       int ,
   reference_group_x    int,
   reference_group_y    int,
   filter               varchar(3000),
   encoding             int ,
   design_status 		int,
   max_travel_depth     int,
   primary key (id),
   unique  (name, squid_flow_id)
);


create table  DS_SQUID_FLOW
(
   id                   int not null AUTO_INCREMENT,
   `key`                  varchar(50) not null,
   `name`                 varchar(50) ,
   creation_date        datetime,
   modification_date    datetime ,
   creator              varchar(50) ,
   project_id           int not null,
   description          varchar(200),
   compilation_status	int,
   primary key (id),
   unique (name, project_id)
);


create table  DS_SQUID_LINK
(
   id                   int not null AUTO_INCREMENT,
   `key`                  varchar(50),
   squid_flow_id        int ,
   from_squid_id        int not null,
   to_squid_id          int not null ,
   end_x                int,
   arrows_style         int,
   end_y                int,
   endmiddle_x          int,
   endmiddle_y          int,
   start_x              int,
   start_y              int,
   startmiddle_x        int,
   startmiddle_y        int,
   line_color           varchar(50),
   line_type            int,
   link_type 			int,
   primary key (id),
   unique  (from_squid_id, to_squid_id)
);


create table  DS_SQUID_TYPE
(
   id                   int not null,
   code                 varchar(50),
   description          varchar(250),
   primary key (id),
   unique (code)
);


create table  DS_TRANSFORMATION
(
   id                   int not null AUTO_INCREMENT,
   `key`                  varchar(50) not null,
   squid_id             int not null,
   transformation_type_id int not null ,
   location_x           int ,
   location_y           int,
   column_id            int ,
   description          varchar(500),
   `name`                 varchar(100),
   output_data_type     int,
   constant_value       varchar(4000) ,
   reg_expression       varchar(500) ,
   term_index           int ,
   output_number        int ,
   delimiter            varchar(50) ,
   algorithm            int ,
   tran_condition       varchar(500) ,
   length               int,
   start_position       int,
   difference_type      int ,
   power                int,
   modulus 				int,
   replica_count 		int,
   is_using_dictionary	int,
   dictionary_squid_id 	int,
   bucket_count 		int,
   model_squid_id		int,
   model_version 		int,
   operator 			int,
   date_format			int, -- update by yi.zhou 2014/12/26
   inc_unit 			int, -- update by yi.zhou 2015/04/01
   primary key (id)
);

create table  DS_TRANSFORMATION_LINK
(
   id                   int not null AUTO_INCREMENT,
   `key`                  varchar(50) not null,
   from_transformation_id int not null,
   to_transformation_id int not null,
   in_order             int,
   primary key (id)
);

create table  DS_TRANSFORMATION_TYPE
(
   id                   int not null,
   code                 varchar(50) not null,
   output_data_type 	int,
   description          varchar(500),
   primary key (id),
   unique  (code)
);


create table  DS_TRAN_INPUTS
(
   id                   int not null AUTO_INCREMENT,
   transformation_id    int not null ,
   relative_order       int not null ,
   source_transform_id  int,
   source_tran_output_index int,
   in_condition      varchar(1000),
   primary key (id)
);

create table  DS_TRAN_INPUT_DEFINITION
(
	code                   varchar(50) not null,
	input_order       int not null ,
	input_data_type       int not null ,
	description varchar(500)
);

create table  DS_URL
(
   id                   int not null AUTO_INCREMENT,
   squid_id             int,
   url                  varchar(500),
   user_name            varchar(50),
   password             varchar(50),
   max_fetch_depth      int,
   filter               varchar(200),
   domain               varchar(50),
   domain_limit_flag    int,
   primary key (id)
);

create table  DS_WEB_CONNECTION
(
   id                   int not null,
   max_threads          int,
   max_fetch_depth      int,
   domain_limit_flag    int,
   start_data_date 		date,
   end_data_date 		date,
   primary key (id)
);

create table  DS_WEIBO_CONNECTION
(
   id                   int not null,
   user_name            varchar(100),
   password             varchar(100) ,
   start_data_date 		date,
   end_data_date 		date,
   service_provider     int,
   primary key (id)
);

create table  DS_DM_SQUID
(
   id                   int not null,
   training_percentage  float,
   versioning           int ,
   min_batch_fraction   double,
   iteration_number     int,
   step_size            double,
   smoothing_parameter  double,
   regularization       double,
   k                    int,
   parallel_runs        int,
   initialization_mode  int,
   implicit_preferences int,
   case_sensitive		int,
   min_value 			float,
   max_value 			float,
   bucket_count 		int,
   seed					int,
   algorithm  			int,
   max_depth  			int,
   impurity 			int, 
   max_bins 			int,
   categorical_squid    int,
   primary key (id)
);

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

--  added by yi.zhou DESTWS(SquidModelBase)
drop table if exists DS_DEST_WEBSERVICE;
create table DS_DEST_WEBSERVICE
(
	id                   int not null,
	service_name 		 varchar(100),
	wsdl                 varchar(200),
	is_realtime 			 varchar(1),
	callback_url		 varchar(200),
	allowed_services      varchar(200),
	primary key(id)
);

--  added by yi.zhou DS_ANNOTATION_SQUID(SquidModelBase)
drop table if exists DS_ANNOTATION_SQUID;
create table DS_ANNOTATION_SQUID
(
	id                   			int not null,
	content 						varchar(4000),
	content_font_size 				int,
	content_font_color				varchar(100),
	content_font_family				varchar(100),
	horizontal_content_alignment 	int,
	vertical_content_alignment		int,
	is_content_bold      			varchar(1),
	is_content_italic    			varchar(1),
	is_content_underline 			varchar(1),
	primary key(id)
);


alter table DS_COLUMN add constraint FK_squid_id foreign key (squid_id)
      references DS_SQUID (id) ;

alter table DS_DATA_SQUID add constraint FK_extract_squid_id foreign key (id)
      references DS_SQUID (id) ;

alter table DS_DOC_EXTRACT add constraint FK_Reference_29 foreign key (id)
      references DS_SQUID (id);

alter table DS_FILEFOLDER_CONNECTION add constraint FK_Reference_24 foreign key (id)
      references DS_SQUID (id);

alter table DS_FTP_CONNECTION add constraint FK_Reference_26 foreign key (id)
      references DS_SQUID (id);

alter table DS_HDFS_CONNECTION add constraint FK_Reference_25 foreign key (id)
      references DS_SQUID (id);

alter table DS_INDEXES add constraint FK_Reference_30 foreign key (squid_id)
      references DS_SQUID (id);

alter table DS_JOIN add constraint FK_join_squid_id foreign key (joined_squid_id)
      references DS_SQUID (id) ;

alter table DS_JOIN add constraint FK_join_type foreign key (join_type_id)
      references DS_JOIN_TYPE (id) ;

alter table DS_JOIN add constraint FK_target_id foreign key (target_squid_id)
      references DS_SQUID (id) ;


alter table DS_REFERENCE_COLUMN add constraint FK_group_id foreign key (group_id)
      references DS_REFERENCE_COLUMN_GROUP (id) ;

alter table DS_REFERENCE_COLUMN add constraint FK_host_squid_id foreign key (host_squid_id)
      references DS_SQUID (id) ;

alter table DS_REFERENCE_COLUMN_GROUP add constraint FK_ref_column_squid_id foreign key (reference_squid_id)
      references DS_SQUID (id) ;

alter table DS_REPORT_SQUID add constraint FK_Reference_23 foreign key (id)
      references DS_SQUID (id);
      
alter table DS_GIS_MAP_SQUID add constraint FK_squid_id_temp foreign key (id)
      references DS_SQUID (id);

alter table DS_SOURCE_COLUMN add constraint FK_source_table_id foreign key (source_table_id)
      references DS_SOURCE_TABLE (id) ON DELETE CASCADE;

alter table DS_SOURCE_TABLE add constraint FK_source_squid_id foreign key (source_squid_id)
      references DS_SQUID (id) ;

alter table DS_SQL_CONNECTION add constraint FK_db_squid_id foreign key (id)
      references DS_SQUID (id) ;

alter table DS_SQUID add constraint FK_squid_flow_id foreign key (squid_flow_id)
      references DS_SQUID_FLOW (id) ;

alter table DS_SQUID add constraint FK_squid_type_id foreign key (squid_type_id)
      references DS_SQUID_TYPE (id) ;

alter table DS_SQUID_FLOW add constraint FK_project_id foreign key (project_id)
      references DS_PROJECT (id) ;

alter table DS_SQUID_LINK add constraint FK_link_squid_id foreign key (from_squid_id)
      references DS_SQUID (id) ;

alter table DS_TRANSFORMATION add constraint FK_tran_type foreign key (transformation_type_id)
      references DS_TRANSFORMATION_TYPE (id) ;

alter table DS_TRANSFORMATION add constraint FK_trans_squid_id foreign key (squid_id)
      references DS_SQUID (id) ;

alter table DS_TRANSFORMATION_LINK add constraint FK_tran_from foreign key (from_transformation_id)
      references DS_TRANSFORMATION (id) ;

alter table DS_TRANSFORMATION_LINK add constraint FK_tran_to foreign key (to_transformation_id)
      references DS_TRANSFORMATION (id) ;

alter table DS_TRAN_INPUTS add constraint FK_Reference_31 foreign key (transformation_id)
      references DS_TRANSFORMATION (id);

alter table DS_TRAN_INPUTS add constraint FK_Reference_32 foreign key (source_transform_id)
      references DS_TRANSFORMATION (id);
      
alter table DS_TRAN_INPUT_DEFINITION add constraint FK_Reference_34 foreign key (code)
      references DS_TRANSFORMATION_TYPE (code);

alter table DS_URL add constraint FK_Reference_33 foreign key (squid_id)
      references DS_WEB_CONNECTION (id) ON DELETE CASCADE;

alter table DS_WEB_CONNECTION add constraint FK_Reference_28 foreign key (id)
      references DS_SQUID (id);

alter table DS_WEIBO_CONNECTION add constraint FK_Reference_27 foreign key (id)
      references DS_SQUID (id);

alter table DS_DM_SQUID add constraint FK_DM_squid_id foreign key (id)
      references DS_SQUID (id);
      
alter table DS_THIRDPARTY_PARAMS add constraint FK_thrid_party_id foreign key (squid_id)
      references DS_SQUID (id);
      

-- 添加索引
CREATE INDEX IDX_REFCOLUMN ON DS_REFERENCE_COLUMN(COLUMN_ID);

CREATE INDEX IDX_TRAN_INPUT_DEFINITION_CODE ON DS_TRAN_INPUT_DEFINITION(CODE);

CREATE INDEX IDX_REFCOLUMN_HOST_ID ON DS_REFERENCE_COLUMN(HOST_SQUID_ID);
CREATE INDEX IDX_REFCOLUMN_GROUP_ID ON DS_REFERENCE_COLUMN(GROUP_ID);

CREATE INDEX IDX_COLUMN_SQUID_ID ON DS_COLUMN(SQUID_ID);
CREATE INDEX IDX_DATA_SQUID_TABLE_NAME ON DS_DATA_SQUID(TABLE_NAME);
CREATE INDEX IDX_DATA_SQUID_SOURCE_TABLE_ID ON DS_DATA_SQUID(SOURCE_TABLE_ID);

CREATE INDEX IDX_DOC_EXTRACT_TABLE_NAME ON  DS_DOC_EXTRACT(TABLE_NAME);
CREATE INDEX IDX_DOC_EXTRACT_SOURCE_TABLE_ID ON  DS_DOC_EXTRACT(SOURCE_TABLE_ID);

CREATE INDEX IDX_INDEXES_SQUID_ID ON DS_INDEXES(SQUID_ID);

CREATE INDEX IDX_JOIN_TARGET_ID ON DS_JOIN(TARGET_SQUID_ID);
CREATE INDEX IDX_JOIN_JOINED_ID ON DS_JOIN(JOINED_SQUID_ID);
CREATE INDEX IDX_JOIN_TYPE_ID ON DS_JOIN(JOIN_TYPE_ID);
 
CREATE INDEX IDX_GROUP_SQUID_ID ON DS_REFERENCE_COLUMN_GROUP(REFERENCE_SQUID_ID);

CREATE INDEX IDX_SROUCE_COLUMN_TABLE_ID ON DS_SOURCE_COLUMN(SOURCE_TABLE_ID);
CREATE INDEX IDX_SROUCE_COLUMN_NAME ON DS_SOURCE_COLUMN(NAME);

CREATE INDEX IDX_SROUCE_TABLE_NAME ON DS_SOURCE_TABLE(TABLE_NAME);
CREATE INDEX IDX_SROUCE_TABLE_SQUID_ID ON DS_SOURCE_TABLE(SOURCE_SQUID_ID);

CREATE INDEX IDX_SQUID_FLOW_ID ON DS_SQUID(SQUID_FLOW_ID);
CREATE INDEX IDX_SQUID_NAME ON DS_SQUID(NAME);
CREATE INDEX IDX_SQUID_TYPE_ID ON DS_SQUID(SQUID_TYPE_ID);

CREATE INDEX IDX_SQUIDFLOW_PROJECT_ID ON DS_SQUID_FLOW(PROJECT_ID);

CREATE INDEX IDX_SQUID_LINK_FROM_ID ON DS_SQUID_LINK(FROM_SQUID_ID);
CREATE INDEX IDX_SQUID_LINK_TO_ID ON DS_SQUID_LINK(TO_SQUID_ID);

CREATE INDEX IDX_TRANS_ID ON DS_TRAN_INPUTS(TRANSFORMATION_ID);
CREATE INDEX IDX_TRANS_SOURCE_ID ON DS_TRAN_INPUTS(SOURCE_TRANSFORM_ID);

CREATE INDEX IDX_TRANSFORMATION_SQUID_ID ON DS_TRANSFORMATION(SQUID_ID);
CREATE INDEX IDX_TRANSFORMATION_TYPE_ID ON DS_TRANSFORMATION(TRANSFORMATION_TYPE_ID);

CREATE INDEX IDX_TRANSFORMATION_LINK_FROM_ID ON DS_TRANSFORMATION_LINK(FROM_TRANSFORMATION_ID);
CREATE INDEX IDX_TRANSFORMATION_LINK_TO_ID ON DS_TRANSFORMATION_LINK(TO_TRANSFORMATION_ID);

CREATE INDEX IDX_URL_SQUID_ID ON DS_URL(SQUID_ID);


INSERT INTO DS_JOIN_TYPE (ID,CODE,DESCRIPTION)VALUES(0,0,'BASETABLE');
INSERT INTO DS_JOIN_TYPE (ID,CODE,DESCRIPTION)VALUES(1,1,'INNERJOIN');
INSERT INTO  DS_JOIN_TYPE (ID,CODE,DESCRIPTION)VALUES(2,2,'LEFTOUTERJOIN');
INSERT INTO  DS_JOIN_TYPE  (ID,CODE,DESCRIPTION)VALUES(3,3,'RIGHTOUTERJOIN');
INSERT INTO  DS_JOIN_TYPE  (ID,CODE,DESCRIPTION)VALUES(4,4,'FULLJOIN');
INSERT INTO  DS_JOIN_TYPE  (ID,CODE,DESCRIPTION)VALUES(5,5,'CROSSJOIN');
INSERT INTO  DS_JOIN_TYPE  (ID,CODE,DESCRIPTION)VALUES(6,6,'UNOIN');
INSERT INTO  DS_JOIN_TYPE  (ID,CODE,DESCRIPTION)VALUES(7,7,'UNOINALL');

INSERT INTO  DS_SQUID_TYPE (ID,CODE,DESCRIPTION)VALUES(-1,-1,'UNKNOWN');
INSERT INTO  DS_SQUID_TYPE (ID,CODE,DESCRIPTION)VALUES(0,0,'DBSOURCE');
INSERT INTO  DS_SQUID_TYPE (ID,CODE,DESCRIPTION)VALUES(1,1,'DBDESTINATION');
INSERT INTO  DS_SQUID_TYPE (ID,CODE,DESCRIPTION)VALUES(2,2,'EXTRACT');
INSERT INTO  DS_SQUID_TYPE (ID,CODE,DESCRIPTION)VALUES(3,3,'STAGE');
INSERT INTO  DS_SQUID_TYPE (ID,CODE,DESCRIPTION)VALUES(4,4,'DIMENSION');
INSERT INTO  DS_SQUID_TYPE (ID,CODE,DESCRIPTION)VALUES(5,5,'FACT');
INSERT INTO  DS_SQUID_TYPE (ID,CODE,DESCRIPTION)VALUES(6, 6, 'REPORT');
INSERT INTO  DS_SQUID_TYPE (ID,CODE,DESCRIPTION)VALUES(7, 7, 'DOC_EXTRACT');
INSERT INTO  DS_SQUID_TYPE (ID,CODE,DESCRIPTION)VALUES(8, 8, 'XML_EXTRACT');
INSERT INTO  DS_SQUID_TYPE (ID,CODE,DESCRIPTION)VALUES(9, 9, 'WEBLOGEXTRACT');
INSERT INTO  DS_SQUID_TYPE (ID,CODE,DESCRIPTION)VALUES(10,10,'WEBEXTRACT');
INSERT INTO  DS_SQUID_TYPE (ID,CODE,DESCRIPTION)VALUES(11,11,'WEIBOEXTRACT');
INSERT INTO  DS_SQUID_TYPE (ID,CODE,DESCRIPTION)VALUES(12,12,'FILEFOLDER');
INSERT INTO  DS_SQUID_TYPE (ID,CODE,DESCRIPTION)VALUES(13,13,'FTP');
INSERT INTO  DS_SQUID_TYPE (ID,CODE,DESCRIPTION)VALUES(14,14,'HDFS');
INSERT INTO  DS_SQUID_TYPE (ID,CODE,DESCRIPTION)VALUES(15,15,'WEB');
-- INSERT INTO  DS_SQUID_TYPE (ID,CODE,DESCRIPTION)VALUES(16,16_9,'WEBLOG');
INSERT INTO  DS_SQUID_TYPE (ID,CODE,DESCRIPTION)VALUES(17,17,'WEIBO');
-- INSERT INTO  DS_SQUID_TYPE (ID,CODE,DESCRIPTION)VALUES(18,18,'DOC');
-- INSERT INTO  DS_SQUID_TYPE (ID,CODE,DESCRIPTION)VALUES(19,19,'XML');
INSERT INTO  DS_SQUID_TYPE (ID,CODE,DESCRIPTION)VALUES(20,20,'EXCEPTION');
INSERT INTO  DS_SQUID_TYPE (ID,CODE,DESCRIPTION)VALUES(21,21,'LOGREG');
INSERT INTO  DS_SQUID_TYPE (ID,CODE,DESCRIPTION)VALUES(22,22,'NAIVEBAYES');
INSERT INTO  DS_SQUID_TYPE (ID,CODE,DESCRIPTION)VALUES(23,23,'SVM');
INSERT INTO  DS_SQUID_TYPE (ID,CODE,DESCRIPTION)VALUES(24,24,'KMEANS');
INSERT INTO  DS_SQUID_TYPE (ID,CODE,DESCRIPTION)VALUES(25,25,'ALS');
INSERT INTO  DS_SQUID_TYPE (ID,CODE,DESCRIPTION)VALUES(26,26,'LINEREG');
INSERT INTO  DS_SQUID_TYPE (ID,CODE,DESCRIPTION)VALUES(27,27,'RIDGEREG');
INSERT INTO  DS_SQUID_TYPE (ID,CODE,DESCRIPTION)VALUES(28,28,'QUANTIFY');
INSERT INTO  DS_SQUID_TYPE (ID,CODE,DESCRIPTION)VALUES(29,29,'DISCRETIZE');
-- add yi.zhou 
INSERT INTO  DS_SQUID_TYPE (ID,CODE,DESCRIPTION)VALUES(30,30,'DECISIONTREE');
INSERT INTO  DS_SQUID_TYPE (ID,CODE,DESCRIPTION)VALUES(31,31,'GISMAP');
INSERT INTO  DS_SQUID_TYPE (ID,CODE,DESCRIPTION)VALUES(32,32,'HTTP');
INSERT INTO  DS_SQUID_TYPE (ID,CODE,DESCRIPTION)VALUES(33,33,'WEBSERVICE');
INSERT INTO  DS_SQUID_TYPE (ID,CODE,DESCRIPTION)VALUES(34,34,'HTTPEXTRACT');
INSERT INTO  DS_SQUID_TYPE (ID,CODE,DESCRIPTION)VALUES(35,35,'WEBSERVICEEXTRACT');
INSERT INTO  DS_SQUID_TYPE (ID,CODE,DESCRIPTION)VALUES(36,36,'DESTWS');
-- add yi.zhou 2015-3-30
INSERT INTO  DS_SQUID_TYPE (ID,CODE,DESCRIPTION)VALUES(37,37,'ANNOTATION');
-- add yi.zhou 2015-05-18
INSERT INTO  DS_SQUID_TYPE (ID,CODE,DESCRIPTION)VALUES(38,38,'MONGODB');
INSERT INTO  DS_SQUID_TYPE (ID,CODE,DESCRIPTION)VALUES(39,39,'MONGODBEXTRACT');


INSERT INTO  DS_TRANSFORMATION_TYPE  (ID,CODE,OUTPUT_DATA_TYPE,DESCRIPTION)VALUES(-1,'UNKNOWN',21,'');
INSERT INTO  DS_TRANSFORMATION_TYPE  (ID,CODE,OUTPUT_DATA_TYPE,DESCRIPTION)VALUES(0,'VIRTUAL',21,'和对应的column相同');
INSERT INTO  DS_TRANSFORMATION_TYPE  (ID,CODE,OUTPUT_DATA_TYPE,DESCRIPTION)VALUES(1,'UPPER',9,'把一个字符串转换成大写串');
INSERT INTO  DS_TRANSFORMATION_TYPE  (ID,CODE,OUTPUT_DATA_TYPE,DESCRIPTION)VALUES(2,'CONCATENATE',9,'把多个字符串按次序连接成一个字符串');
INSERT INTO  DS_TRANSFORMATION_TYPE  (ID,CODE,OUTPUT_DATA_TYPE,DESCRIPTION)VALUES(3,'LOWER',9,'把一个字符串转换成小写串');
INSERT INTO  DS_TRANSFORMATION_TYPE  (ID,CODE,OUTPUT_DATA_TYPE,DESCRIPTION)VALUES(4,'CONSTANT',21,'定义一个常量，可以是不同类型。');
INSERT INTO  DS_TRANSFORMATION_TYPE  (ID,CODE,OUTPUT_DATA_TYPE,DESCRIPTION)VALUES(5,'CHOICE',21,'根据各个输入对象的逻辑条件取值，输出一个逻辑条件取值为True的输入对象。');
INSERT INTO  DS_TRANSFORMATION_TYPE  (ID,CODE,OUTPUT_DATA_TYPE,DESCRIPTION)VALUES(7,'TERMEXTRACT',9,'从输入字符串的左边开始搜索，提取出和RegExpression匹配的第TermIndex个字串。如果没有，则返回空。');
INSERT INTO  DS_TRANSFORMATION_TYPE  (ID,CODE,OUTPUT_DATA_TYPE,DESCRIPTION)VALUES(8,'SPLIT',9,'对string类型的数据进行变换，把一个string拆分为几个字串，同时输出每一个子串。');
INSERT INTO  DS_TRANSFORMATION_TYPE  (ID,CODE,OUTPUT_DATA_TYPE,DESCRIPTION)VALUES(9,'ASCII',2,'返回一个字符的ASCII数值');
INSERT INTO  DS_TRANSFORMATION_TYPE  (ID,CODE,OUTPUT_DATA_TYPE,DESCRIPTION)VALUES(10,'UNICODE',2,'返回一个字符的Unicode数值');
INSERT INTO  DS_TRANSFORMATION_TYPE  (ID,CODE,OUTPUT_DATA_TYPE,DESCRIPTION)VALUES(11,'SIMILARITY',6,'计算两个字符串的相似度。');
INSERT INTO  DS_TRANSFORMATION_TYPE  (ID,CODE,OUTPUT_DATA_TYPE,DESCRIPTION)VALUES(12,'CHAR',8,'把一个ASCII整型数值转换为一个对应的字符。');
INSERT INTO  DS_TRANSFORMATION_TYPE  (ID,CODE,OUTPUT_DATA_TYPE,DESCRIPTION)VALUES(13,'PATTERNINDEX',2,'搜索一个字符串，返回另一个字符串（可以是正则表达式）在其中第1次出现的位置。如果没有出现返回-1.');
INSERT INTO  DS_TRANSFORMATION_TYPE  (ID,CODE,OUTPUT_DATA_TYPE,DESCRIPTION)VALUES(14,'REPLICATE',9,'重复一个字符串指定次数');
INSERT INTO  DS_TRANSFORMATION_TYPE  (ID,CODE,OUTPUT_DATA_TYPE,DESCRIPTION)VALUES(15,'NUMERICTOSTRING',9,'把一个数值数据转换为字符串。');
INSERT INTO  DS_TRANSFORMATION_TYPE  (ID,CODE,OUTPUT_DATA_TYPE,DESCRIPTION)VALUES(16,'STRINGTONUMERIC',6,'把一个字符串转换为数值数据。');
INSERT INTO  DS_TRANSFORMATION_TYPE  (ID,CODE,OUTPUT_DATA_TYPE,DESCRIPTION)VALUES(17,'REPLACE',9,'在一个字符串中寻找一个子串，并全部替换为另外一个字串。');
INSERT INTO  DS_TRANSFORMATION_TYPE  (ID,CODE,OUTPUT_DATA_TYPE,DESCRIPTION)VALUES(18,'LEFT',9,'从一个字符串的左边中截取特定长度的字串。');
INSERT INTO  DS_TRANSFORMATION_TYPE  (ID,CODE,OUTPUT_DATA_TYPE,DESCRIPTION)VALUES(19,'RIGHT',9,'从一个字符串的右边中截取特定长度的字串。');
INSERT INTO  DS_TRANSFORMATION_TYPE  (ID,CODE,OUTPUT_DATA_TYPE,DESCRIPTION)VALUES(21,'SUBSTRING',9,'从一个字符串中截取特定长度的子串。');
INSERT INTO  DS_TRANSFORMATION_TYPE  (ID,CODE,OUTPUT_DATA_TYPE,DESCRIPTION)VALUES(22,'LENGTH',2,'返回一个字符串的长度');
INSERT INTO  DS_TRANSFORMATION_TYPE  (ID,CODE,OUTPUT_DATA_TYPE,DESCRIPTION)VALUES(23,'REVERSE',9,'把一个字符串倒序后返回');
INSERT INTO  DS_TRANSFORMATION_TYPE  (ID,CODE,OUTPUT_DATA_TYPE,DESCRIPTION)VALUES(24,'LEFTTRIM',9,'去掉一个字符串左边的空格。');
INSERT INTO  DS_TRANSFORMATION_TYPE  (ID,CODE,OUTPUT_DATA_TYPE,DESCRIPTION)VALUES(25,'RIGHTTRIM',9,'去掉一个字符串右边的空格。');
INSERT INTO  DS_TRANSFORMATION_TYPE  (ID,CODE,OUTPUT_DATA_TYPE,DESCRIPTION)VALUES(26,'TRIM',9,'去掉一个字符串左边和右边的空格。');
INSERT INTO  DS_TRANSFORMATION_TYPE  (ID,CODE,OUTPUT_DATA_TYPE,DESCRIPTION)VALUES(27,'ROWNUMBER',2,'返回一行的记录号。');
INSERT INTO  DS_TRANSFORMATION_TYPE  (ID,CODE,OUTPUT_DATA_TYPE,DESCRIPTION)VALUES(28,'SYSTEMDATETIME',13,'返回一个YYYY-MM-DD hh:mm:ss[.nnn]类型的系统当前日期和时间');
INSERT INTO  DS_TRANSFORMATION_TYPE  (ID,CODE,OUTPUT_DATA_TYPE,DESCRIPTION)VALUES(29,'STRINGTODATETIME',13,'把一个YYYY-MM-DD hh:mm:ss[.nnn]字符串转换为日期时间。');
INSERT INTO  DS_TRANSFORMATION_TYPE  (ID,CODE,OUTPUT_DATA_TYPE,DESCRIPTION)VALUES(30,'DATETIMETOSTRING',9,'把一个YYYY-MM-DD hh:mm:ss[.nnn]日期时间转换为字符串。');
INSERT INTO  DS_TRANSFORMATION_TYPE  (ID,CODE,OUTPUT_DATA_TYPE,DESCRIPTION)VALUES(31,'YEAR',2,'把一个YYYY-MM-DD hh:mm:ss[.nnn]日期时间的年部分转换为整型数值。');
INSERT INTO  DS_TRANSFORMATION_TYPE  (ID,CODE,OUTPUT_DATA_TYPE,DESCRIPTION)VALUES(32,'MONTH',2,'把一个YYYY-MM-DD hh:mm:ss[.nnn]日期时间的月部分转换为整型数值。');
INSERT INTO  DS_TRANSFORMATION_TYPE  (ID,CODE,OUTPUT_DATA_TYPE,DESCRIPTION)VALUES(33,'DAY',2,'把一个YYYY-MM-DD hh:mm:ss[.nnn]日期时间的天部分转换为整型数值。');
INSERT INTO  DS_TRANSFORMATION_TYPE  (ID,CODE,OUTPUT_DATA_TYPE,DESCRIPTION)VALUES(34,'DATEDIFF',2,'计算两个日期之间的差。');
INSERT INTO  DS_TRANSFORMATION_TYPE  (ID,CODE,OUTPUT_DATA_TYPE,DESCRIPTION)VALUES(35,'FORMATDATE',13,'把一个日期值格式化为DataShire系统内部统一格式');
INSERT INTO  DS_TRANSFORMATION_TYPE  (ID,CODE,OUTPUT_DATA_TYPE,DESCRIPTION)VALUES(36,'ABS',6,'求绝对值');
INSERT INTO  DS_TRANSFORMATION_TYPE  (ID,CODE,OUTPUT_DATA_TYPE,DESCRIPTION)VALUES(37,'RANDOM',6,'返回0到1之间的一个随机数');
INSERT INTO  DS_TRANSFORMATION_TYPE  (ID,CODE,OUTPUT_DATA_TYPE,DESCRIPTION)VALUES(38,'ACOS',6,'数学函数，返回其余弦是所指定的 float 表达式的角（弧度）；也称为反余弦');
INSERT INTO  DS_TRANSFORMATION_TYPE  (ID,CODE,OUTPUT_DATA_TYPE,DESCRIPTION)VALUES(39,'EXP',6,'返回指定的 float 表达式的指数值');
INSERT INTO  DS_TRANSFORMATION_TYPE  (ID,CODE,OUTPUT_DATA_TYPE,DESCRIPTION)VALUES(40,'ROUND',6,'返回一个舍入到指定的长度的数值。');
INSERT INTO  DS_TRANSFORMATION_TYPE  (ID,CODE,OUTPUT_DATA_TYPE,DESCRIPTION)VALUES(41,'ASIN',6,'反正弦。');
INSERT INTO  DS_TRANSFORMATION_TYPE  (ID,CODE,OUTPUT_DATA_TYPE,DESCRIPTION)VALUES(42,'FLOOR',2,'返回小于或等于指定数值表达式的最大整数');
INSERT INTO  DS_TRANSFORMATION_TYPE  (ID,CODE,OUTPUT_DATA_TYPE,DESCRIPTION)VALUES(43,'SIGN',2,'返回指定表达式的正号 (+1)、零 (0) 或负号 (-1)');
INSERT INTO  DS_TRANSFORMATION_TYPE  (ID,CODE,OUTPUT_DATA_TYPE,DESCRIPTION)VALUES(44,'ATAN',6,'反正切函数');
INSERT INTO  DS_TRANSFORMATION_TYPE  (ID,CODE,OUTPUT_DATA_TYPE,DESCRIPTION)VALUES(45,'LOG',6,'自然对数');
INSERT INTO  DS_TRANSFORMATION_TYPE  (ID,CODE,OUTPUT_DATA_TYPE,DESCRIPTION)VALUES(46,'SIN',6,'返回指定角度（以弧度为单位）的三角正弦值');
INSERT INTO  DS_TRANSFORMATION_TYPE  (ID,CODE,OUTPUT_DATA_TYPE,DESCRIPTION)VALUES(47,'LOG10',6,'返回指定 float 表达式的常用对数');
INSERT INTO  DS_TRANSFORMATION_TYPE  (ID,CODE,OUTPUT_DATA_TYPE,DESCRIPTION)VALUES(48,'SQRT',6,'平方根');
INSERT INTO  DS_TRANSFORMATION_TYPE  (ID,CODE,OUTPUT_DATA_TYPE,DESCRIPTION)VALUES(49,'CEILING',2,'返回大于或等于指定数值表达式的最小整数');
INSERT INTO  DS_TRANSFORMATION_TYPE  (ID,CODE,OUTPUT_DATA_TYPE,DESCRIPTION)VALUES(50,'PI',6,'返回 PI 的常量值');
INSERT INTO  DS_TRANSFORMATION_TYPE  (ID,CODE,OUTPUT_DATA_TYPE,DESCRIPTION)VALUES(51,'SQUARE',6,'返回指定浮点值的平方');
INSERT INTO  DS_TRANSFORMATION_TYPE  (ID,CODE,OUTPUT_DATA_TYPE,DESCRIPTION)VALUES(52,'COS',6,'返回指定表达式中以弧度表示的指定角的三角余弦');
INSERT INTO  DS_TRANSFORMATION_TYPE  (ID,CODE,OUTPUT_DATA_TYPE,DESCRIPTION)VALUES(53,'POWER',6,'返回指定表达式的指定幂的值');
INSERT INTO  DS_TRANSFORMATION_TYPE  (ID,CODE,OUTPUT_DATA_TYPE,DESCRIPTION)VALUES(54,'TAN',6,'正切');
INSERT INTO  DS_TRANSFORMATION_TYPE  (ID,CODE,OUTPUT_DATA_TYPE,DESCRIPTION)VALUES(55,'COT',6,'三角余切');
INSERT INTO  DS_TRANSFORMATION_TYPE  (ID,CODE,OUTPUT_DATA_TYPE,DESCRIPTION)VALUES(56,'RADIANS',6,'对于在数值表达式中输入的度数值返回弧度值');
INSERT INTO  DS_TRANSFORMATION_TYPE  (ID,CODE,OUTPUT_DATA_TYPE,DESCRIPTION)VALUES(57,'CALCULATION',6,'对两个数值进行四则运算');
INSERT INTO  DS_TRANSFORMATION_TYPE  (ID,CODE,OUTPUT_DATA_TYPE,DESCRIPTION)VALUES(58,'MOD',2,'对一个整数取模');
INSERT INTO  DS_TRANSFORMATION_TYPE  (ID,CODE,OUTPUT_DATA_TYPE,DESCRIPTION)VALUES(59,'PROJECTID',2,'返回该Squid所在的Project的ID');
INSERT INTO  DS_TRANSFORMATION_TYPE  (ID,CODE,OUTPUT_DATA_TYPE,DESCRIPTION)VALUES(60,'PROJECTNAME',9,'返回该Squid所在的Project的名字');
INSERT INTO  DS_TRANSFORMATION_TYPE  (ID,CODE,OUTPUT_DATA_TYPE,DESCRIPTION)VALUES(61,'SQUIDFLOWID',2,'返回该Squid所在的SquidFlow的ID');
INSERT INTO  DS_TRANSFORMATION_TYPE  (ID,CODE,OUTPUT_DATA_TYPE,DESCRIPTION)VALUES(62,'SQUIDFLOWNAME',9,'返回该Squid所在的SquidFlow的名字');
INSERT INTO  DS_TRANSFORMATION_TYPE  (ID,CODE,OUTPUT_DATA_TYPE,DESCRIPTION)VALUES(63,'JOBID',2,'返回运行该Squid的Job的ID');
INSERT INTO  DS_TRANSFORMATION_TYPE  (ID,CODE,OUTPUT_DATA_TYPE,DESCRIPTION)VALUES(64,'TOKENIZATION',10,'把文本按照分词规则转化为CSN');
INSERT INTO  DS_TRANSFORMATION_TYPE  (ID,CODE,OUTPUT_DATA_TYPE,DESCRIPTION)VALUES(65,'PREDICT',6,'使用ModelSquid的model给出输入数据的预测值');
-- INSERT INTO  DS_TRANSFORMATION_TYPE  (ID,CODE,OUTPUT_DATA_TYPE,DESCRIPTION)VALUES(66,'QUANTIFY',6,'把分类数据转换为数值型数据');
-- INSERT INTO  DS_TRANSFORMATION_TYPE  (ID,CODE,OUTPUT_DATA_TYPE,DESCRIPTION)VALUES(67,'DISCRETIZE',6,'把连续数值型数据转化为离散数值型数据');
INSERT INTO  DS_TRANSFORMATION_TYPE  (ID,CODE,OUTPUT_DATA_TYPE,DESCRIPTION)VALUES(68,'NUMASSEMBLE',10,'把多个浮点值输入组装为一个CSN格式的字符串');
-- INSERT INTO  DS_TRANSFORMATION_TYPE  (ID,CODE,OUTPUT_DATA_TYPE,DESCRIPTION)VALUES(69,'STRASSEMBLE',23,'把多个字符串输入组装为一个CSS格式的字符串');
INSERT INTO  DS_TRANSFORMATION_TYPE  (ID,CODE,OUTPUT_DATA_TYPE,DESCRIPTION)VALUES(70,'TRAIN',12,'根据该组件所归属的数据挖据squid，使用训练数据得出特定模型。要求标签在输入CSN的第1个位置');
INSERT INTO  DS_TRANSFORMATION_TYPE  (ID,CODE,OUTPUT_DATA_TYPE,DESCRIPTION)VALUES(75,'NUMERICCAST',6,'数值精度转换');
INSERT INTO  DS_TRANSFORMATION_TYPE  (ID,CODE,OUTPUT_DATA_TYPE,DESCRIPTION)VALUES(76,'INVERSEQUANTIFY',21,'把基于dm squidModelBase model的predict输出，反推得到原始的标签值');
INSERT INTO  DS_TRANSFORMATION_TYPE  (ID,CODE,OUTPUT_DATA_TYPE,DESCRIPTION)VALUES(77,'CSNTOSTRING',9,'把一个CSN序列转换成字符串');

INSERT INTO  DS_TRANSFORMATION_TYPE  (ID,CODE,OUTPUT_DATA_TYPE,DESCRIPTION)VALUES(78,'COUNTRY',9,'例子: “中国河南省郑州市二七区花园路88号”.输出为“中国”');
INSERT INTO  DS_TRANSFORMATION_TYPE  (ID,CODE,OUTPUT_DATA_TYPE,DESCRIPTION)VALUES(79,'PROVINCE',9,'例子: “中国河南省郑州市二七区花园路88号”.输出为“河南省”');
INSERT INTO  DS_TRANSFORMATION_TYPE  (ID,CODE,OUTPUT_DATA_TYPE,DESCRIPTION)VALUES(80,'CITY',9,'例子: “中国河南省郑州市二七区花园路88号”.输出为“郑州市”');
INSERT INTO  DS_TRANSFORMATION_TYPE  (ID,CODE,OUTPUT_DATA_TYPE,DESCRIPTION)VALUES(81,'DISTRICT',9,'例子: “中国河南省郑州市二七区花园路88号”.输出为“二七区”');
INSERT INTO  DS_TRANSFORMATION_TYPE  (ID,CODE,OUTPUT_DATA_TYPE,DESCRIPTION)VALUES(82,'STREET',9,'例子: “中国河南省郑州市二七区花园路88号”.输出为“花园路88号”');

INSERT INTO  DS_TRANSFORMATION_TYPE  (ID,CODE,OUTPUT_DATA_TYPE,DESCRIPTION)VALUES(83,'DATEPART',2,'把一个YYYY-MM-DD hh:mm:ss[.nnn]日期时间的部分转换为整型数值');
INSERT INTO  DS_TRANSFORMATION_TYPE  (ID,CODE,OUTPUT_DATA_TYPE,DESCRIPTION)VALUES(84,'DATETOUNIXTIME',1,'把一个标准的系统日期转换为Unix时间戳数字');
INSERT INTO  DS_TRANSFORMATION_TYPE  (ID,CODE,OUTPUT_DATA_TYPE,DESCRIPTION)VALUES(85,'UNIXTIMETODATE',13,'把一个Unix时间戳数字转换为标准的系统日期');
INSERT INTO  DS_TRANSFORMATION_TYPE  (ID,CODE,OUTPUT_DATA_TYPE,DESCRIPTION)VALUES(86,'DATEINC',13,'对一个系统时间增加/减少特定时间单位');

INSERT INTO  DS_TRANSFORMATION_TYPE  (ID,CODE,OUTPUT_DATA_TYPE,DESCRIPTION)VALUES(88,'MAPPUT',1022,'把一个添加或修改集合中的元素');
INSERT INTO  DS_TRANSFORMATION_TYPE  (ID,CODE,OUTPUT_DATA_TYPE,DESCRIPTION)VALUES(89,'MAPGET',1022,'获取集合中某一个元素');
INSERT INTO  DS_TRANSFORMATION_TYPE  (ID,CODE,OUTPUT_DATA_TYPE,DESCRIPTION)VALUES(90,'MAPREMOVE',1022,'对一个系统时间增加/减少特定时间单位');

INSERT INTO  DS_TRANSFORMATION_TYPE  (ID,CODE,OUTPUT_DATA_TYPE,DESCRIPTION)VALUES(91,'ARRAYPUT',86,'把一个添加或修改集合中的元素');
INSERT INTO  DS_TRANSFORMATION_TYPE  (ID,CODE,OUTPUT_DATA_TYPE,DESCRIPTION)VALUES(92,'ARRAYGET',86,'获取集合中某一个元素');
INSERT INTO  DS_TRANSFORMATION_TYPE  (ID,CODE,OUTPUT_DATA_TYPE,DESCRIPTION)VALUES(93,'ARRAYREMOVE',86,'对一个系统时间增加/减少特定时间单位');

INSERT INTO DS_TRAN_INPUT_DEFINITION(CODE,INPUT_ORDER,INPUT_DATA_TYPE,DESCRIPTION)VALUES('VIRTUAL',0,21,'和squidColumn/referenceColumn绑定的虚拟转换。');
INSERT INTO DS_TRAN_INPUT_DEFINITION(CODE,INPUT_ORDER,INPUT_DATA_TYPE,DESCRIPTION)VALUES('CONSTANT',-1,0,'');
INSERT INTO DS_TRAN_INPUT_DEFINITION(CODE,INPUT_ORDER,INPUT_DATA_TYPE,DESCRIPTION)VALUES('CHOICE',9999,21,'');
INSERT INTO DS_TRAN_INPUT_DEFINITION(CODE,INPUT_ORDER,INPUT_DATA_TYPE,DESCRIPTION)VALUES('CONCATENATE',9999,9,'输入字符串');
INSERT INTO DS_TRAN_INPUT_DEFINITION(CODE,INPUT_ORDER,INPUT_DATA_TYPE,DESCRIPTION)VALUES('TERMEXTRACT',0,9,'要分析的源字符串');
INSERT INTO DS_TRAN_INPUT_DEFINITION(CODE,INPUT_ORDER,INPUT_DATA_TYPE,DESCRIPTION)VALUES('SPLIT',0,9,'被拆分的字符串');
INSERT INTO DS_TRAN_INPUT_DEFINITION(CODE,INPUT_ORDER,INPUT_DATA_TYPE,DESCRIPTION)VALUES('ASCII',0,9,'要转换的字符');
INSERT INTO DS_TRAN_INPUT_DEFINITION(CODE,INPUT_ORDER,INPUT_DATA_TYPE,DESCRIPTION)VALUES('UNICODE',0,9,'要转换的字符');
INSERT INTO DS_TRAN_INPUT_DEFINITION(CODE,INPUT_ORDER,INPUT_DATA_TYPE,DESCRIPTION)VALUES('SIMILARITY',0,9,'要对比的第一个字符串');
INSERT INTO DS_TRAN_INPUT_DEFINITION(CODE,INPUT_ORDER,INPUT_DATA_TYPE,DESCRIPTION)VALUES('SIMILARITY',1,9,'要对比的第二个字符串');
INSERT INTO DS_TRAN_INPUT_DEFINITION(CODE,INPUT_ORDER,INPUT_DATA_TYPE,DESCRIPTION)VALUES('CHAR',0,2,'ASCII数值');
INSERT INTO DS_TRAN_INPUT_DEFINITION(CODE,INPUT_ORDER,INPUT_DATA_TYPE,DESCRIPTION)VALUES('PATTERNINDEX',0,9,'要搜索的目标字符串表达式');
INSERT INTO DS_TRAN_INPUT_DEFINITION(CODE,INPUT_ORDER,INPUT_DATA_TYPE,DESCRIPTION)VALUES('PATTERNINDEX',1,9,'源字符串');
INSERT INTO DS_TRAN_INPUT_DEFINITION(CODE,INPUT_ORDER,INPUT_DATA_TYPE,DESCRIPTION)VALUES('REPLICATE',0,9,'被重复的字符串');
INSERT INTO DS_TRAN_INPUT_DEFINITION(CODE,INPUT_ORDER,INPUT_DATA_TYPE,DESCRIPTION)VALUES('NUMERICTOSTRING',0,6,'要转化的数值');
INSERT INTO DS_TRAN_INPUT_DEFINITION(CODE,INPUT_ORDER,INPUT_DATA_TYPE,DESCRIPTION)VALUES('STRINGTONUMERIC',0,9,'要转化的字符串数值');
INSERT INTO DS_TRAN_INPUT_DEFINITION(CODE,INPUT_ORDER,INPUT_DATA_TYPE,DESCRIPTION)VALUES('REPLACE',0,9,'在此字符串中搜索');
INSERT INTO DS_TRAN_INPUT_DEFINITION(CODE,INPUT_ORDER,INPUT_DATA_TYPE,DESCRIPTION)VALUES('REPLACE',1,9,'替换成此字符串');
INSERT INTO DS_TRAN_INPUT_DEFINITION(CODE,INPUT_ORDER,INPUT_DATA_TYPE,DESCRIPTION)VALUES('LEFT',0,9,'被截取的源字符串');
INSERT INTO DS_TRAN_INPUT_DEFINITION(CODE,INPUT_ORDER,INPUT_DATA_TYPE,DESCRIPTION)VALUES('RIGHT',0,9,'被截取的源字符串');
INSERT INTO DS_TRAN_INPUT_DEFINITION(CODE,INPUT_ORDER,INPUT_DATA_TYPE,DESCRIPTION)VALUES('SUBSTRING',0,9,'源字符串');
INSERT INTO DS_TRAN_INPUT_DEFINITION(CODE,INPUT_ORDER,INPUT_DATA_TYPE,DESCRIPTION)VALUES('LENGTH',0,9,'源字符串');
INSERT INTO DS_TRAN_INPUT_DEFINITION(CODE,INPUT_ORDER,INPUT_DATA_TYPE,DESCRIPTION)VALUES('REVERSE',0,9,'源字符串');
INSERT INTO DS_TRAN_INPUT_DEFINITION(CODE,INPUT_ORDER,INPUT_DATA_TYPE,DESCRIPTION)VALUES('LOWER',0,9,'源字符串');
INSERT INTO DS_TRAN_INPUT_DEFINITION(CODE,INPUT_ORDER,INPUT_DATA_TYPE,DESCRIPTION)VALUES('UPPER',0,9,'源字符串');
INSERT INTO DS_TRAN_INPUT_DEFINITION(CODE,INPUT_ORDER,INPUT_DATA_TYPE,DESCRIPTION)VALUES('LEFTTRIM',0,9,'源字符串');
INSERT INTO DS_TRAN_INPUT_DEFINITION(CODE,INPUT_ORDER,INPUT_DATA_TYPE,DESCRIPTION)VALUES('RIGHTTRIM',0,9,'源字符串');
INSERT INTO DS_TRAN_INPUT_DEFINITION(CODE,INPUT_ORDER,INPUT_DATA_TYPE,DESCRIPTION)VALUES('TRIM',0,9,'源字符串');
INSERT INTO DS_TRAN_INPUT_DEFINITION(CODE,INPUT_ORDER,INPUT_DATA_TYPE,DESCRIPTION)VALUES('ROWNUMBER',9999,21,'');
INSERT INTO DS_TRAN_INPUT_DEFINITION(CODE,INPUT_ORDER,INPUT_DATA_TYPE,DESCRIPTION)VALUES('SYSTEMDATETIME',-1,0,'');
INSERT INTO DS_TRAN_INPUT_DEFINITION(CODE,INPUT_ORDER,INPUT_DATA_TYPE,DESCRIPTION)VALUES('STRINGTODATETIME',0,9,'要转换的日期');
INSERT INTO DS_TRAN_INPUT_DEFINITION(CODE,INPUT_ORDER,INPUT_DATA_TYPE,DESCRIPTION)VALUES('DATETIMETOSTRING',0,13,'要转换的日期');
INSERT INTO DS_TRAN_INPUT_DEFINITION(CODE,INPUT_ORDER,INPUT_DATA_TYPE,DESCRIPTION)VALUES('YEAR',0,13,'要转换的日期');
INSERT INTO DS_TRAN_INPUT_DEFINITION(CODE,INPUT_ORDER,INPUT_DATA_TYPE,DESCRIPTION)VALUES('MONTH',0,13,'要转换的日期');
INSERT INTO DS_TRAN_INPUT_DEFINITION(CODE,INPUT_ORDER,INPUT_DATA_TYPE,DESCRIPTION)VALUES('DAY',0,13,'要转换的日期');
INSERT INTO DS_TRAN_INPUT_DEFINITION(CODE,INPUT_ORDER,INPUT_DATA_TYPE,DESCRIPTION)VALUES('DATEDIFF',0,13,'开始日期');
INSERT INTO DS_TRAN_INPUT_DEFINITION(CODE,INPUT_ORDER,INPUT_DATA_TYPE,DESCRIPTION)VALUES('DATEDIFF',1,13,'结束日期');
INSERT INTO DS_TRAN_INPUT_DEFINITION(CODE,INPUT_ORDER,INPUT_DATA_TYPE,DESCRIPTION)VALUES('FORMATDATE',0,9,'日期值');
INSERT INTO DS_TRAN_INPUT_DEFINITION(CODE,INPUT_ORDER,INPUT_DATA_TYPE,DESCRIPTION)VALUES('ABS',0,6,'数值');
INSERT INTO DS_TRAN_INPUT_DEFINITION(CODE,INPUT_ORDER,INPUT_DATA_TYPE,DESCRIPTION)VALUES('RANDOM',-1,0,'');
INSERT INTO DS_TRAN_INPUT_DEFINITION(CODE,INPUT_ORDER,INPUT_DATA_TYPE,DESCRIPTION)VALUES('ACOS',0,6,'-1到+1之间的浮点数');
INSERT INTO DS_TRAN_INPUT_DEFINITION(CODE,INPUT_ORDER,INPUT_DATA_TYPE,DESCRIPTION)VALUES('EXP',0,6,'浮点数');
INSERT INTO DS_TRAN_INPUT_DEFINITION(CODE,INPUT_ORDER,INPUT_DATA_TYPE,DESCRIPTION)VALUES('ROUND',0,6,'浮点数');
INSERT INTO DS_TRAN_INPUT_DEFINITION(CODE,INPUT_ORDER,INPUT_DATA_TYPE,DESCRIPTION)VALUES('ASIN',0,6,'-1到+1之间的浮点数');
INSERT INTO DS_TRAN_INPUT_DEFINITION(CODE,INPUT_ORDER,INPUT_DATA_TYPE,DESCRIPTION)VALUES('FLOOR',0,6,'浮点数');
INSERT INTO DS_TRAN_INPUT_DEFINITION(CODE,INPUT_ORDER,INPUT_DATA_TYPE,DESCRIPTION)VALUES('SIGN',0,6,'浮点数');
INSERT INTO DS_TRAN_INPUT_DEFINITION(CODE,INPUT_ORDER,INPUT_DATA_TYPE,DESCRIPTION)VALUES('ATAN',0,6,'-1到+1之间的浮点数');
INSERT INTO DS_TRAN_INPUT_DEFINITION(CODE,INPUT_ORDER,INPUT_DATA_TYPE,DESCRIPTION)VALUES('LOG',0,6,'浮点数');
INSERT INTO DS_TRAN_INPUT_DEFINITION(CODE,INPUT_ORDER,INPUT_DATA_TYPE,DESCRIPTION)VALUES('SIN',0,6,'浮点数');
INSERT INTO DS_TRAN_INPUT_DEFINITION(CODE,INPUT_ORDER,INPUT_DATA_TYPE,DESCRIPTION)VALUES('LOG10',0,6,'浮点数');
INSERT INTO DS_TRAN_INPUT_DEFINITION(CODE,INPUT_ORDER,INPUT_DATA_TYPE,DESCRIPTION)VALUES('SQRT',0,6,'浮点数');
INSERT INTO DS_TRAN_INPUT_DEFINITION(CODE,INPUT_ORDER,INPUT_DATA_TYPE,DESCRIPTION)VALUES('CEILING',0,6,'浮点数');
INSERT INTO DS_TRAN_INPUT_DEFINITION(CODE,INPUT_ORDER,INPUT_DATA_TYPE,DESCRIPTION)VALUES('PI',-1,0,'');
INSERT INTO DS_TRAN_INPUT_DEFINITION(CODE,INPUT_ORDER,INPUT_DATA_TYPE,DESCRIPTION)VALUES('SQUARE',0,6,'浮点数');
INSERT INTO DS_TRAN_INPUT_DEFINITION(CODE,INPUT_ORDER,INPUT_DATA_TYPE,DESCRIPTION)VALUES('COS',0,6,'浮点数');
INSERT INTO DS_TRAN_INPUT_DEFINITION(CODE,INPUT_ORDER,INPUT_DATA_TYPE,DESCRIPTION)VALUES('POWER',0,6,'浮点数');
INSERT INTO DS_TRAN_INPUT_DEFINITION(CODE,INPUT_ORDER,INPUT_DATA_TYPE,DESCRIPTION)VALUES('TAN',0,6,'浮点数');
INSERT INTO DS_TRAN_INPUT_DEFINITION(CODE,INPUT_ORDER,INPUT_DATA_TYPE,DESCRIPTION)VALUES('COT',0,6,'-1到+1之间的浮点数');
INSERT INTO DS_TRAN_INPUT_DEFINITION(CODE,INPUT_ORDER,INPUT_DATA_TYPE,DESCRIPTION)VALUES('RADIANS',0,6,'浮点数');
INSERT INTO DS_TRAN_INPUT_DEFINITION(CODE,INPUT_ORDER,INPUT_DATA_TYPE,DESCRIPTION)VALUES('CALCULATION',0,6,'参加运算的第一个浮点数');
INSERT INTO DS_TRAN_INPUT_DEFINITION(CODE,INPUT_ORDER,INPUT_DATA_TYPE,DESCRIPTION)VALUES('CALCULATION',1,6,'参加运算的第二个浮点数');
INSERT INTO DS_TRAN_INPUT_DEFINITION(CODE,INPUT_ORDER,INPUT_DATA_TYPE,DESCRIPTION)VALUES('MOD',0,2,'参加运算的整数');
INSERT INTO DS_TRAN_INPUT_DEFINITION(CODE,INPUT_ORDER,INPUT_DATA_TYPE,DESCRIPTION)VALUES('PROJECTID',-1,0,'');
INSERT INTO DS_TRAN_INPUT_DEFINITION(CODE,INPUT_ORDER,INPUT_DATA_TYPE,DESCRIPTION)VALUES('PROJECTNAME',-1,0,'');
INSERT INTO DS_TRAN_INPUT_DEFINITION(CODE,INPUT_ORDER,INPUT_DATA_TYPE,DESCRIPTION)VALUES('SQUIDFLOWID',-1,0,'');
INSERT INTO DS_TRAN_INPUT_DEFINITION(CODE,INPUT_ORDER,INPUT_DATA_TYPE,DESCRIPTION)VALUES('SQUIDFLOWNAME',-1,0,'');
INSERT INTO DS_TRAN_INPUT_DEFINITION(CODE,INPUT_ORDER,INPUT_DATA_TYPE,DESCRIPTION)VALUES('JOBID',-1,0,'');
INSERT INTO DS_TRAN_INPUT_DEFINITION(CODE,INPUT_ORDER,INPUT_DATA_TYPE,DESCRIPTION)VALUES('TOKENIZATION',0,9,'分词文本');
INSERT INTO DS_TRAN_INPUT_DEFINITION(CODE,INPUT_ORDER,INPUT_DATA_TYPE,DESCRIPTION)VALUES('PREDICT ',0,10,'预测数据');
INSERT INTO DS_TRAN_INPUT_DEFINITION(CODE,INPUT_ORDER,INPUT_DATA_TYPE,DESCRIPTION)VALUES('PREDICT ',1,21,'预测数据的KEY');

INSERT INTO DS_TRAN_INPUT_DEFINITION(CODE,INPUT_ORDER,INPUT_DATA_TYPE,DESCRIPTION)VALUES('NUMASSEMBLE',9999,6,'浮点型输入');
INSERT INTO DS_TRAN_INPUT_DEFINITION(CODE,INPUT_ORDER,INPUT_DATA_TYPE,DESCRIPTION)VALUES('TRAIN',0,10,'训练数据');
INSERT INTO DS_TRAN_INPUT_DEFINITION(CODE,INPUT_ORDER,INPUT_DATA_TYPE,DESCRIPTION)VALUES('NUMERICCAST',0,6,'被转换的数值输入');
INSERT INTO DS_TRAN_INPUT_DEFINITION(CODE,INPUT_ORDER,INPUT_DATA_TYPE,DESCRIPTION)VALUES('INVERSEQUANTIFY',0,6,'浮点输入');
INSERT INTO DS_TRAN_INPUT_DEFINITION(CODE,INPUT_ORDER,INPUT_DATA_TYPE,DESCRIPTION)VALUES('CSNTOSTRING',0,10,'CSN输入');

INSERT INTO DS_TRAN_INPUT_DEFINITION(CODE,INPUT_ORDER,INPUT_DATA_TYPE,DESCRIPTION)VALUES('COUNTRY',0,9,'源字符串');
INSERT INTO DS_TRAN_INPUT_DEFINITION(CODE,INPUT_ORDER,INPUT_DATA_TYPE,DESCRIPTION)VALUES('PROVINCE',0,9,'源字符串');
INSERT INTO DS_TRAN_INPUT_DEFINITION(CODE,INPUT_ORDER,INPUT_DATA_TYPE,DESCRIPTION)VALUES('CITY',0,9,'源字符串');
INSERT INTO DS_TRAN_INPUT_DEFINITION(CODE,INPUT_ORDER,INPUT_DATA_TYPE,DESCRIPTION)VALUES('DISTRICT',0,9,'源字符串');
INSERT INTO DS_TRAN_INPUT_DEFINITION(CODE,INPUT_ORDER,INPUT_DATA_TYPE,DESCRIPTION)VALUES('STREET',0,9,'源字符串');

INSERT INTO DS_TRAN_INPUT_DEFINITION(CODE,INPUT_ORDER,INPUT_DATA_TYPE,DESCRIPTION)VALUES('DATEPART',0,13,'原始日期');
INSERT INTO DS_TRAN_INPUT_DEFINITION(CODE,INPUT_ORDER,INPUT_DATA_TYPE,DESCRIPTION)VALUES('DATETOUNIXTIME',0,13,'要转换的日期');
INSERT INTO DS_TRAN_INPUT_DEFINITION(CODE,INPUT_ORDER,INPUT_DATA_TYPE,DESCRIPTION)VALUES('UNIXTIMETODATE',0,1,'要转换unix时间戳整数');
INSERT INTO DS_TRAN_INPUT_DEFINITION(CODE,INPUT_ORDER,INPUT_DATA_TYPE,DESCRIPTION)VALUES('DATEINC',0,13,'要做增减运算的日期');

INSERT INTO DS_TRAN_INPUT_DEFINITION(CODE,INPUT_ORDER,INPUT_DATA_TYPE,DESCRIPTION)VALUES('MAPPUT',0,1022,'原始集合');
INSERT INTO DS_TRAN_INPUT_DEFINITION(CODE,INPUT_ORDER,INPUT_DATA_TYPE,DESCRIPTION)VALUES('MAPPUT',1,9,'要添加的元素键');
INSERT INTO DS_TRAN_INPUT_DEFINITION(CODE,INPUT_ORDER,INPUT_DATA_TYPE,DESCRIPTION)VALUES('MAPPUT',2,21,'要添加的元素值');
INSERT INTO DS_TRAN_INPUT_DEFINITION(CODE,INPUT_ORDER,INPUT_DATA_TYPE,DESCRIPTION)VALUES('MAPGET',0,1022,'原始集合');
INSERT INTO DS_TRAN_INPUT_DEFINITION(CODE,INPUT_ORDER,INPUT_DATA_TYPE,DESCRIPTION)VALUES('MAPGET',1,9,'要获取的元素键');
INSERT INTO DS_TRAN_INPUT_DEFINITION(CODE,INPUT_ORDER,INPUT_DATA_TYPE,DESCRIPTION)VALUES('MAPREMOVE',0,1022,'原始集合');
INSERT INTO DS_TRAN_INPUT_DEFINITION(CODE,INPUT_ORDER,INPUT_DATA_TYPE,DESCRIPTION)VALUES('MAPREMOVE',1,9,'要移除的元素键');

INSERT INTO DS_TRAN_INPUT_DEFINITION(CODE,INPUT_ORDER,INPUT_DATA_TYPE,DESCRIPTION)VALUES('ARRAYPUT',0,86,'原始集合');
INSERT INTO DS_TRAN_INPUT_DEFINITION(CODE,INPUT_ORDER,INPUT_DATA_TYPE,DESCRIPTION)VALUES('ARRAYPUT',1,2,'要添加的元素键');
INSERT INTO DS_TRAN_INPUT_DEFINITION(CODE,INPUT_ORDER,INPUT_DATA_TYPE,DESCRIPTION)VALUES('ARRAYPUT',2,21,'要添加的元素值');
INSERT INTO DS_TRAN_INPUT_DEFINITION(CODE,INPUT_ORDER,INPUT_DATA_TYPE,DESCRIPTION)VALUES('ARRAYGET',0,86,'原始集合');
INSERT INTO DS_TRAN_INPUT_DEFINITION(CODE,INPUT_ORDER,INPUT_DATA_TYPE,DESCRIPTION)VALUES('ARRAYGET',1,2,'要获取的元素键');
INSERT INTO DS_TRAN_INPUT_DEFINITION(CODE,INPUT_ORDER,INPUT_DATA_TYPE,DESCRIPTION)VALUES('ARRAYREMOVE',0,86,'原始集合');
INSERT INTO DS_TRAN_INPUT_DEFINITION(CODE,INPUT_ORDER,INPUT_DATA_TYPE,DESCRIPTION)VALUES('ARRAYREMOVE',1,2,'要移除的元素键');
