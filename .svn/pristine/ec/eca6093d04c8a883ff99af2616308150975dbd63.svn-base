-- 系统默认数据库（权限相关，及元数据仓库配置信息）
-- !该脚本需要手工执行(或由部署脚本执行)!

CREATE TABLE DS_SYS_TEAM (  
 id  int AUTO_INCREMENT NOT NULL primary key,
 `key` varchar(50),
 `name` nvarchar(100) NOT NULL,
 description nvarchar(500),
 creation_date TIMESTAMP
);
CREATE TABLE DS_SYS_REPOSITORY ( 
 id  INTEGER AUTO_INCREMENT NOT NULL primary key,
 team_id INTEGER NOT NULL,
 `key` varchar(50),
 `name` nvarchar(100) NOT NULL,
 description nvarchar(500),
 repository_db_name nvarchar(100) NOT NULL,
 type INTEGER,
 status_id INTEGER NOT NULL,
 creation_date TIMESTAMP
);
CREATE TABLE DS_SYS_REPOSITORY_STATUS ( 
 id  INTEGER AUTO_INCREMENT NOT NULL primary key,
 `name` nvarchar(100) NOT NULL,
 description nvarchar(500)
);
CREATE TABLE DS_SYS_GROUP ( 
  id  INTEGER AUTO_INCREMENT NOT NULL primary key,
 `key` varchar(50),
 team_id INTEGER NOT NULL,
 parent_group_id INTEGER,
 `name` nvarchar(128) NOT NULL,
 description nvarchar(500),
 location_x INTEGER,
 location_y INTEGER,
 creation_date datetime
);
CREATE TABLE DS_SYS_ROLE ( 
 id  INTEGER AUTO_INCREMENT NOT NULL primary key,
 `key` varchar(50) NOT NULL,
 group_id INTEGER,
 `name` nvarchar(100) NOT NULL,
 description nvarchar(500),
 creation_date TIMESTAMP
);
CREATE TABLE DS_SYS_USER ( 
 id  INTEGER AUTO_INCREMENT NOT NULL primary key,
 `key` varchar(50) NOT NULL,
 role_id INTEGER,
 user_name varchar(50) NOT NULL,
 `password` varchar(50) ,
 full_name nvarchar(20),
 email_address varchar(100),
 status_id INTEGER,
 is_active varchar(1),
 last_logon_date TIMESTAMP,
 creation_date TIMESTAMP
);
CREATE TABLE DS_SYS_USER_STATUS ( 
 id  INTEGER AUTO_INCREMENT NOT NULL primary key,
 `name` nvarchar(100),
 description nvarchar(500)
);
CREATE TABLE DS_SYS_PRIVILEGE (
 party_id INTEGER,
 party_type varchar(1),
 entity_type_id INTEGER,
 can_view varchar(1),
 can_create varchar(1),
 can_update varchar(1),
 can_delete varchar(1)
);
CREATE TABLE DS_SYS_ENTITY_TYPE (
 id INTEGER primary key,
 `name` nvarchar(50) NOT NULL,
 description nvarchar(500)
);
CREATE TABLE DS_SYS_ACTION_HEADER ( 
 id  INTEGER AUTO_INCREMENT NOT NULL primary key,
 sys_table_name varchar(50) NOT NULL,
 action varchar(1) NOT NULL,
 user_id INTEGER NOT NULL,
 is_undone varchar(1) NOT NULL,
 action_date datetime NOT NULL
);
CREATE TABLE DS_SYS_ACTION_LINE ( 
 id INTEGER NOT NULL,
 header_id INTEGER,
 column_name varchar(100) NOT NULL,
 cell_value nvarchar(1000)
);
CREATE TABLE DS_SYS_SERVER_PARAMETER ( 
 id  INTEGER AUTO_INCREMENT NOT NULL primary key,
 `name` nvarchar(128),
 `value` nvarchar(256),
 data_type varchar(1) NOT NULL,
 is_collection varchar(1) NOT NULL
);
-- 数据类型映射关系配置表 (precision、scale分别对应目前表结构中的length、precision)
-- create_param_count, 0表示创建时不指定参数；1表示需指定max-length/precision；2表示需指定precision,scale；3表示precision必填scale可选
-- RESTORE_PRIORITY, 0表示不可还原（比如DATE不可还原为YEAR）；1-10优先级逐级递减（比如REAL可以优先还原为REAL,如果是货币也可以还原为SMALLMONEY）
CREATE TABLE DS_SYS_DATATYPE_MAPPING (
 id INT AUTO_INCREMENT primary key,
 DATABASE_TYPE VARCHAR(128) NOT NULL,
 SOURCE_DATATYPE VARCHAR(128) NOT NULL,
 SYSTEM_DATATYPE VARCHAR(128) NOT NULL,
 RESTORE_PRIORITY TINYINT,
 `PRECISION` BIGINT,
 SCALE SMALLINT,
 CREATE_PARAM_COUNT TINYINT,
 DESCRIPTION VARCHAR(500)
);
CREATE TABLE DS_SYS_TEMPLATE_TYPE (
 id  INTEGER AUTO_INCREMENT NOT NULL primary key,
 type_name nvarchar(50),
 data_type int,
 creation_date TIMESTAMP
);
CREATE TABLE DS_SYS_TEMPLATE_DATA (
 id  INTEGER AUTO_INCREMENT NOT NULL primary key,
 type_id int,
 data_value nvarchar(500),
 creation_date TIMESTAMP
);
-- ReportFolder相关表start --
CREATE TABLE DS_SYS_REPORT_FOLDER(
ID INTEGER AUTO_INCREMENT NOT NULL PRIMARY KEY,
FOLDER_NAME VARCHAR(50),
CREATION_DATE TIMESTAMP,
PID INTEGER,
IS_DISPLAY BOOLEAN,
CONSTRAINT FK_PID FOREIGN KEY(PID) REFERENCES DS_SYS_REPORT_FOLDER(ID) ON DELETE CASCADE
);


CREATE TABLE DS_SYS_REPORT_FOLDER_MAPPING(
	ID INTEGER AUTO_INCREMENT NOT NULL PRIMARY KEY,
	REPOSITORY_ID INTEGER,
	SQUID_ID INTEGER,
	FOLDER_ID INTEGER,
	REPORT_NAME VARCHAR(50),
	CREATION_DATE TIMESTAMP,
	CONSTRAINT FK_FOLDER_ID FOREIGN KEY(FOLDER_ID) REFERENCES DS_SYS_REPORT_FOLDER(ID) ON DELETE CASCADE
);

CREATE TABLE DS_SYS_METADATA_NODE
(
   ID                   INT NOT NULL AUTO_INCREMENT,
   PARENT_ID            INT,
   NODE_TYPE            INT,
   NODE_NAME            VARCHAR(100),
   ORDER_NUMBER         INT,
   NODE_DESC            VARCHAR(200),
   CREATION_DATE        TIMESTAMP,
   PRIMARY KEY (ID)
);

CREATE TABLE DS_SYS_METADATA_NODE_ATTR
(
   ID                   INT NOT NULL,
   ATTR_NAME            VARCHAR(100),
   ATTR_VALUE           VARCHAR(500),
   NODE_ID              INT,
   CREATION_DATE        TIMESTAMP,
   PRIMARY KEY (ID)
);
create table DS_SYS_JOB_SCHEDULE
(
   id                   int not null AUTO_INCREMENT,
   object_type 			int,
   team_id              int,
   repository_id        int,
   project_id           int,
   project_name         varchar(100),
   squid_flow_id        int,
   squid_flow_name      varchar(100),
   squid_id 			int,
   schedule_type        varchar(50),
   schedule_begin_date  TIMESTAMP,
   schedule_end_date    TIMESTAMP,
   schedule_valid       int,
   day_dely             int,
   day_run_count        int,
   day_begin_date       time,
   day_end_date         time,
   day_run_dely         int,
   day_once_off_time 	time,
   week_day             int,
   week_begin_date      time,
   month_day            int,
   month_begin_date     time,
   last_scheduled_date  TIMESTAMP,
   creation_date        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
   `status`               varchar(1),
   enable_email_sending int,
   email_address        varchar(500),
   primary key (id)
);

create table DS_SYS_SF_JOB_HISTORY
(
   task_id              varchar(40) not null,
   repository_id        int,
   squid_flow_Id         int,
   job_id               int,
   `status`               int,
   debug_squids         varchar(500),
   destination_squids   varchar(500),
   caller               varchar(10),
   create_time          TIMESTAMP,
   update_time          TIMESTAMP,
   primary key (task_id)
);

create table DS_SYS_SF_JOB_MODULE_LOG
(
   task_id              varchar(40) not null,
   squid_id             int not null,
   `status`               int,
   create_time          TIMESTAMP,
   update_time          TIMESTAMP,
   primary key (task_id, squid_id)
);
CREATE TABLE DS_SYS_SF_LOG (
  id int NOT NULL AUTO_INCREMENT ,
  task_id varchar(36) NOT NULL ,
  message varchar(500)  ,
  create_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP ,
  log_level int  ,
  squid_id int ,
  tsquid_id varchar(40) ,
  tsquid_type int ,
  PRIMARY KEY (id)
);
create table DS_SYS_SQUID_FLOW_STATUS
(
   id int NOT NULL AUTO_INCREMENT,
   team_id              int,
   repository_id        int,
   project_id           int,
   squid_flow_id        int,
   `status`         		int,
   owner_client_token   varchar(50),
   primary key (id)
);

-- 建立索引
CREATE INDEX IDX_REPORT_FOLDER_PID ON DS_SYS_REPORT_FOLDER(PID);

CREATE INDEX IDX_MAPPING_FOLDER_ID ON DS_SYS_REPORT_FOLDER_MAPPING(FOLDER_ID);

CREATE INDEX IDX_job_HISTORY_job_ID ON DS_SYS_SF_JOB_HISTORY(job_id);

CREATE INDEX IDX_status_repository_id ON DS_SYS_SQUID_FLOW_STATUS(repository_id);
CREATE INDEX IDX_status_project_id ON DS_SYS_SQUID_FLOW_STATUS(project_id);
CREATE INDEX IDX_status_squid_flow_id ON DS_SYS_SQUID_FLOW_STATUS(squid_flow_id);


-- INSERT INTO DS_SYS_REPORT_FOLDER (ID, FOLDER_NAME, CREATION_DATE, PID, IS_DISPLAY)VALUES(-1,'报表文件夹','2014-01-01 15:53:27.733000',-1, TRUE);
-- ReportFolder相关表     end---

-- alter table DS_SYS_REPOSITORY add constraint Q_DS_REPOSITORY_repository_db_name unique(repository_db_name);
alter table DS_SYS_REPOSITORY add constraint unique_repository_name_in_team unique (team_id, name);
alter table DS_SYS_USER add constraint unique_user_name unique(user_name);
alter table DS_SYS_ROLE add constraint unique_role_name_in_group unique(name, group_id);
alter table DS_SYS_group add constraint unique_group_name_in_team unique(name, team_id);
alter table DS_SYS_team add constraint unique_team_name unique(name);
alter table DS_SYS_REPOSITORY add constraint FK_REP_TEAM_ID foreign key (team_id) references DS_SYS_TEAM (id);
-- alter table DS_SYS_REPOSITORY add constraint FK_REP_STATUS_ID foreign key (status_id) references DS_SYS_REPOSITORY_STATUS (id);
alter table DS_SYS_GROUP add constraint FK_GROUP_TEAM_ID foreign key (team_id) references DS_SYS_TEAM (id);
alter table DS_SYS_ROLE add constraint FK_ROLE_GROUP_ID foreign key (group_id) references DS_SYS_GROUP (id);
alter table DS_SYS_USER add constraint FK_USER_ROLE_ID foreign key (role_id) references DS_SYS_ROLE (id);

alter table DS_SYS_TEMPLATE_DATA add constraint FK_TEMPLATE_FOR_TYPE_ID foreign key (type_id) references DS_SYS_TEMPLATE_TYPE(id);
alter table DS_SYS_METADATA_NODE_ATTR add constraint FK_Reference_10 foreign key (node_id)
      references DS_SYS_METADATA_NODE (id)  ON DELETE CASCADE;

alter table DS_SYS_SF_JOB_HISTORY add constraint FK_JOB_ID foreign key (job_id)
      references DS_SYS_JOB_SCHEDULE (id);
alter table DS_SYS_SF_JOB_MODULE_LOG add constraint FK_TASK_ID foreign key (task_id)
      references DS_SYS_SF_JOB_HISTORY (task_id);
 alter table DS_SYS_SF_LOG add constraint FK_SF_TASK_ID foreign key (task_id)
      references DS_SYS_SF_JOB_HISTORY (task_id);     

-- alter table DS_SYS_USER add constraint FK_USER_STATUS_ID foreign key (status_id) references DS_SYS_USER_STATUS (id);
-- alter table DS_SYS_PRIVILEGE add constraint FK_PRI_ENTITY_TYPE_ID foreign key (entity_type_id) references DS_SYS_ENTITY_TYPE (id);

-- INSERT INTO DS_SYS_REPOSITORY_STATUS(name,description)values('ACTIVE','活动，可访问');
-- INSERT INTO DS_SYS_REPOSITORY_STATUS(name,description)values('LOCKED','被锁定，不可访问');
-- INSERT INTO DS_SYS_REPOSITORY_STATUS(name,description)values('VALIDATION','正在校验');
-- INSERT INTO DS_SYS_REPOSITORY_STATUS(name,description)values('VALIDATION_ERROR','校验出现错误');
-- INSERT INTO DS_SYS_REPOSITORY_STATUS(name,description)values('VALIDATION_WARNING','校验结果发现一般性错误可正常访问');
-- INSERT INTO DS_SYS_REPOSITORY_STATUS(name,description)values('INVALID','非法的repository数据');

-- INSERT INTO DS_SYS_USER_STATUS(name,description)values('Online','登陆');
-- INSERT INTO DS_SYS_USER_STATUS(name,description)values('Offline','注销');
-- INSERT INTO DS_SYS_USER_STATUS(name,description)values('DoNotDisturb','勿扰');
-- INSERT INTO DS_SYS_USER_STATUS(name,description)values('Meeting','会议');

-- 权限设置所涉及到的被操作对象类型(id refers DSObjectType.Entity_Type_ID)
-- insert into DS_SYS_ENTITY_TYPE(id, name) values (2,'User');
-- insert into DS_SYS_ENTITY_TYPE(id, name) values (14,'Team');
-- insert into DS_SYS_ENTITY_TYPE(id, name) values (17,'Group');
-- insert into DS_SYS_ENTITY_TYPE(id, name) values (16,'Role');
-- insert into DS_SYS_ENTITY_TYPE(id, name) values (3,'Repository');
-- insert into DS_SYS_ENTITY_TYPE(id, name) values (1,'SquidModelBase');
-- insert into DS_SYS_ENTITY_TYPE(id, name) values (4,'PROJECT');

-- 系统参数配置表 初始化参数
INSERT INTO DS_SYS_SERVER_PARAMETER(name,value,data_type,is_collection) VALUES
	-- 超级用户默认密码
	('SuperUserPwd','111111','S','N'),
	-- repository物理数据库路径(注意：创建仓库时在该路径后追加'db_name/',否则所有不同仓库的物理文件都会在一个文件夹下)
	('HsqlDBPath','D:/datashire/repository/','S','N'),
	-- socket服务侦听端口
	('ServerPort','8888','N','N'),
	-- 过期时间
	('LimitedTime','rl7yKEgF+EeKop/YzzdtXg==','N','N'),
	-- licensekey
	('LicenseKey','','N','N');
	
-- 测试数据 （前台登录时要选择team）
INSERT INTO DS_SYS_TEAM VALUES
 	(1,'vkmDfKoI-oFGw-ftvQ-y16F-MaLDwm7JsUWw','测试team',NULL,'2013-10-19 16:17:45');
INSERT INTO DS_SYS_REPOSITORY(ID,TEAM_ID,`KEY`, `NAME`, DESCRIPTION, REPOSITORY_DB_NAME, TYPE, STATUS_ID, CREATION_DATE )VALUES 
	(1, 1, 'F0EDEC4A-1D4D-CC2A-2BC6-9A6D81E52932', 'test', 'test', 'prod', 1,1,'2013-10-19 16:17:45'),
	(2, 1, 'DD98E05F-AE33-BAB9-DBF4-28BCF31A786E', 'dev', 'dev', 'prod', 1,1,'2013-10-19 16:17:45'),
	(3, 1, '23CCFA2B-CA06-1E23-EEB1-F6CFFB83C939', 'prod', 'prod', 'prod', 1,1,'2013-10-19 16:17:45');

INSERT INTO DS_SYS_REPORT_FOLDER(ID, FOLDER_NAME, CREATION_DATE, PID, IS_DISPLAY) VALUES
	(-1, null, null, null, false),
	(1, 'root', '2013-10-19 16:17:45', -1, true);

-- 测试环境数据
-- INSERT INTO DS_SYS_GROUP(KEY,TEAM_ID, PARENT_GROUP_ID, NAME, DESCRIPTION, LOCATION_X, LOCATION_Y, CREATION_DATE )VALUES ('48D18496-CF6C-CDDF-F55F-08614608BF09',1 ,0, '测试数据插入', '测试数据插入', 1, 1,'2013-10-19 16:17:45.000000' );
-- INSERT INTO DS_SYS_ROLE( ID,KEY, GROUP_ID, NAME, DESCRIPTION, CREATION_DATE )
-- VALUES (1,'vkmDfKoI-oFGw-ftvQ-y16F-MaLDwm7JsUWw',1 , 'Group', 'GRoup','2013-10-19 16:17:45.000000' );
-- INSERT INTO DS_SYS_USER(  KEY, ROLE_ID, USER_NAME, PASSWORD, FULL_NAME, EMAIL_ADDRESS, STATUS_ID, IS_ACTIVE, LAST_LOGON_DATE, CREATION_DATE )
-- VALUES (  'vkmDfKoI-oFGw-ftvQ-y16F-MaLDwm7JsUWw',1 , 'TESTUser', '123456', 'TESTUser', '测试地址',1 , 'n','2013-10-19 16:17:45.000000' , '2013-10-19 16:17:45.000000');