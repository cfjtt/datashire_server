UPDATE DS_SYS_SERVER_PARAMETER SET value='2.1.0' where name = 'VERSION';
/*修改rulesquery的出参为array*/
UPDATE ds_transformation_type  SET OUTPUT_DATA_TYPE=86 WHERE ID=98;
/*增加IS_PARTITION_COLUMN作为HDFS落地分片的标示*/
ALTER TABLE ds_dest_hdfs_column ADD `IS_PARTITION_COLUMN` INT;

/*创建历史版本列表数据表*/
DROP TABLE IF EXISTS SQUID_FLOW_HISTORY;
CREATE TABLE SQUID_FLOW_HISTORY (
  ID INTEGER NOT NULL AUTO_INCREMENT,
  SQUID_FLOW_ID INTEGER,
  SQUID_FLOW_NAME varchar(255),
  PROJECT_ID INTEGER,
  PROJECT_NAME varchar(255) ,
  REPOSITORY_ID INTEGER ,
  USER_ID INTEGER ,
  COMMIT_TIME TIMESTAMP,
  COMMENTS varchar(4000) ,
  VERSION INTEGER,
  SQUID_FLOW_TYPE INTEGER ,
  SUBMIT_USERNAME varchar(255),
  PRIMARY KEY (ID)
);
CREATE UNIQUE INDEX SQUID_FLOW_HISTORY_ID ON SQUID_FLOW_HISTORY(ID ASC);
CREATE INDEX HISTORY_SQUIDFLOWID_PROJECTID_REP_ID ON SQUID_FLOW_HISTORY(SQUID_FLOW_ID,PROJECT_ID,REPOSITORY_ID);

/*创建历史版本数据表*/
DROP TABLE IF EXISTS HISTORY_VERSION_DATA;
CREATE TABLE HISTORY_VERSION_DATA (
  ID INTEGER NOT NULL AUTO_INCREMENT,
  SQUIDFLOWHISTORYID INTEGER,
  SQUID_FLOW_ID INTEGER ,
  REPOSITORY_ID INTEGER ,
  COMMIT_TIME timestamp ,
  DATA longtext,
  SQUIDTYPE INTEGER ,
  PROCESSTYPE INTEGER ,
  SQUID_FLOW_TYPE INTEGER ,
  PRIMARY KEY (id)
);
CREATE UNIQUE  INDEX VERSION_DATA_ID ON HISTORY_VERSION_DATA(ID ASC);
CREATE INDEX DATA_FLOWID_REPID_PROTYPE ON HISTORY_VERSION_DATA(SQUIDFLOWHISTORYID,SQUID_FLOW_ID,REPOSITORY_ID,PROCESSTYPE,SQUID_FLOW_TYPE);

/*这里需要在mysql的配置文件my.ini(my.cnf)中添加或者修改成这段配置
  max_allowed_packet = 30M
  避免在版本控制里面数据插入不成功
  同时在配置文件中，修改innodb_lock_wait_timeout=120
*/
update ds_sys_server_parameter set value='{"MYSQL":"BLOB,LONGBLOB,MEDIUMBLOB,TINYBLOB,XML,VARCHAR","ORACLE":"BLOB,BINARY_DOUBLE,NCLOB,CLOB","SQLSERVER":"BINARY,VARCHAR,NVARCHAR,VARBINARY,TIMESTAMP,UNIQUEIDENTIFIER,IMAGE,XML,HIERARCHYID,TEXT,NTEXT,DATETIMEOFFSET,SQL_VARIANT,BIT,CHAR","DB2":"CLOB"}' where name='SPLIT_COLUMN_FILTER';

/*修改sourceColumn的relative_order的长度为11，防止抽取大数据量的时候，长度不够*/
ALTER TABLE  DS_SOURCE_COLUMN MODIFY RELATIVE_ORDER INT;
/*修改doc_extract中字段delimiter长度*/
ALTER TABLE DS_DOC_EXTRACT MODIFY DELIMITER VARCHAR(200);

/*设置ds_reference_column索引*/
CREATE  INDEX IDX_REFERENCE_COLUMN ON DS_REFERENCE_COLUMN (REFERENCE_SQUID_ID asc);

/*修改版本号*/
update ds_sys_server_parameter set VALUE='2.1.0' where name='VERSION' and id=6;

/**增加新的squid类型*/
insert into ds_squid_type(ID,CODE,DESCRIPTION) value (49,51,'SOURCECLOUDFILE');
insert into ds_squid_type(ID,code,DESCRIPTION) value(50,52,'CLOUDDB');
insert into ds_squid_type(ID,code,DESCRIPTION) value(51,53,'DESTCLOUDFILE');

/**更新ftp_connect  host中字段的长度**/
ALTER TABLE ds_ftp_connection MODIFY HOST VARCHAR(30);

/**ds_squid_flow添加field_type字段**/
ALTER TABLE ds_squid_flow ADD FIELD_TYPE INT(5);

/**SQUID_FLOW_HISTORY添加field_type字段**/
ALTER TABLE SQUID_FLOW_HISTORY ADD FIELD_TYPE INT(5);

/**修改ds_dm_squid double类型精度**/
ALTER TABLE DS_DM_SQUID MODIFY TRAINING_PERCENTAGE DOUBLE(64,15);
ALTER TABLE DS_DM_SQUID MODIFY MIN_BATCH_FRACTION DOUBLE(64,15);
ALTER TABLE DS_DM_SQUID MODIFY STEP_SIZE DOUBLE(64,15);
ALTER TABLE DS_DM_SQUID MODIFY SMOOTHING_PARAMETER DOUBLE(64,15);
ALTER TABLE DS_DM_SQUID MODIFY REGULARIZATION DOUBLE(64,15);
ALTER TABLE DS_DM_SQUID MODIFY MIN_VALUE DOUBLE(64,15);
ALTER TABLE DS_DM_SQUID MODIFY MAX_VALUE DOUBLE(64,15);
ALTER TABLE DS_DM_SQUID MODIFY MIN_SUPPORT DOUBLE(64,15);
ALTER TABLE DS_DM_SQUID MODIFY MIN_CONFIDENCE DOUBLE(64,15);


/**修改ds_source_column自增字段的起始值(防止出现ds_column的id和source_column_id一样)**/
ALTER TABLE DS_SOURCE_COLUMN AUTO_INCREMENT=1000000;

insert into ds_squid_type(ID,CODE,DESCRIPTION) value (52,54,'GROUPTAGGING');

/**添加新的squid**/
insert into ds_squid_type(ID,CODE,DESCRIPTION) value (53,55,'HIVE');
insert into ds_squid_type(ID,CODE,DESCRIPTION) value (54,56,'HIVEEXTRACT');
/**data_squid添加新的column*/
ALTER TABLE DS_DATA_SQUID ADD GROUP_COLUMN LONGTEXT;
ALTER TABLE DS_DATA_SQUID ADD SORT_COLUMN LONGTEXT;
ALTER TABLE DS_DATA_SQUID ADD TAGGING_COLUMN LONGTEXT;

/**添加新的squid 2017-4-17**/
insert into ds_squid_type(ID,CODE,DESCRIPTION) value (55,57,'CASSANDRA');
insert into ds_squid_type(ID,CODE,DESCRIPTION) value (56,58,'CASSANDRAEXTRACT');
/**创建DS_CASSANDRA_SQL_CONNECTION 2017-4-17**/
DROP TABLE IF EXISTS `ds_cassandra_sql_connection`;
CREATE TABLE `ds_cassandra_sql_connection` (
  `ID` int(11) NOT NULL AUTO_INCREMENT,
  `DB_TYPE_ID` int(11) DEFAULT NULL,
  `HOST` varchar(100) DEFAULT NULL,
  `PORT` VARCHAR(10) DEFAULT NULL,
  `USERNAME` varchar(100) DEFAULT NULL,
  `PASSWORD` varchar(100) DEFAULT NULL,
  `KEYSPACE` varchar(500) DEFAULT NULL,
  `CLUSTER` varchar(500) DEFAULT NULL,
  `VERIFICATION_MODE` int(11) DEFAULT NULL,
  PRIMARY KEY (`ID`)
);
/**2017-4-17**/
update  ds_transformation_type set output_data_type=15 where id=97;


/**2017-4-21**/
insert into ds_transformation_type(ID,CODE,OUTPUT_DATA_TYPE,DESCRIPTION) value(99,'BINARYTOSTRING',9,'把一个二进制序列转换成字符串');
insert into ds_tran_input_definition(CODE,INPUT_ORDER,INPUT_DATA_TYPE,DESCRIPTION) value('BINARYTOSTRING',0,12,'需要被转化的二进制序列');
/**2017-4-24**/
DROP TABLE IF EXISTS `ds_userdefined_squid`;
CREATE TABLE `ds_userdefined_squid` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `selectClassName` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`)
);

DROP TABLE IF EXISTS `ds_userdefined_datamap_column`;
CREATE TABLE `ds_userdefined_datamap_column` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `squid_id` int(11) DEFAULT NULL,
  `name` varchar(255) DEFAULT NULL,
  `type` int(11) DEFAULT NULL,
  `column_id` int(11) DEFAULT NULL,
  `scale` int(11) DEFAULT NULL,
  `precision` int(11) DEFAULT NULL,
  `description` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`)
);

DROP TABLE IF EXISTS `DS_USERDEFINED_PARAMETERS_COLUMN`;
CREATE TABLE `DS_USERDEFINED_PARAMETERS_COLUMN` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `squid_id` int(11) DEFAULT NULL,
  `name` varchar(255) DEFAULT NULL,
  `value` VARCHAR(255) DEFAULT NULL,
  `description` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`)
);
alter TABLE DS_TRANSFORMATION ADD encoding int(2);
insert into ds_squid_type(ID,CODE,DESCRIPTION) value (57,59,'USERDEFINED');

-- 创建第三方jar包定义表
CREATE TABLE `third_jar_definition` (
  `id` INT(11) NOT NULL AUTO_INCREMENT,
  `alias_name` VARCHAR(100) COMMENT '别名',
  `class_name` VARCHAR(100) COMMENT '带有包名的完整类名',
  `data_mapping` VARCHAR(500) COMMENT '入参数据集定义',
  `parameter` VARCHAR(500) COMMENT '入参定义',
  `output_mapping` VARCHAR(500) COMMENT '出参数据集定义',
  `author` VARCHAR(50) COMMENT '作者',
  `create_date` DATETIME COMMENT '创建时间',
  `update_date` DATETIME COMMENT '更新时间',
  PRIMARY KEY (`id`)
);
CREATE INDEX data_mapping_column_index ON ds_userdefined_datamap_column (id ASC,squid_id);
CREATE INDEX parameter_column_index ON ds_userdefined_parameters_column (id ASC,squid_id);
CREATE INDEX userdefined_squid_index ON ds_userdefined_squid (id ASC);
CREATE INDEX third_jar_definition_index ON third_jar_definition (id ASC,alias_name,class_name);
/**2017-4-28*/
alter table third_jar_definition MODIFY alias_name VARCHAR(100) NOT NULL  UNIQUE ;
alter table third_jar_definition MODIFY class_name VARCHAR(100) NOT NULL ;
alter table third_jar_definition MODIFY data_mapping LONGTEXT;
alter table third_jar_definition MODIFY parameter LONGTEXT;
alter table third_jar_definition MODIFY output_mapping LONGTEXT;
alter table ds_userdefined_parameters_column MODIFY value LONGTEXT;
/**2017-5-3**/
insert into ds_squid_type(ID,CODE,DESCRIPTION) value (58,60,'DEST_HIVE');
DROP TABLE IF EXISTS `ds_dest_hive_squid`;
CREATE TABLE `ds_dest_hive_squid` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `save_type` int(11) DEFAULT NULL,
  `table_name` VARCHAR(100) DEFAULT NULL,
  `db_name` VARCHAR(100) DEFAULT NULL ,
  PRIMARY KEY (`id`)
);
DROP TABLE IF EXISTS `ds_dest_hive_column`;
CREATE TABLE `ds_dest_hive_column` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `column_id` int(11) NOT NULL,
  `squid_id` int(11) NOT NULL,
  `is_dest_column` int(11) DEFAULT NULL,
  `field_name` varchar(200) DEFAULT NULL,
  `column_order` int(11) DEFAULT NULL,
  `is_partition_column` int(11) DEFAULT NULL,
  `data_type` int(11) DEFAULT NULL ,
  `length` int(11) DEFAULT NULL ,
  `precision` int(11) DEFAULT NULL ,
  `scale` int(11) DEFAULT NULL ,
  PRIMARY KEY (`id`)
);
CREATE INDEX dest_hive_column_index ON ds_dest_hive_column (id ASC,squid_id,column_id);
CREATE INDEX dest_hive_squid_index ON ds_dest_hive_squid (id ASC);
/**2017-5-5**/
DROP TABLE IF EXISTS `ds_statistics_definition`;
CREATE TABLE `ds_statistics_definition` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `alias_name` varchar(100) DEFAULT NULL COMMENT '算法别名',
  `statistics_name` varchar(100) DEFAULT NULL COMMENT '算法名',
  `data_mapping` longtext,
  `parameter` longtext,
  `output_mapping` longtext,
  PRIMARY KEY (`id`)
);
DROP TABLE IF EXISTS `ds_statistics_squid`;
CREATE TABLE `ds_statistics_squid` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `statistics_name` varchar(100) DEFAULT NULL COMMENT '算法名',
  PRIMARY KEY (`id`)
);
DROP TABLE IF EXISTS `ds_statistics_datamap_column`;
CREATE TABLE `ds_statistics_datamap_column` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `squid_id` int(11) DEFAULT NULL,
  `name` varchar(255) DEFAULT NULL,
  `type` varchar(255) DEFAULT NULL,
  `column_id` int(11) DEFAULT NULL,
  `scale` int(11) DEFAULT NULL,
  `precision` int(11) DEFAULT NULL,
  `description` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`)
);
DROP TABLE IF EXISTS `ds_statistics_parameters_column`;
CREATE TABLE `ds_statistics_parameters_column` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `squid_id` int(11) DEFAULT NULL,
  `name` varchar(255) DEFAULT NULL,
  `value` varchar(255) DEFAULT NULL,
  `description` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`)
);
alter table ds_statistics_definition MODIFY statistics_name VARCHAR(100) NOT NULL UNIQUE ;
CREATE INDEX ds_statistics_datamap_column_index ON ds_statistics_datamap_column (id ASC,squid_id);
CREATE INDEX ds_statistics_parameters_column_index ON ds_statistics_parameters_column (id ASC,squid_id);
CREATE INDEX ds_statistics_squid_index ON ds_statistics_squid (id ASC);
CREATE INDEX ds_statistics_index ON ds_statistics_definition (id ASC,alias_name,statistics_name);
insert into ds_squid_type(ID,CODE,DESCRIPTION) value (59,61,'STATISTICS');


/**2017-5-10*/
alter table ds_data_squid add incremental_mode int;
alter table ds_data_squid add check_column_id int;
alter table ds_data_squid add last_value longtext;
/**2017-5-15**/
DROP TABLE IF EXISTS `ds_dest_cassandra_squid`;
CREATE TABLE `ds_dest_cassandra_squid` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `save_type` int(11) DEFAULT NULL,
  `table_name` varchar(100) DEFAULT NULL,
  `dest_squid_id` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`)
);
DROP TABLE IF EXISTS `ds_dest_cassandra_column`;
CREATE TABLE `ds_dest_cassandra_column` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `column_id` int(11) NOT NULL,
  `squid_id` int(11) NOT NULL,
  `is_dest_column` int(11) DEFAULT NULL,
  `field_name` varchar(200) DEFAULT NULL,
  `column_order` int(11) DEFAULT NULL,
  `is_primary_column` int(11) DEFAULT NULL,
  `data_type` int(11) DEFAULT NULL,
  `length` int(11) DEFAULT NULL,
  `precision` int(11) DEFAULT NULL,
  `scale` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`)
);
insert into ds_squid_type(ID,CODE,DESCRIPTION) value (60,62,'DEST_CASSANDRA');
CREATE INDEX dest_cassandra_column_index ON ds_dest_cassandra_column (id ASC,squid_id,column_id);
CREATE INDEX dest_cassandra_squid_index ON ds_dest_cassandra_squid (id ASC);
/**2015-5-23**/
alter table ds_statistics_parameters_column ADD type INT;
alter table ds_statistics_parameters_column ADD `precision` INT;
alter table ds_statistics_parameters_column ADD scale INT;

-- 增加统计算法： 主成分分析 PCA
INSERT INTO `ds_statistics_definition` VALUES ('1', null, 'Stddev', '[{\"description\":\"标准差入参\",\"name\":\"StddevInput\",\"precision\":38,\"scale\":38,\"type\":5}]', null, '[{\"description\":\"标准差\",\"name\":\"Stddev\",\"precision\":38,\"scale\":38,\"type\":5}]');
INSERT INTO `ds_statistics_definition` VALUES ('2', null, 'Kurtosis', '[{\"description\":\"峰度入参\",\"name\":\"KurtosisInput\",\"precision\":38,\"scale\":38,\"type\":5}]', null, '[{\"description\":\"峰度\",\"name\":\"Kurtosis\",\"precision\":38,\"scale\":38,\"type\":5}]');
INSERT INTO `ds_statistics_definition` VALUES ('3', null, 'Percentile', '[{\"description\":\"百分位数入参\",\"name\":\"PercentileInput\",\"precision\":38,\"scale\":38,\"type\":5}]', '[{\"description\":\"百分位\",\"name\":\"Quantile\",\"precision\":0,\"scale\":0,\"type\":6}]', '[{\"description\":\"百分位数\",\"name\":\"Percentil\",\"precision\":38,\"scale\":38,\"type\":5}]');
INSERT INTO `ds_statistics_definition` VALUES ('4', null, 'MovingAverage', '[{\"description\":\"移动平均入参\",\"name\":\"MovingAverageInput\",\"precision\":38,\"scale\":38,\"type\":5}]', '[{\"description\":\"移动步长\",\"name\":\"MoveStep\",\"precision\":0,\"scale\":0,\"type\":2}]', '[{\"description\":\"移动平均值\",\"name\":\"MovingAverage\",\"precision\":38,\"scale\":38,\"type\":5}]');
INSERT INTO `ds_statistics_definition` VALUES ('5', null, 'Correlation', '[{\"description\":\"相关系数入参1\",\"name\":\"CorrelationInput1\",\"precision\":38,\"scale\":38,\"type\":5},{\"description\":\"相关系数入参2\",\"name\":\"CorrelationInput2\",\"precision\":38,\"scale\":38,\"type\":5}]', '[{\"description\":\"计算相关系数方法\",\"name\":\"Method\",\"precision\":0,\"scale\":0,\"type\":9}]', '[{\"description\":\"相关系数\",\"name\":\"Correlation\",\"precision\":0,\"scale\":0,\"type\":6}]');
INSERT INTO `ds_statistics_definition` VALUES ('6', null, 'OnewayANOVA', '[{\"description\":\"单因素方差分析入参1\",\"name\":\"OneWayANOVAInput1\",\"precision\":38,\"scale\":38,\"type\":5},{\"description\":\"单因素方差分析入参2\",\"name\":\"OneWayANOVAInput2\",\"precision\":38,\"scale\":38,\"type\":5}]', '[{\"description\":\"水平\",\"name\":\"α\",\"precision\":0,\"scale\":0,\"type\":4}]', '[{\"description\":\"总误差平方和\",\"name\":\"SST\",\"precision\":38,\"scale\":38,\"type\":5},{\"description\":\"总自由度\",\"name\":\"dfT\",\"precision\":0,\"scale\":0,\"type\":2},{\"description\":\"组内误差平方和\",\"name\":\"SSE\",\"precision\":38,\"scale\":38,\"type\":5},{\"description\":\"组内自由度\",\"name\":\"dfE\",\"precision\":0,\"scale\":0,\"type\":2},{\"description\":\"组内误差均值\",\"name\":\"MSE\",\"precision\":38,\"scale\":38,\"type\":5},{\"description\":\"组间误差平方和\",\"name\":\"SSA\",\"precision\":38,\"scale\":38,\"type\":5},{\"description\":\"组间自由度\",\"name\":\"dfA\",\"precision\":0,\"scale\":0,\"type\":2},{\"description\":\"组间误差平均值\",\"name\":\"MSA\",\"precision\":38,\"scale\":38,\"type\":5},{\"description\":\"F比\",\"name\":\"F\",\"precision\":38,\"scale\":38,\"type\":5},{\"description\":\"显著性\",\"name\":\"pValue\",\"precision\":38,\"scale\":38,\"type\":5},{\"description\":\"F标准值\",\"name\":\"FCriterion\",\"precision\":38,\"scale\":38,\"type\":5}] ');
insert into ds_statistics_definition (`statistics_name`, `data_mapping`, `parameter`, `output_mapping`)
values ('PCA', '[{"description":"主成分分析入参1","name":"PCAInput1","precision":38,"scale":38,"type":5}]',
        '[{"description":"主成分数量，大于或等于1，且小于或等于PCAInput的列数","name":"k","precision":0,"scale":0,"type":4}]',
        '[{"description":"主成分","name":"PrincipalComponent","precision":0,"scale":0,"type":10,"length":-1},{"description":"解释方差","name":"ExplainedVariance","precision":0,"scale":0,"type":10,"length":-1}]');
-- 增加dataming
alter  table ds_dm_squid add max_integer_number int;
alter  table ds_dm_squid add aggregation_depth int;
alter  table ds_dm_squid add fit_intercept int;
alter  table ds_dm_squid add solver int;
alter  table ds_dm_squid add standardization int;
alter  table ds_dm_squid add tolerance double;
alter  table ds_dm_squid add tree_number int;
alter  table ds_dm_squid add feature_subset_strategy int;
alter  table ds_dm_squid add min_info_gain double;
alter  table ds_dm_squid add subsampling_rate double;
alter  table ds_dm_squid add initialweights LONGTEXT;
alter  table ds_dm_squid add layers LONGTEXT;
alter  table ds_dm_squid add max_categories int;
insert into ds_squid_type(ID,CODE,DESCRIPTION) value (61,63,'LASSO');
insert into ds_squid_type(ID,CODE,DESCRIPTION) value (62,64,'RANDOMFORESTCLASSIFIER');
insert into ds_squid_type(ID,CODE,DESCRIPTION) value (63,65,'RANDOMFORESTREGRESSION');
insert into ds_squid_type(ID,CODE,DESCRIPTION) value (64,66,'MULTILAYERPERCEPERONCLASSIFIER');

-- 2017-6-7
alter TABLE ds_column MODIFY name VARCHAR(255)  NOT NULL ;
alter table ds_reference_column MODIFY name VARCHAR(255) ;
-- 2017-6-9
alter table ds_dm_squid add feature_subset_scale DOUBLE ;
-- 2017-6-20
insert into ds_transformation_type(ID,CODE,OUTPUT_DATA_TYPE,DESCRIPTION) value(100,'CST',0,'类型转换');
insert into ds_tran_input_definition(CODE,INPUT_ORDER,INPUT_DATA_TYPE,DESCRIPTION) value('CST',1,21,'类型转换');

-- 2017-6-26
insert into ds_transformation_type(ID,CODE,OUTPUT_DATA_TYPE,DESCRIPTION) value(101,'INVERSENORMALIZER',10,'');
insert into ds_tran_input_definition(CODE,INPUT_ORDER,INPUT_DATA_TYPE,DESCRIPTION) value('INVERSENORMALIZER',1,10,'CSN中元素个数与训练时CSN中元素个数相同');



