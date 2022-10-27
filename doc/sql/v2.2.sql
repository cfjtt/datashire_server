/*
SQLyog Ultimate v12.09 (64 bit)
MySQL - 5.6.35-log : Database - datashire_database
*********************************************************************
*/

/*!40101 SET NAMES utf8 */;

/*!40101 SET SQL_MODE=''*/;

/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;
CREATE DATABASE /*!32312 IF NOT EXISTS*/datashire_database /*!40100 DEFAULT CHARACTER SET utf8 */;

USE datashire_database;

/*Table structure for table ds_column */

DROP TABLE IF EXISTS ds_column;

CREATE TABLE ds_column (
  id int(11) NOT NULL AUTO_INCREMENT,
  `key` varchar(50) DEFAULT NULL,
  RELATIVE_ORDER smallint(6) DEFAULT NULL,
  SQUID_ID int(11) NOT NULL,
  name varchar(255) NOT NULL,
  DATA_TYPE int(11) NOT NULL,
  COLLATION int(11) DEFAULT NULL,
  NULLABLE char(1) DEFAULT NULL,
  LENGTH int(11) DEFAULT NULL,
  `precision` int(11) DEFAULT NULL,
  SCALE int(11) DEFAULT NULL,
  IS_GROUPBY char(1) DEFAULT NULL,
  AGGREGATION_TYPE int(11) DEFAULT NULL,
  DESCRIPTION varchar(500) DEFAULT NULL,
  ISUNIQUE char(1) DEFAULT NULL,
  ISPK char(1) DEFAULT NULL,
  CDC int(11) DEFAULT NULL,
  IS_BUSINESS_KEY int(11) DEFAULT NULL,
  SORT_LEVEL int(11) DEFAULT NULL,
  SORT_TYPE int(11) DEFAULT NULL,
  PRIMARY KEY (ID),
  KEY SYS_IDX_SYS_CT_10265_10271 (name),
  KEY IDX_COLUMN_SQUID_ID (SQUID_ID),
  KEY IDX_KEY (`key`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

/*Data for the table ds_column */

/*Table structure for table ds_dest_cassandra_column */

DROP TABLE IF EXISTS ds_dest_cassandra_column;

CREATE TABLE ds_dest_cassandra_column (
  id int(11) NOT NULL AUTO_INCREMENT,
  column_id int(11) NOT NULL,
  squid_id int(11) NOT NULL,
  is_dest_column int(11) DEFAULT NULL,
  field_name varchar(200) DEFAULT NULL,
  column_order int(11) DEFAULT NULL,
  is_primary_column int(11) DEFAULT NULL,
  data_type int(11) DEFAULT NULL,
  length int(11) DEFAULT NULL,
  `precision` int(11) DEFAULT NULL,
  scale int(11) DEFAULT NULL,
  PRIMARY KEY (id),
  KEY dest_cassandra_column_index (id,squid_id,column_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

/*Data for the table ds_dest_cassandra_column */

/*Table structure for table ds_dest_hdfs_column */

DROP TABLE IF EXISTS ds_dest_hdfs_column;

CREATE TABLE ds_dest_hdfs_column (
  ID int(11) NOT NULL AUTO_INCREMENT,
  COLUMN_ID int(11) NOT NULL,
  SQUID_ID int(11) NOT NULL,
  IS_DEST_COLUMN int(11) DEFAULT NULL,
  FIELD_NAME varchar(200) DEFAULT NULL,
  COLUMN_ORDER int(11) DEFAULT NULL,
  IS_PARTITION_COLUMN int(11) DEFAULT NULL,
  PRIMARY KEY (ID)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

/*Data for the table ds_dest_hdfs_column */

/*Table structure for table ds_dest_hive_column */

DROP TABLE IF EXISTS ds_dest_hive_column;

CREATE TABLE ds_dest_hive_column (
  id int(11) NOT NULL AUTO_INCREMENT,
  column_id int(11) NOT NULL,
  squid_id int(11) NOT NULL,
  is_dest_column int(11) DEFAULT NULL,
  field_name varchar(200) DEFAULT NULL,
  column_order int(11) DEFAULT NULL,
  is_partition_column int(11) DEFAULT NULL,
  data_type int(11) DEFAULT NULL,
  length int(11) DEFAULT NULL,
  `precision` int(11) DEFAULT NULL,
  scale int(11) DEFAULT NULL,
  PRIMARY KEY (id),
  KEY dest_hive_column_index (id,squid_id,column_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

/*Data for the table ds_dest_hive_column */

/*Table structure for table ds_dest_impala_column */

DROP TABLE IF EXISTS ds_dest_impala_column;

CREATE TABLE ds_dest_impala_column (
  ID int(11) NOT NULL AUTO_INCREMENT,
  COLUMN_ID int(11) NOT NULL,
  SQUID_ID int(11) NOT NULL,
  IS_DEST_COLUMN int(11) DEFAULT NULL,
  FIELD_NAME varchar(200) DEFAULT NULL,
  COLUMN_ORDER int(11) DEFAULT NULL,
  PRIMARY KEY (ID)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

/*Data for the table ds_dest_impala_column */

/*Table structure for table ds_dest_impala_squid */

DROP TABLE IF EXISTS ds_dest_impala_squid;

CREATE TABLE ds_dest_impala_squid (
  ID int(11) NOT NULL AUTO_INCREMENT,
  HOST varchar(50) DEFAULT NULL,
  STORE_NAME varchar(50) DEFAULT NULL,
  IMPALA_TABLE_NAME varchar(50) DEFAULT NULL,
  AUTHENTICATION_TYPE int(11) DEFAULT NULL,
  PRIMARY KEY (ID)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

/*Data for the table ds_dest_impala_squid */

/*Table structure for table ds_dest_webservice */

DROP TABLE IF EXISTS ds_dest_webservice;

CREATE TABLE ds_dest_webservice (
  ID int(11) NOT NULL AUTO_INCREMENT,
  SERVICE_NAME varchar(100) DEFAULT NULL,
  WSDL varchar(200) DEFAULT NULL,
  IS_REALTIME char(1) DEFAULT NULL,
  CALLBACK_URL varchar(200) DEFAULT NULL,
  ALLOWED_SERVICES varchar(200) DEFAULT NULL,
  PRIMARY KEY (ID)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

/*Data for the table ds_dest_webservice */

/*Table structure for table ds_es_column */

DROP TABLE IF EXISTS ds_es_column;

CREATE TABLE ds_es_column (
  ID int(11) NOT NULL AUTO_INCREMENT,
  COLUMN_ID int(11) NOT NULL,
  SQUID_ID int(11) NOT NULL,
  IS_MAPPING_ID int(11) DEFAULT NULL,
  FIELD_NAME varchar(200) DEFAULT NULL,
  IS_PERSIST int(11) DEFAULT NULL,
  PRIMARY KEY (ID)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

/*Data for the table ds_es_column */

/*Table structure for table ds_gis_map_squid */

DROP TABLE IF EXISTS ds_gis_map_squid;

CREATE TABLE ds_gis_map_squid (
  ID int(11) NOT NULL AUTO_INCREMENT,
  MAP_NAME varchar(50) DEFAULT NULL,
  MAP_TEMPLATE text,
  IS_SUPPORT_HISTORY tinyint(1) DEFAULT NULL,
  MAX_HISTORY_COUNT int(11) DEFAULT NULL,
  IS_SEND_EMAIL tinyint(1) DEFAULT NULL,
  EMAIL_RECEIVERS varchar(500) DEFAULT NULL,
  EMAIL_TITLE varchar(200) DEFAULT NULL,
  EMAIL_REPORT_FORMAT varchar(10) DEFAULT NULL,
  CREATION_DATE timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  IS_REAL_TIME tinyint(1) DEFAULT NULL,
  FOLDER_ID int(11) DEFAULT NULL,
  IS_PACKED tinyint(1) DEFAULT NULL,
  IS_COMPRESSED tinyint(1) DEFAULT NULL,
  IS_ENCRYPTED tinyint(1) DEFAULT NULL,
  PASSWORD varchar(10) DEFAULT NULL,
  PRIMARY KEY (ID)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

/*Data for the table ds_gis_map_squid */

/*Table structure for table ds_http_connection */

DROP TABLE IF EXISTS ds_http_connection;

CREATE TABLE ds_http_connection (
  ID int(11) NOT NULL AUTO_INCREMENT,
  HOST varchar(30) DEFAULT NULL,
  PORT int(11) DEFAULT NULL,
  PRIMARY KEY (ID)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

/*Data for the table ds_http_connection */

/*Table structure for table ds_indexes */

DROP TABLE IF EXISTS ds_indexes;

CREATE TABLE ds_indexes (
  ID int(11) NOT NULL AUTO_INCREMENT,
  SQUID_ID int(11) NOT NULL,
  INDEX_NAME varchar(100) NOT NULL,
  INDEX_TYPE int(11) DEFAULT NULL,
  COLUMN_ID10 int(11) DEFAULT NULL,
  COLUMN_ID9 int(11) DEFAULT NULL,
  COLUMN_ID8 int(11) DEFAULT NULL,
  COLUMN_ID7 int(11) DEFAULT NULL,
  COLUMN_ID6 int(11) DEFAULT NULL,
  COLUMN_ID5 int(11) DEFAULT NULL,
  COLUMN_ID4 int(11) DEFAULT NULL,
  COLUMN_ID3 int(11) DEFAULT NULL,
  COLUMN_ID2 int(11) DEFAULT NULL,
  COLUMN_ID1 int(11) DEFAULT NULL,
  PRIMARY KEY (ID),
  UNIQUE KEY SYS_IDX_SYS_CT_10296_10301 (INDEX_NAME,SQUID_ID),
  KEY SYS_IDX_10607 (SQUID_ID)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

/*Data for the table ds_indexes */

/*Table structure for table ds_join */

DROP TABLE IF EXISTS ds_join;

CREATE TABLE ds_join (
  ID int(11) NOT NULL AUTO_INCREMENT,
  `key` varchar(50) NOT NULL,
  TARGET_SQUID_ID int(11) NOT NULL,
  JOINED_SQUID_ID int(11) NOT NULL,
  PRIOR_JOIN_ID int(11) NOT NULL,
  JOIN_TYPE_ID int(11) NOT NULL,
  JOIN_CONDITION varchar(500) DEFAULT NULL,
  PRIMARY KEY (ID),
  KEY IDX_JOIN_JOINED_ID (JOINED_SQUID_ID),
  KEY IDX_JOIN_TARGET_ID (TARGET_SQUID_ID),
  KEY IDX_JOIN_TYPE_ID (JOIN_TYPE_ID)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

/*Data for the table ds_join */

/*Table structure for table ds_join_type */

DROP TABLE IF EXISTS ds_join_type;

CREATE TABLE ds_join_type (
  ID int(11) NOT NULL,
  CODE varchar(50) DEFAULT NULL,
  DESCRIPTION varchar(500) DEFAULT NULL,
  PRIMARY KEY (ID)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

/*Data for the table ds_join_type */

insert  into ds_join_type(ID,CODE,DESCRIPTION) values (0,'0','BASETABLE'),(1,'1','INNERJOIN'),(2,'2','LEFTOUTERJOIN'),(3,'3','RIGHTOUTERJOIN'),(4,'4','FULLJOIN'),(5,'5','CROSSJOIN'),(6,'6','UNOIN'),(7,'7','UNOINALL');

/*Table structure for table ds_project */

DROP TABLE IF EXISTS ds_project;

CREATE TABLE ds_project (
  ID int(11) NOT NULL AUTO_INCREMENT,
  `key` varchar(50) DEFAULT NULL,
  NAME varchar(50) DEFAULT NULL,
  REPOSITORY_ID int(11) NOT NULL,
  DESCRIPTION varchar(200) DEFAULT NULL,
  CREATION_DATE timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  MODIFICATION_DATE datetime DEFAULT NULL,
  CREATOR varchar(50) DEFAULT NULL,
  PRIMARY KEY (ID),
  UNIQUE KEY SYS_IDX_SYS_CT_10323_10327 (NAME,REPOSITORY_ID)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

/*Data for the table ds_project */

/*Table structure for table ds_reference_column */

DROP TABLE IF EXISTS ds_reference_column;

CREATE TABLE ds_reference_column (
  COLUMN_ID int(11) NOT NULL,
  RELATIVE_ORDER smallint(6) DEFAULT NULL,
  REFERENCE_SQUID_ID int(11) NOT NULL,
  name varchar(255) DEFAULT NULL,
  DATA_TYPE int(11) DEFAULT NULL,
  COLLATION int(11) DEFAULT NULL,
  NULLABLE char(1) DEFAULT NULL,
  LENGTH int(11) DEFAULT NULL,
  `precision` int(11) DEFAULT NULL,
  SCALE int(11) DEFAULT NULL,
  DESCRIPTION varchar(500) DEFAULT NULL,
  ISUNIQUE char(1) DEFAULT NULL,
  ISPK char(1) DEFAULT NULL,
  CDC int(11) DEFAULT NULL,
  IS_BUSINESS_KEY int(11) DEFAULT NULL,
  HOST_SQUID_ID int(11) DEFAULT NULL,
  IS_REFERENCED char(1) DEFAULT NULL,
  GROUP_ID int(11) DEFAULT NULL,
  KEY IDX_REFCOLUMN (COLUMN_ID),
  KEY IDX_REFCOLUMN_GROUP_ID (GROUP_ID),
  KEY IDX_REFCOLUMN_HOST_ID (HOST_SQUID_ID),
  KEY IDX_REFERENCE_COLUMN (REFERENCE_SQUID_ID)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

/*Data for the table ds_reference_column */

/*Table structure for table ds_reference_column_group */

DROP TABLE IF EXISTS ds_reference_column_group;

CREATE TABLE ds_reference_column_group (
  ID int(11) NOT NULL AUTO_INCREMENT,
  REFERENCE_SQUID_ID int(11) NOT NULL,
  RELATIVE_ORDER smallint(6) DEFAULT NULL,
  PRIMARY KEY (ID),
  KEY IDX_GROUP_SQUID_ID (REFERENCE_SQUID_ID)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

/*Data for the table ds_reference_column_group */

/*Table structure for table ds_report_squid */

DROP TABLE IF EXISTS ds_report_squid;

CREATE TABLE ds_report_squid (
  ID int(11) NOT NULL AUTO_INCREMENT,
  REPORT_NAME varchar(50) DEFAULT NULL,
  REPORT_TEMPLATE text,
  IS_SUPPORT_HISTORY tinyint(1) DEFAULT NULL,
  MAX_HISTORY_COUNT int(11) DEFAULT NULL,
  IS_SEND_EMAIL tinyint(1) DEFAULT NULL,
  EMAIL_RECEIVERS varchar(500) DEFAULT NULL,
  EMAIL_TITLE varchar(200) DEFAULT NULL,
  EMAIL_REPORT_FORMAT varchar(10) DEFAULT NULL,
  CREATION_DATE timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  IS_REAL_TIME tinyint(1) DEFAULT NULL,
  FOLDER_ID int(11) DEFAULT NULL,
  IS_PACKED tinyint(1) DEFAULT NULL,
  IS_COMPRESSED tinyint(1) DEFAULT NULL,
  IS_ENCRYPTED tinyint(1) DEFAULT NULL,
  PASSWORD varchar(10) DEFAULT NULL,
  PRIMARY KEY (ID)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

/*Data for the table ds_report_squid */

/*Table structure for table ds_report_version */

DROP TABLE IF EXISTS ds_report_version;

CREATE TABLE ds_report_version (
  ID int(11) NOT NULL AUTO_INCREMENT,
  SQUID_ID int(11) NOT NULL,
  VERSION int(11) DEFAULT NULL,
  TEMPLATE text,
  ADD_DATE timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (ID)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

/*Data for the table ds_report_version */

/*Table structure for table ds_source_column */

DROP TABLE IF EXISTS ds_source_column;

CREATE TABLE ds_source_column (
  ID int(11) NOT NULL AUTO_INCREMENT,
  SOURCE_TABLE_ID int(11) NOT NULL,
  NAME varchar(128) NOT NULL,
  DATA_TYPE int(11) NOT NULL,
  NULLABLE char(1) DEFAULT NULL,
  LENGTH int(11) DEFAULT NULL,
  `precision` int(11) DEFAULT NULL,
  SCALE int(11) DEFAULT NULL,
  RELATIVE_ORDER int(11) DEFAULT NULL,
  ISUNIQUE char(1) DEFAULT NULL,
  ISPK char(1) DEFAULT NULL,
  COLLATION int(11) DEFAULT NULL,
  PRIMARY KEY (ID),
  UNIQUE KEY SYS_IDX_SYS_CT_10350_10356 (SOURCE_TABLE_ID,NAME),
  KEY IDX_SROUCE_COLUMN_NAME (NAME),
  KEY IDX_SROUCE_COLUMN_TABLE_ID (SOURCE_TABLE_ID),
  CONSTRAINT FK_SOURCE_TABLE_ID FOREIGN KEY (SOURCE_TABLE_ID) REFERENCES ds_source_table (ID) ON DELETE CASCADE ON UPDATE NO ACTION
) ENGINE=InnoDB AUTO_INCREMENT=1000000 DEFAULT CHARSET=utf8;

/*Data for the table ds_source_column */

/*Table structure for table ds_source_table */

DROP TABLE IF EXISTS ds_source_table;

CREATE TABLE ds_source_table (
  ID int(11) NOT NULL AUTO_INCREMENT,
  TABLE_NAME varchar(300) NOT NULL,
  SOURCE_SQUID_ID int(11) NOT NULL,
  IS_EXTRACTED char(1) DEFAULT NULL,
  RELATIVE_ORDER smallint(6) DEFAULT NULL,
  URL varchar(300) DEFAULT NULL,
  URL_PARAMS varchar(300) DEFAULT NULL,
  HEADER_PARAMS varchar(3000) DEFAULT NULL,
  METHOD int(11) DEFAULT NULL,
  IS_DIRECTORY tinyint(1) DEFAULT '0',
  PRIMARY KEY (ID),
  KEY IDX_SROUCE_TABLE_SQUID_ID (SOURCE_SQUID_ID)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

/*Data for the table ds_source_table */

/*Table structure for table ds_squid */

DROP TABLE IF EXISTS ds_squid;

CREATE TABLE ds_squid (
  ID int(11) NOT NULL AUTO_INCREMENT,
  SQUID_FLOW_ID int(11) NOT NULL,
  NAME varchar(300) NOT NULL,
  DESCRIPTION varchar(300) DEFAULT NULL,
  SQUID_TYPE_ID int(11) NOT NULL,
  LOCATION_X int(11) NOT NULL,
  LOCATION_Y int(11) NOT NULL,
  SQUID_HEIGHT int(11) NOT NULL,
  SQUID_WIDTH int(11) NOT NULL,
  TABLE_NAME varchar(300) DEFAULT NULL,
  IS_SHOW_ALL char(1) DEFAULT NULL,
  SOURCE_IS_SHOW_ALL char(1) DEFAULT NULL,
  COLUMN_GROUP_X int(11) DEFAULT NULL,
  COLUMN_GROUP_Y int(11) DEFAULT NULL,
  REFERENCE_GROUP_X int(11) DEFAULT NULL,
  REFERENCE_GROUP_Y int(11) DEFAULT NULL,
  FILTER varchar(2000) DEFAULT NULL,
  ENCODING int(11) DEFAULT NULL,
  DESIGN_STATUS int(11) DEFAULT NULL,
  MAX_TRAVEL_DEPTH int(11) DEFAULT NULL,
  CONTENT varchar(4000) DEFAULT NULL,
  CONTENT_FONT_SIZE int(11) DEFAULT NULL,
  CONTENT_FONT_COLOR varchar(100) DEFAULT NULL,
  CONTENT_FONT_FAMILY varchar(100) DEFAULT NULL,
  HORIZONTAL_CONTENT_ALIGNMENT int(11) DEFAULT NULL,
  VERTICAL_CONTENT_ALIGNMENT int(11) DEFAULT NULL,
  IS_CONTENT_BOLD char(1) DEFAULT NULL,
  IS_CONTENT_ITALIC char(1) DEFAULT NULL,
  IS_CONTENT_UNDERLINE char(1) DEFAULT NULL,
  DB_TYPE_ID int(11) DEFAULT NULL,
  HOST varchar(100) DEFAULT NULL,
  PORT varchar(10) DEFAULT NULL,
  USERNAME varchar(100) DEFAULT NULL,
  PASSWORD varchar(100) DEFAULT NULL,
  KEYSPACE varchar(500) DEFAULT NULL,
  CLUSTER varchar(500) DEFAULT NULL,
  VERIFICATION_MODE int(11) DEFAULT NULL,
  IS_INCREMENTAL char(1) DEFAULT NULL,
  INCREMENTAL_EXPRESSION varchar(4000) DEFAULT NULL,
  IS_PERSISTED char(1) DEFAULT NULL,
  DESTINATION_SQUID_ID int(11) DEFAULT NULL,
  IS_INDEXED char(1) DEFAULT NULL,
  TOP_N int(11) DEFAULT NULL,
  TRUNCATE_EXISTING_DATA_FLAG int(11) DEFAULT NULL,
  PROCESS_MODE int(2) DEFAULT NULL,
  CDC int(2) DEFAULT NULL,
  EXCEPTION_HANDLING_FLAG int(2) DEFAULT NULL,
  LOG_FORMAT int(2) DEFAULT NULL,
  POST_PROCESS int(2) DEFAULT NULL,
  XSD_DTD_FILE varchar(100) DEFAULT NULL,
  XSD_DTD_PATH varchar(200) DEFAULT NULL,
  SOURCE_TABLE_ID int(11) DEFAULT NULL,
  UNION_ALL_FLAG int(2) DEFAULT NULL,
  IS_DISTINCT int(2) DEFAULT NULL,
  REF_SQUID_ID int(11) DEFAULT NULL,
  SPLIT_COL varchar(128) DEFAULT NULL,
  COLSPLIT_NUMUMN int(11) DEFAULT NULL,
  SPLIT_NUM int(11) DEFAULT NULL,
  WINDOW_DURATION bigint(20) DEFAULT NULL,
  ENABLE_WINDOW char(1) DEFAULT NULL,
  GROUP_COLUMN longtext,
  SORT_COLUMN longtext,
  TAGGING_COLUMN longtext,
  incremental_mode int(2) DEFAULT NULL,
  check_column_id int(11) DEFAULT NULL,
  last_value longtext,
  save_type int(2) DEFAULT NULL,
  dest_squid_id int(11) DEFAULT NULL,
  ip varchar(20) DEFAULT NULL,
  ESINDEX varchar(200) DEFAULT NULL,
  ESTYPE varchar(200) DEFAULT NULL,
  HDFS_PATH varchar(200) DEFAULT NULL,
  FILE_FORMATE int(2) DEFAULT NULL,
  ZIP_TYPE int(2) DEFAULT NULL,
  ROW_DELIMITER varchar(500) DEFAULT NULL,
  COLUMN_DELIMITER varchar(30) DEFAULT NULL,
  db_name varchar(100) DEFAULT NULL,
  STORE_NAME varchar(50) DEFAULT NULL,
  IMPALA_TABLE_NAME varchar(50) DEFAULT NULL,
  AUTHENTICATION_TYPE int(2) DEFAULT NULL,
  TRAINING_PERCENTAGE double(64,15) DEFAULT NULL,
  VERSIONING int(2) DEFAULT NULL,
  MIN_BATCH_FRACTION double(64,15) DEFAULT NULL,
  ITERATION_NUMBER int(2) DEFAULT NULL,
  STEP_SIZE double(64,15) DEFAULT NULL,
  SMOOTHING_PARAMETER double(64,15) DEFAULT NULL,
  REGULARIZATION double(64,15) DEFAULT NULL,
  k int(11) DEFAULT NULL,
  parallel_runs int(11) DEFAULT NULL,
  INITIALIZATION_MODE int(2) DEFAULT NULL,
  implicit_preferences int(2) DEFAULT NULL,
  case_sensitive int(2) DEFAULT NULL,
  MIN_VALUE double(64,15) DEFAULT NULL,
  MAX_VALUE double(64,15) DEFAULT NULL,
  bucket_count int(11) DEFAULT NULL,
  SEED int(11) DEFAULT NULL,
  ALGORITHM int(11) DEFAULT NULL,
  MAX_DEPTH int(11) DEFAULT NULL,
  IMPURITY int(11) DEFAULT NULL,
  MAX_BINS int(11) DEFAULT NULL,
  CATEGORICAL_SQUID int(11) DEFAULT NULL,
  MIN_SUPPORT double(64,15) DEFAULT NULL,
  MIN_CONFIDENCE double(64,15) DEFAULT NULL,
  max_integer_number int(11) DEFAULT NULL,
  aggregation_depth int(11) DEFAULT NULL,
  fit_intercept int(11) DEFAULT NULL,
  solver int(11) DEFAULT NULL,
  standardization int(11) DEFAULT NULL,
  tolerance double DEFAULT NULL,
  tree_number int(11) DEFAULT NULL,
  feature_subset_strategy int(11) DEFAULT NULL,
  min_info_gain double DEFAULT NULL,
  subsampling_rate double DEFAULT NULL,
  initialweights longtext,
  layers longtext,
  max_categories longtext,
  feature_subset_scale double DEFAULT NULL,
  DOC_FORMAT int(2) DEFAULT NULL,
  ROW_FORMAT int(2) DEFAULT NULL,
  DELIMITER varchar(300) DEFAULT NULL,
  FIELD_LENGTH int(11) DEFAULT NULL,
  HEADER_ROW_NO int(11) DEFAULT NULL,
  FIRST_DATA_ROW_NO int(11) DEFAULT NULL,
  ROW_DELIMITER_POSITION int(11) DEFAULT NULL,
  SKIP_ROWS int(11) DEFAULT NULL,
  COMPRESSICON_CODEC int(11) DEFAULT NULL,
  USER_NAME varchar(100) DEFAULT NULL,
  FILE_PATH varchar(500) DEFAULT NULL,
  INCLUDING_SUBFOLDERS_FLAG int(2) DEFAULT NULL,
  UNIONALL_FLAG int(2) DEFAULT NULL,
  POSTPROCESS int(11) DEFAULT NULL,
  PROTOCOL int(11) DEFAULT NULL,
  ENCRYPTION int(11) DEFAULT NULL,
  ALLOWANONYMOUS_FLAG int(11) DEFAULT NULL,
  MAXCONNECTIONS int(11) DEFAULT NULL,
  TRANSFERMODE_FLAG int(11) DEFAULT NULL,
  URL varchar(200) DEFAULT NULL,
  FILTER_TYPE int(11) DEFAULT NULL,
  SCAN text,
  CODE text,
  ZKQUORUM varchar(200) DEFAULT NULL,
  NUMPARTITIONS int(11) DEFAULT NULL,
  GROUP_ID varchar(100) DEFAULT NULL,
  DATABASE_NAME varchar(100) DEFAULT NULL,
  statistics_name varchar(100) DEFAULT NULL,
  selectClassName varchar(255) DEFAULT NULL,
  x_model_squid_id int(11) DEFAULT NULL,
  y_model_squid_id int(11) DEFAULT NULL,
  method int(11) DEFAULT NULL,
  elastic_net_param double DEFAULT NULL,
  min_instances_per_node int(11) DEFAULT NULL,
  family int(11) DEFAULT NULL,
  threshold varchar(100) DEFAULT NULL,
  PRIMARY KEY (ID),
  KEY IDX_SQUID_FLOW_ID (SQUID_FLOW_ID),
  KEY IDX_SQUID_TYPE_ID (SQUID_TYPE_ID)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

/*Data for the table ds_squid */

/*Table structure for table ds_squid_flow */

DROP TABLE IF EXISTS ds_squid_flow;

CREATE TABLE ds_squid_flow (
  ID int(11) NOT NULL AUTO_INCREMENT,
  `key` varchar(50) NOT NULL,
  NAME varchar(50) DEFAULT NULL,
  CREATION_DATE timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  MODIFICATION_DATE datetime DEFAULT NULL,
  CREATOR varchar(50) DEFAULT NULL,
  PROJECT_ID int(11) NOT NULL,
  DESCRIPTION varchar(200) DEFAULT NULL,
  COMPILATION_STATUS int(11) DEFAULT NULL,
  SQUIDFLOW_TYPE int(11) DEFAULT '0',
  FIELD_TYPE int(5) DEFAULT NULL,
  PRIMARY KEY (ID),
  UNIQUE KEY SYS_IDX_SYS_CT_10405_10410 (NAME,PROJECT_ID),
  KEY IDX_SQUIDFLOW_PROJECT_ID (PROJECT_ID)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

/*Data for the table ds_squid_flow */

/*Table structure for table ds_squid_link */

DROP TABLE IF EXISTS ds_squid_link;

CREATE TABLE ds_squid_link (
  ID int(11) NOT NULL AUTO_INCREMENT,
  `key` varchar(50) DEFAULT NULL,
  SQUID_FLOW_ID int(11) DEFAULT NULL,
  FROM_SQUID_ID int(11) NOT NULL,
  TO_SQUID_ID int(11) NOT NULL,
  END_X int(11) DEFAULT NULL,
  ARROWS_STYLE int(11) DEFAULT NULL,
  END_Y int(11) DEFAULT NULL,
  ENDMIDDLE_X int(11) DEFAULT NULL,
  ENDMIDDLE_Y int(11) DEFAULT NULL,
  START_X int(11) DEFAULT NULL,
  START_Y int(11) DEFAULT NULL,
  STARTMIDDLE_X int(11) DEFAULT NULL,
  STARTMIDDLE_Y int(11) DEFAULT NULL,
  LINE_COLOR varchar(50) DEFAULT NULL,
  LINE_TYPE int(11) DEFAULT NULL,
  LINK_TYPE int(11) DEFAULT NULL,
  PRIMARY KEY (ID),
  UNIQUE KEY SYS_IDX_SYS_CT_10417_10422 (FROM_SQUID_ID,TO_SQUID_ID),
  KEY IDX_SQUID_LINK_FROM_ID (FROM_SQUID_ID),
  KEY IDX_SQUID_LINK_TO_ID (TO_SQUID_ID)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

/*Data for the table ds_squid_link */

/*Table structure for table ds_squid_type */

DROP TABLE IF EXISTS ds_squid_type;

CREATE TABLE ds_squid_type (
  ID int(11) NOT NULL,
  CODE int(30) DEFAULT NULL,
  DESCRIPTION varchar(250) DEFAULT NULL,
  PRIMARY KEY (ID),
  UNIQUE KEY SYS_IDX_SYS_CT_10427_10430 (CODE)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

/*Data for the table ds_squid_type */

insert  into ds_squid_type(ID,CODE,DESCRIPTION) values (-1,-1,'UNKNOWN'),(0,0,'DBSOURCE'),(1,1,'DBDESTINATION'),(2,2,'EXTRACT'),(3,3,'STAGE'),(4,4,'DIMENSION'),(5,5,'FACT'),(6,6,'REPORT'),(7,7,'DOC_EXTRACT'),(8,8,'XML_EXTRACT'),(9,9,'WEBLOGEXTRACT'),(10,10,'WEBEXTRACT'),(11,11,'WEIBOEXTRACT'),(12,12,'FILEFOLDER'),(13,13,'FTP'),(14,14,'HDFS'),(15,15,'WEB'),(17,17,'WEIBO'),(20,20,'EXCEPTION'),(21,21,'LOGREG'),(22,22,'NAIVEBAYES'),(23,23,'SVM'),(24,24,'KMEANS'),(25,25,'ALS'),(26,26,'LINEREG'),(27,27,'RIDGEREG'),(28,28,'QUANTIFY'),(29,29,'DISCRETIZE'),(30,30,'DECISIONTREE'),(31,31,'GISMAP'),(32,32,'HTTP'),(33,33,'WEBSERVICE'),(34,34,'HTTPEXTRACT'),(35,35,'WEBSERVICEEXTRACT'),(36,36,'DESTWS'),(37,37,'ANNOTATION'),(38,38,'MONGODB'),(39,39,'MONGODBEXTRACT'),(40,40,'DESTES'),(41,41,'KAFKA'),(42,42,'KAFKAEXTRACT'),(43,43,'HBASE'),(44,44,'HBASEEXTRACT'),(45,45,'STREAM_STAGE'),(46,46,'DEST_HDFS'),(47,47,'DEST_IMPALA'),(48,48,'ASSOCIATION_RULES'),(49,51,'SOURCECLOUDFILE'),(50,52,'CLOUDDB'),(51,53,'DESTCLOUDFILE'),(52,54,'GROUPTAGGING'),(53,55,'HIVE'),(54,56,'HIVEEXTRACT'),(55,57,'CASSANDRA'),(56,58,'CASSANDRAEXTRACT'),(57,59,'USERDEFINED'),(58,60,'DEST_HIVE'),(59,61,'STATISTICS'),(60,62,'DEST_CASSANDRA'),(61,63,'LASSO'),(62,64,'RANDOMFORESTCLASSIFIER'),(63,65,'RANDOMFORESTREGRESSION'),(64,66,'MULTILAYERPERCEPERONCLASSIFIER'),(65,67,'NORMALIZER'),(66,68,'PLS'),(67,69,'DATAVIEW'),(68,70,'COEFFICIENT'),(69,71,'DECISIONTREEREGRESSION'),(70,72,'DECISIONTREECLASSIFICATION');

/*Table structure for table ds_start_squid_flow_log */

DROP TABLE IF EXISTS ds_start_squid_flow_log;

CREATE TABLE ds_start_squid_flow_log (
  ID int(11) NOT NULL AUTO_INCREMENT,
  TASK_ID varchar(200) DEFAULT NULL,
  CALL_BACK_URL int(11) DEFAULT NULL,
  CREATE_DATE timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  CALL_BACK_DATE datetime DEFAULT NULL,
  PRIMARY KEY (ID)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

/*Data for the table ds_start_squid_flow_log */

/*Table structure for table ds_statistics_datamap_column */

DROP TABLE IF EXISTS ds_statistics_datamap_column;

CREATE TABLE ds_statistics_datamap_column (
  id int(11) NOT NULL AUTO_INCREMENT,
  squid_id int(11) DEFAULT NULL,
  name varchar(255) DEFAULT NULL,
  type varchar(255) DEFAULT NULL,
  column_id int(11) DEFAULT NULL,
  scale int(11) DEFAULT NULL,
  `precision` int(11) DEFAULT NULL,
  description varchar(255) DEFAULT NULL,
  PRIMARY KEY (id),
  KEY ds_statistics_datamap_column_index (id,squid_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

/*Data for the table ds_statistics_datamap_column */

/*Table structure for table ds_statistics_definition */

DROP TABLE IF EXISTS ds_statistics_definition;

CREATE TABLE ds_statistics_definition (
  id int(11) NOT NULL AUTO_INCREMENT,
  alias_name varchar(100) DEFAULT NULL COMMENT '算法别名',
  statistics_name varchar(100) NOT NULL,
  data_mapping longtext,
  parameter longtext,
  output_mapping longtext,
  PRIMARY KEY (id),
  UNIQUE KEY statistics_name (statistics_name),
  KEY ds_statistics_index (id,alias_name,statistics_name)
) ENGINE=InnoDB AUTO_INCREMENT=8 DEFAULT CHARSET=utf8;

/*Data for the table ds_statistics_definition */

insert  into ds_statistics_definition(id,alias_name,statistics_name,data_mapping,parameter,output_mapping) values (1,NULL,'Stddev','[{\"description\":\"标准差入参\",\"name\":\"StddevInput\",\"precision\":38,\"scale\":38,\"type\":5}]',NULL,'[{\"description\":\"标准差\",\"name\":\"Stddev\",\"precision\":38,\"scale\":38,\"type\":5}]'),(2,NULL,'Kurtosis','[{\"description\":\"峰度入参\",\"name\":\"KurtosisInput\",\"precision\":38,\"scale\":38,\"type\":5}]',NULL,'[{\"description\":\"峰度\",\"name\":\"Kurtosis\",\"precision\":38,\"scale\":38,\"type\":5}]'),(3,NULL,'Percentile','[{\"description\":\"百分位数入参\",\"name\":\"PercentileInput\",\"precision\":38,\"scale\":38,\"type\":5}]','[{\"description\":\"百分位\",\"name\":\"Quantile\",\"precision\":0,\"scale\":0,\"type\":6}]','[{\"description\":\"百分位数\",\"name\":\"Percentil\",\"precision\":38,\"scale\":38,\"type\":5}]'),(4,NULL,'MovingAverage','[{\"description\":\"移动平均入参\",\"name\":\"MovingAverageInput\",\"precision\":38,\"scale\":38,\"type\":5}]','[{\"description\":\"移动步长\",\"name\":\"MoveStep\",\"precision\":0,\"scale\":0,\"type\":2}]','[{\"description\":\"移动平均值\",\"name\":\"MovingAverage\",\"precision\":38,\"scale\":38,\"type\":5}]'),(5,NULL,'Correlation','[{\"description\":\"相关系数入参1\",\"name\":\"CorrelationInput1\",\"precision\":38,\"scale\":38,\"type\":5},{\"description\":\"相关系数入参2\",\"name\":\"CorrelationInput2\",\"precision\":38,\"scale\":38,\"type\":5}]','[{\"description\":\"计算相关系数方法\",\"name\":\"Method\",\"precision\":0,\"scale\":0,\"type\":9}]','[{\"description\":\"相关系数\",\"name\":\"Correlation\",\"precision\":0,\"scale\":0,\"type\":6}]'),(6,NULL,'OnewayANOVA','[{\"description\":\"单因素方差分析入参1\",\"name\":\"OneWayANOVAInput1\",\"precision\":38,\"scale\":38,\"type\":5},{\"description\":\"单因素方差分析入参2\",\"name\":\"OneWayANOVAInput2\",\"precision\":38,\"scale\":38,\"type\":5}]','[{\"description\":\"水平\",\"name\":\"α\",\"precision\":0,\"scale\":0,\"type\":4}]','[{\"description\":\"总误差平方和\",\"name\":\"SST\",\"precision\":38,\"scale\":38,\"type\":5},{\"description\":\"总自由度\",\"name\":\"dfT\",\"precision\":0,\"scale\":0,\"type\":2},{\"description\":\"组内误差平方和\",\"name\":\"SSE\",\"precision\":38,\"scale\":38,\"type\":5},{\"description\":\"组内自由度\",\"name\":\"dfE\",\"precision\":0,\"scale\":0,\"type\":2},{\"description\":\"组内误差均值\",\"name\":\"MSE\",\"precision\":38,\"scale\":38,\"type\":5},{\"description\":\"组间误差平方和\",\"name\":\"SSA\",\"precision\":38,\"scale\":38,\"type\":5},{\"description\":\"组间自由度\",\"name\":\"dfA\",\"precision\":0,\"scale\":0,\"type\":2},{\"description\":\"组间误差平均值\",\"name\":\"MSA\",\"precision\":38,\"scale\":38,\"type\":5},{\"description\":\"F比\",\"name\":\"F\",\"precision\":38,\"scale\":38,\"type\":5},{\"description\":\"显著性\",\"name\":\"pValue\",\"precision\":38,\"scale\":38,\"type\":5},{\"description\":\"F标准值\",\"name\":\"FCriterion\",\"precision\":38,\"scale\":38,\"type\":5}] '),(7,NULL,'PCA','[{\"description\":\"主成分分析入参1\",\"name\":\"PCAInput1\",\"precision\":38,\"scale\":38,\"type\":5}]','[{\"description\":\"主成分数量，大于或等于1，且小于或等于PCAInput的列数\",\"name\":\"k\",\"precision\":0,\"scale\":0,\"type\":4}]','[{\"description\":\"主成分\",\"name\":\"PrincipalComponent\",\"precision\":0,\"scale\":0,\"type\":10,\"length\":-1},{\"description\":\"解释方差\",\"name\":\"ExplainedVariance\",\"precision\":0,\"scale\":0,\"type\":10,\"length\":-1}]');

/*Table structure for table ds_statistics_parameters_column */

DROP TABLE IF EXISTS ds_statistics_parameters_column;

CREATE TABLE ds_statistics_parameters_column (
  id int(11) NOT NULL AUTO_INCREMENT,
  squid_id int(11) DEFAULT NULL,
  name varchar(255) DEFAULT NULL,
  value varchar(255) DEFAULT NULL,
  description varchar(255) DEFAULT NULL,
  type int(11) DEFAULT NULL,
  `precision` int(11) DEFAULT NULL,
  scale int(11) DEFAULT NULL,
  PRIMARY KEY (id),
  KEY ds_statistics_parameters_column_index (id,squid_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

/*Data for the table ds_statistics_parameters_column */

/*Table structure for table ds_sys_action_header */

DROP TABLE IF EXISTS ds_sys_action_header;

CREATE TABLE ds_sys_action_header (
  ID int(11) NOT NULL AUTO_INCREMENT,
  SYS_TABLE_NAME varchar(50) NOT NULL,
  ACTION char(1) NOT NULL,
  USER_ID int(11) NOT NULL,
  IS_UNDONE char(1) NOT NULL,
  ACTION_DATE timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (ID)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

/*Data for the table ds_sys_action_header */

/*Table structure for table ds_sys_action_line */

DROP TABLE IF EXISTS ds_sys_action_line;

CREATE TABLE ds_sys_action_line (
  ID int(11) NOT NULL AUTO_INCREMENT,
  HEADER_ID int(11) DEFAULT NULL,
  COLUMN_NAME varchar(100) NOT NULL,
  CELL_VALUE text,
  PRIMARY KEY (ID)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

/*Data for the table ds_sys_action_line */

/*Table structure for table ds_sys_application_status */

DROP TABLE IF EXISTS ds_sys_application_status;

CREATE TABLE ds_sys_application_status (
  ID int(11) NOT NULL AUTO_INCREMENT,
  REPOSITORY_ID int(11) DEFAULT NULL,
  PROJECT_ID int(11) DEFAULT NULL,
  SQUIDFLOW_ID int(11) DEFAULT NULL,
  LAUNCH_USER_ID int(11) DEFAULT NULL,
  STOP_USER_ID int(11) DEFAULT NULL,
  APPLICATION_ID varchar(100) DEFAULT NULL,
  STATUS varchar(20) DEFAULT NULL,
  CONFIG varchar(500) DEFAULT NULL,
  CREATE_DATE timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UPDATE_DATE datetime DEFAULT NULL,
  STOP_DATE datetime DEFAULT NULL,
  PRIMARY KEY (ID)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

/*Data for the table ds_sys_application_status */

/*Table structure for table ds_sys_datatype_mapping */

DROP TABLE IF EXISTS ds_sys_datatype_mapping;

CREATE TABLE ds_sys_datatype_mapping (
  ID int(11) NOT NULL AUTO_INCREMENT,
  DATABASE_TYPE varchar(128) NOT NULL,
  SOURCE_DATATYPE varchar(128) NOT NULL,
  SYSTEM_DATATYPE varchar(128) NOT NULL,
  RESTORE_PRIORITY tinyint(4) DEFAULT NULL,
  `precision` bigint(20) DEFAULT NULL,
  SCALE smallint(6) DEFAULT NULL,
  CREATE_PARAM_COUNT tinyint(4) DEFAULT NULL,
  DESCRIPTION varchar(500) DEFAULT NULL,
  PRIMARY KEY (ID)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

/*Data for the table ds_sys_datatype_mapping */

/*Table structure for table ds_sys_entity_type */

DROP TABLE IF EXISTS ds_sys_entity_type;

CREATE TABLE ds_sys_entity_type (
  ID int(11) NOT NULL AUTO_INCREMENT,
  NAME varchar(50) NOT NULL,
  DESCRIPTION varchar(500) DEFAULT NULL,
  PRIMARY KEY (ID)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

/*Data for the table ds_sys_entity_type */

/*Table structure for table ds_sys_group */

DROP TABLE IF EXISTS ds_sys_group;

CREATE TABLE ds_sys_group (
  ID int(11) NOT NULL AUTO_INCREMENT,
  `key` varchar(50) DEFAULT NULL,
  TEAM_ID int(11) NOT NULL,
  PARENT_GROUP_ID int(11) DEFAULT NULL,
  NAME varchar(128) NOT NULL,
  DESCRIPTION varchar(500) DEFAULT NULL,
  LOCATION_X int(11) DEFAULT NULL,
  LOCATION_Y int(11) DEFAULT NULL,
  CREATION_DATE timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (ID),
  UNIQUE KEY SYS_IDX_UNIQUE_GROUP_NAME_IN_TEAM_10127 (NAME,TEAM_ID),
  KEY SYS_IDX_10128 (TEAM_ID)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

/*Data for the table ds_sys_group */

/*Table structure for table ds_sys_job_schedule */

DROP TABLE IF EXISTS ds_sys_job_schedule;

CREATE TABLE ds_sys_job_schedule (
  ID int(11) NOT NULL AUTO_INCREMENT,
  OBJECT_TYPE int(11) DEFAULT NULL,
  TEAM_ID int(11) DEFAULT NULL,
  REPOSITORY_ID int(11) DEFAULT NULL,
  PROJECT_ID int(11) DEFAULT NULL,
  PROJECT_NAME varchar(100) DEFAULT NULL,
  SQUID_FLOW_ID int(11) DEFAULT NULL,
  SQUID_FLOW_NAME varchar(100) DEFAULT NULL,
  SQUID_ID int(11) DEFAULT NULL,
  SCHEDULE_TYPE varchar(50) DEFAULT NULL,
  SCHEDULE_BEGIN_DATE datetime DEFAULT NULL,
  SCHEDULE_END_DATE datetime DEFAULT NULL,
  SCHEDULE_VALID int(11) DEFAULT NULL,
  DAY_DELY int(11) DEFAULT NULL,
  DAY_RUN_COUNT int(11) DEFAULT NULL,
  DAY_BEGIN_DATE time DEFAULT NULL,
  DAY_END_DATE time DEFAULT NULL,
  DAY_RUN_DELY int(11) DEFAULT NULL,
  DAY_ONCE_OFF_TIME time DEFAULT NULL,
  WEEK_DAY int(11) DEFAULT NULL,
  WEEK_BEGIN_DATE time DEFAULT NULL,
  MONTH_DAY int(11) DEFAULT NULL,
  MONTH_BEGIN_DATE time DEFAULT NULL,
  LAST_SCHEDULED_DATE datetime DEFAULT NULL,
  CREATION_DATE timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  STATUS char(1) DEFAULT NULL,
  ENABLE_EMAIL_SENDING int(11) DEFAULT NULL,
  EMAIL_ADDRESS varchar(500) DEFAULT NULL,
  PRIMARY KEY (ID)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

/*Data for the table ds_sys_job_schedule */

/*Table structure for table ds_sys_metadata_node */

DROP TABLE IF EXISTS ds_sys_metadata_node;

CREATE TABLE ds_sys_metadata_node (
  ID int(11) NOT NULL AUTO_INCREMENT,
  PARENT_ID int(11) DEFAULT NULL,
  NODE_TYPE int(11) DEFAULT NULL,
  NODE_NAME varchar(100) DEFAULT NULL,
  ORDER_NUMBER int(11) DEFAULT NULL,
  NODE_DESC varchar(200) DEFAULT NULL,
  CREATION_DATE timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (ID)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

/*Data for the table ds_sys_metadata_node */

/*Table structure for table ds_sys_metadata_node_attr */

DROP TABLE IF EXISTS ds_sys_metadata_node_attr;

CREATE TABLE ds_sys_metadata_node_attr (
  ID int(11) NOT NULL AUTO_INCREMENT,
  ATTR_NAME varchar(100) DEFAULT NULL,
  ATTR_VALUE varchar(500) DEFAULT NULL,
  NODE_ID int(11) DEFAULT NULL,
  CREATION_DATE timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (ID),
  KEY SYS_IDX_10228 (NODE_ID),
  CONSTRAINT FK_REFERENCE_10 FOREIGN KEY (NODE_ID) REFERENCES ds_sys_metadata_node (ID) ON DELETE CASCADE ON UPDATE NO ACTION
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

/*Data for the table ds_sys_metadata_node_attr */

/*Table structure for table ds_sys_privilege */

DROP TABLE IF EXISTS ds_sys_privilege;

CREATE TABLE ds_sys_privilege (
  PARTY_ID int(11) DEFAULT NULL,
  PARTY_TYPE char(1) DEFAULT NULL,
  ENTITY_TYPE_ID int(11) DEFAULT NULL,
  CAN_VIEW char(1) DEFAULT NULL,
  CAN_CREATE char(1) DEFAULT NULL,
  CAN_UPDATE char(1) DEFAULT NULL,
  CAN_DELETE char(1) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

/*Data for the table ds_sys_privilege */

/*Table structure for table ds_sys_report_folder */

DROP TABLE IF EXISTS ds_sys_report_folder;

CREATE TABLE ds_sys_report_folder (
  ID int(11) NOT NULL AUTO_INCREMENT,
  FOLDER_NAME varchar(50) DEFAULT NULL,
  CREATION_DATE timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PID int(11) DEFAULT NULL,
  IS_DISPLAY tinyint(1) DEFAULT NULL,
  PRIMARY KEY (ID),
  KEY IDX_REPORT_FOLDER_PID (PID),
  CONSTRAINT FK_PID FOREIGN KEY (PID) REFERENCES ds_sys_report_folder (ID) ON DELETE CASCADE ON UPDATE NO ACTION
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

/*Data for the table ds_sys_report_folder */

/*Table structure for table ds_sys_report_folder_mapping */

DROP TABLE IF EXISTS ds_sys_report_folder_mapping;

CREATE TABLE ds_sys_report_folder_mapping (
  ID int(11) NOT NULL AUTO_INCREMENT,
  REPOSITORY_ID int(11) DEFAULT NULL,
  SQUID_ID int(11) DEFAULT NULL,
  FOLDER_ID int(11) DEFAULT NULL,
  REPORT_NAME varchar(50) DEFAULT NULL,
  CREATION_DATE timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (ID),
  KEY IDX_MAPPING_FOLDER_ID (FOLDER_ID),
  CONSTRAINT FK_FOLDER_ID FOREIGN KEY (FOLDER_ID) REFERENCES ds_sys_report_folder (ID) ON DELETE CASCADE ON UPDATE NO ACTION
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

/*Data for the table ds_sys_report_folder_mapping */

/*Table structure for table ds_sys_repository */

DROP TABLE IF EXISTS ds_sys_repository;

CREATE TABLE ds_sys_repository (
  ID int(11) NOT NULL AUTO_INCREMENT,
  TEAM_ID int(11) NOT NULL,
  `key` varchar(50) DEFAULT NULL,
  NAME varchar(100) NOT NULL,
  DESCRIPTION varchar(500) DEFAULT NULL,
  REPOSITORY_DB_NAME varchar(100) NOT NULL,
  TYPE int(11) DEFAULT NULL,
  STATUS_ID int(11) NOT NULL,
  CREATION_DATE timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (ID),
  UNIQUE KEY SYS_IDX_UNIQUE_REPOSITORY_NAME_IN_TEAM_10110 (TEAM_ID,NAME),
  KEY SYS_IDX_10111 (TEAM_ID)
) ENGINE=InnoDB AUTO_INCREMENT=4 DEFAULT CHARSET=utf8;

/*Data for the table ds_sys_repository */

insert  into ds_sys_repository(ID,TEAM_ID,`key`,NAME,DESCRIPTION,REPOSITORY_DB_NAME,TYPE,STATUS_ID,CREATION_DATE) values (1,1,'F0EDEC4A-1D4D-CC2A-2BC6-9A6D81E52932','test','test','prod',1,1,now()),(2,1,'DD98E05F-AE33-BAB9-DBF4-28BCF31A786E','dev','dev','prod',1,1,now()),(3,1,'23CCFA2B-CA06-1E23-EEB1-F6CFFB83C939','prod','prod','prod',1,1,now());

/*Table structure for table ds_sys_repository_status */

DROP TABLE IF EXISTS ds_sys_repository_status;

CREATE TABLE ds_sys_repository_status (
  ID int(11) NOT NULL AUTO_INCREMENT,
  NAME varchar(100) NOT NULL,
  DESCRIPTION varchar(500) DEFAULT NULL,
  PRIMARY KEY (ID)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

/*Data for the table ds_sys_repository_status */

/*Table structure for table ds_sys_role */

DROP TABLE IF EXISTS ds_sys_role;

CREATE TABLE ds_sys_role (
  ID int(11) NOT NULL AUTO_INCREMENT,
  `key` varchar(50) NOT NULL,
  GROUP_ID int(11) DEFAULT NULL,
  NAME varchar(100) NOT NULL,
  DESCRIPTION varchar(500) DEFAULT NULL,
  CREATION_DATE timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (ID),
  UNIQUE KEY SYS_IDX_UNIQUE_ROLE_NAME_IN_GROUP_10138 (NAME,GROUP_ID),
  KEY SYS_IDX_10139 (GROUP_ID)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

/*Data for the table ds_sys_role */

/*Table structure for table ds_sys_server_parameter */

DROP TABLE IF EXISTS ds_sys_server_parameter;

CREATE TABLE ds_sys_server_parameter (
  ID int(11) NOT NULL AUTO_INCREMENT,
  NAME varchar(128) DEFAULT NULL,
  VALUE varchar(256) DEFAULT NULL,
  DATA_TYPE char(1) NOT NULL,
  IS_COLLECTION char(1) NOT NULL,
  PRIMARY KEY (ID)
) ENGINE=InnoDB AUTO_INCREMENT=8 DEFAULT CHARSET=utf8;

/*Data for the table ds_sys_server_parameter */

insert  into ds_sys_server_parameter(ID,NAME,VALUE,DATA_TYPE,IS_COLLECTION) values (1,'SuperUserPwd','111111','S','N'),(2,'HsqlDBPath','D:/datashire/repository/','S','N'),(3,'ServerPort','9999','N','N'),(4,'LimitedTime','m0fR5vEhxJ6X5Ey46nPIRA==','N','N'),(5,'LicenseKey','','N','N'),(6,'VERSION','2.2.2','N','N'),(7,'SPLIT_COLUMN_FILTER','{\"MYSQL\":\"BLOB,LONGBLOB,MEDIUMBLOB,TINYBLOB,XML,VARCHAR\",\"ORACLE\":\"BLOB,BINARY_DOUBLE,NCLOB,CLOB\",\"SQLSERVER\":\"BINARY,VARCHAR,NVARCHAR,VARBINARY,TIMESTAMP,UNIQUEIDENTIFIER,IMAGE,XML,HIERARCHYID,TEXT,NTEXT,DATETIMEOFFSET,SQL_VARIANT,BIT,CHAR\",\"DB2\":\"CLOB\"}','N','N');

/*Table structure for table ds_sys_sf_job_history */

DROP TABLE IF EXISTS ds_sys_sf_job_history;

CREATE TABLE ds_sys_sf_job_history (
  TASK_ID varchar(40) NOT NULL,
  REPOSITORY_ID int(11) DEFAULT NULL,
  SQUID_FLOW_ID int(11) DEFAULT NULL,
  JOB_ID int(11) DEFAULT NULL,
  STATUS int(11) DEFAULT NULL,
  DEBUG_SQUIDS varchar(500) DEFAULT NULL,
  DESTINATION_SQUIDS varchar(500) DEFAULT NULL,
  CALLER varchar(10) DEFAULT NULL,
  CREATE_TIME timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UPDATE_TIME datetime DEFAULT NULL,
  PRIMARY KEY (TASK_ID),
  KEY IDX_JOB_HISTORY_JOB_ID (JOB_ID)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

/*Data for the table ds_sys_sf_job_history */

/*Table structure for table ds_sys_sf_job_module_log */

DROP TABLE IF EXISTS ds_sys_sf_job_module_log;

CREATE TABLE ds_sys_sf_job_module_log (
  TASK_ID varchar(40) NOT NULL,
  SQUID_ID int(11) NOT NULL,
  STATUS int(11) DEFAULT NULL,
  CREATE_TIME timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UPDATE_TIME datetime DEFAULT NULL,
  PRIMARY KEY (TASK_ID,SQUID_ID),
  UNIQUE KEY SYS_IDX_SYS_PK_10242_10243 (TASK_ID,SQUID_ID)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

/*Data for the table ds_sys_sf_job_module_log */

/*Table structure for table ds_sys_sf_log */

DROP TABLE IF EXISTS ds_sys_sf_log;

CREATE TABLE ds_sys_sf_log (
  ID int(11) NOT NULL AUTO_INCREMENT,
  TASK_ID varchar(36) NOT NULL,
  MESSAGE varchar(500) DEFAULT NULL,
  CREATE_TIME timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  LOG_LEVEL int(11) DEFAULT NULL,
  SQUID_ID int(11) DEFAULT NULL,
  TSQUID_ID varchar(40) DEFAULT NULL,
  TSQUID_TYPE int(11) DEFAULT NULL,
  PRIMARY KEY (ID),
  KEY SYS_IDX_10254 (TASK_ID)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

/*Data for the table ds_sys_sf_log */

/*Table structure for table ds_sys_squid_flow_status */

DROP TABLE IF EXISTS ds_sys_squid_flow_status;

CREATE TABLE ds_sys_squid_flow_status (
  ID int(11) NOT NULL AUTO_INCREMENT,
  TEAM_ID int(11) DEFAULT NULL,
  REPOSITORY_ID int(11) DEFAULT NULL,
  PROJECT_ID int(11) DEFAULT NULL,
  SQUID_FLOW_ID int(11) DEFAULT NULL,
  STATUS int(11) DEFAULT NULL,
  OWNER_CLIENT_TOKEN varchar(50) DEFAULT NULL,
  PRIMARY KEY (ID),
  KEY IDX_STATUS_PROJECT_ID (PROJECT_ID),
  KEY IDX_STATUS_REPOSITORY_ID (REPOSITORY_ID),
  KEY IDX_STATUS_SQUID_FLOW_ID (SQUID_FLOW_ID)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

/*Data for the table ds_sys_squid_flow_status */

/*Table structure for table ds_sys_team */

DROP TABLE IF EXISTS ds_sys_team;

CREATE TABLE ds_sys_team (
  ID int(11) NOT NULL AUTO_INCREMENT,
  `key` varchar(50) DEFAULT NULL,
  NAME varchar(100) NOT NULL,
  DESCRIPTION varchar(500) DEFAULT NULL,
  CREATION_DATE timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (ID),
  UNIQUE KEY SYS_IDX_UNIQUE_TEAM_NAME_10097 (NAME)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

/*Data for the table ds_sys_team */

/*Table structure for table ds_sys_template_data */

DROP TABLE IF EXISTS ds_sys_template_data;

CREATE TABLE ds_sys_template_data (
  ID int(11) NOT NULL AUTO_INCREMENT,
  TYPE_ID int(11) DEFAULT NULL,
  DATA_VALUE varchar(500) DEFAULT NULL,
  CREATION_DATE timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (ID),
  KEY SYS_IDX_10206 (TYPE_ID)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

/*Data for the table ds_sys_template_data */

/*Table structure for table ds_sys_template_type */

DROP TABLE IF EXISTS ds_sys_template_type;

CREATE TABLE ds_sys_template_type (
  ID int(11) NOT NULL AUTO_INCREMENT,
  TYPE_NAME varchar(50) DEFAULT NULL,
  DATA_TYPE int(11) DEFAULT NULL,
  CREATION_DATE timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (ID)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

/*Data for the table ds_sys_template_type */

/*Table structure for table ds_sys_user */

DROP TABLE IF EXISTS ds_sys_user;

CREATE TABLE ds_sys_user (
  ID int(11) NOT NULL AUTO_INCREMENT,
  `key` varchar(50) NOT NULL,
  ROLE_ID int(11) DEFAULT NULL,
  USER_NAME varchar(50) NOT NULL,
  PASSWORD varchar(50) DEFAULT NULL,
  FULL_NAME varchar(20) DEFAULT NULL,
  EMAIL_ADDRESS varchar(100) DEFAULT NULL,
  STATUS_ID int(11) DEFAULT NULL,
  IS_ACTIVE char(1) DEFAULT NULL,
  LAST_LOGON_DATE datetime DEFAULT NULL,
  CREATION_DATE timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (ID),
  UNIQUE KEY SYS_IDX_UNIQUE_USER_NAME_10149 (USER_NAME),
  KEY SYS_IDX_10150 (ROLE_ID)
) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8;

/*Data for the table ds_sys_user */

insert  into ds_sys_user(ID,`key`,ROLE_ID,USER_NAME,PASSWORD,FULL_NAME,EMAIL_ADDRESS,STATUS_ID,IS_ACTIVE,LAST_LOGON_DATE,CREATION_DATE) values (1,'58610ba8-5225-4300-81a2-b73dd5bdc168',NULL,'superuser','7066a4f427769cc43347aa96b72931a','superuser','www.eurlanda.com',NULL,'N',NULL,'2017-10-10 15:16:27');

/*Table structure for table ds_sys_user_status */

DROP TABLE IF EXISTS ds_sys_user_status;

CREATE TABLE ds_sys_user_status (
  ID int(11) NOT NULL AUTO_INCREMENT,
  NAME varchar(100) DEFAULT NULL,
  DESCRIPTION varchar(500) DEFAULT NULL,
  PRIMARY KEY (ID)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

/*Data for the table ds_sys_user_status */

/*Table structure for table ds_thirdparty_params */

DROP TABLE IF EXISTS ds_thirdparty_params;

CREATE TABLE ds_thirdparty_params (
  ID int(11) NOT NULL AUTO_INCREMENT,
  SQUID_ID int(11) DEFAULT NULL,
  SOURCE_TABLE_ID int(11) DEFAULT NULL,
  NAME varchar(200) DEFAULT NULL,
  PARAMS_TYPE int(11) DEFAULT NULL,
  VALUE_TYPE int(11) DEFAULT NULL,
  VAL varchar(200) DEFAULT NULL,
  COLUMN_ID int(11) DEFAULT NULL,
  REF_SQUID_ID int(11) DEFAULT NULL,
  PRIMARY KEY (ID)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

/*Data for the table ds_thirdparty_params */

/*Table structure for table ds_tran_input_definition */

DROP TABLE IF EXISTS ds_tran_input_definition;

CREATE TABLE ds_tran_input_definition (
  CODE varchar(50) NOT NULL,
  INPUT_ORDER int(11) NOT NULL,
  INPUT_DATA_TYPE int(11) NOT NULL,
  DESCRIPTION varchar(500) DEFAULT NULL,
  KEY IDX_TRAN_INPUT_DEFINITION_CODE (CODE),
  KEY SYS_IDX_10484 (CODE)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

/*Data for the table ds_tran_input_definition */

insert  into ds_tran_input_definition(CODE,INPUT_ORDER,INPUT_DATA_TYPE,DESCRIPTION) values ('VIRTUAL',0,21,'和squidColumn/referenceColumn绑定的虚拟转换。'),('CONSTANT',-1,0,''),('CHOICE',9999,21,''),('CONCATENATE',9999,9,'输入字符串'),('TERMEXTRACT',0,9,'要分析的源字符串'),('TERMEXTRACT',1,9,'表达式'),('TERMEXTRACT',2,2,'提取索引'),('SPLIT',0,9,'被拆分的字符串'),('SPLIT',1,9,'分隔符'),('ASCII',0,9,'要转换的字符'),('UNICODE',0,9,'要转换的字符'),('SIMILARITY',0,9,'要对比的第一个字符串'),('SIMILARITY',1,9,'要对比的第二个字符串'),('CHAR',0,2,'ASCII数值'),('PATTERNINDEX',0,9,'源字符串'),('PATTERNINDEX',1,9,'要搜索的目标字符串表达式'),('REPLICATE',0,9,'被重复的字符串'),('REPLICATE',1,2,'重复的次数'),('NUMERICTOSTRING',0,6,'要转化的数值'),('STRINGTONUMERIC',0,9,'要转化的字符串数值'),('REPLACE',0,9,'在此字符串中搜索'),('REPLACE',1,9,'替换成此字符串'),('REPLACE',2,9,'匹配模式'),('LEFT',0,9,'被截取的源字符串'),('LEFT',1,2,'截取的长度'),('RIGHT',0,9,'被截取的源字符串'),('RIGHT',1,2,'截取的长度'),('SUBSTRING',0,9,'源字符串'),('SUBSTRING',1,2,'开始截取的位置'),('SUBSTRING',2,2,'截取的长度'),('LENGTH',0,9,'源字符串'),('REVERSE',0,9,'源字符串'),('LOWER',0,9,'源字符串'),('UPPER',0,9,'源字符串'),('LEFTTRIM',0,9,'源字符串'),('RIGHTTRIM',0,9,'源字符串'),('TRIM',0,9,'源字符串'),('SYSTEMDATETIME',-1,0,''),('STRINGTODATETIME',0,9,'要转换的日期'),('DATETIMETOSTRING',0,13,'要转换的日期'),('YEAR',0,13,'要转换的日期'),('MONTH',0,13,'要转换的日期'),('DAY',0,13,'要转换的日期'),('DATEDIFF',0,13,'开始日期'),('DATEDIFF',1,13,'结束日期'),('FORMATDATE',0,9,'日期值'),('ABS',0,6,'数值'),('RANDOM',-1,0,''),('ACOS',0,6,'-1到+1之间的浮点数'),('EXP',0,6,'浮点数'),('ROUND',0,6,'浮点数'),('ROUND',1,2,'舍入长度'),('ASIN',0,6,'-1到+1之间的浮点数'),('FLOOR',0,6,'浮点数'),('SIGN',0,6,'浮点数'),('ATAN',0,6,'-∞到+∞'),('LOG',0,6,'浮点数'),('SIN',0,6,'浮点数'),('LOG10',0,6,'浮点数'),('SQRT',0,6,'浮点数'),('CEILING',0,6,'浮点数'),('PI',-1,0,''),('SQUARE',0,6,'浮点数'),('COS',0,6,'浮点数'),('POWER',0,6,'浮点数'),('POWER',1,2,'幂'),('TAN',0,6,'浮点数'),('COT',0,6,'-∞到+∞，且不等于kπ，k∈Z'),('RADIANS',0,6,'浮点数'),('CALCULATION',0,6,'参加运算的第一个浮点数'),('CALCULATION',1,6,'参加运算的第二个浮点数'),('MOD',0,2,'参加运算的整数'),('MOD',1,2,'模'),('PROJECTID',-1,0,''),('PROJECTNAME',-1,0,''),('SQUIDFLOWID',-1,0,''),('SQUIDFLOWNAME',-1,0,''),('JOBID',-1,0,''),('TOKENIZATION',0,9,'分词文本'),('PREDICT ',0,10,'预测数据'),('PREDICT ',1,21,'预测数据的KEY'),('NUMASSEMBLE',9999,6,'浮点型输入'),('TRAIN',0,10,'训练数据'),('NUMERICCAST',0,6,'被转换的数值输入'),('NUMERICCAST',1,2,'舍入的位数'),('INVERSEQUANTIFY',0,6,'浮点输入'),('CSNTOSTRING',0,10,'CSN输入'),('COUNTRY',0,9,'源字符串'),('PROVINCE',0,9,'源字符串'),('CITY',0,9,'源字符串'),('DISTRICT',0,9,'源字符串'),('STREET',0,9,'源字符串'),('DATEPART',0,13,'原始日期'),('DATETOUNIXTIME',0,13,'要转换的日期'),('UNIXTIMETODATE',0,1,'要转换unix时间戳整数'),('DATEINC',0,13,'要做增减运算的日期'),('MAPPUT',0,1022,'原始集合'),('MAPPUT',1,9,'要添加的元素键'),('MAPPUT',2,21,'要添加的元素值'),('MAPGET',0,1022,'原始集合'),('MAPGET',1,9,'要获取的元素键'),('MAPREMOVE',0,1022,'原始集合'),('MAPREMOVE',1,9,'要移除的元素键'),('ARRAYPUT',0,86,'原始集合'),('ARRAYPUT',1,2,'要添加的元素键'),('ARRAYPUT',2,21,'要添加的元素值'),('ARRAYGET',0,86,'原始集合'),('ARRAYGET',1,2,'要获取的元素键'),('ARRAYREMOVE',0,86,'原始集合'),('ARRAYREMOVE',1,2,'要移除的元素键'),('DATEFORMAT',0,13,'将时间类型格式化为字符串'),('SPLIT2ARRAY',0,9,'将字符串分割为数组'),('SPLIT2ARRAY',1,9,'分隔符'),('TERMEXTRACT2ARRAY',0,9,'需要提取的原始字符串'),('TERMEXTRACT2ARRAY',1,9,'表达式'),('CSVASSEMBLE',9999,9,'要组合CSV的字符串'),('RULESQUERY',0,15,'前项'),('RULESQUERY',1,2,'前项中的元素数量'),('RULESQUERY',2,15,'后项'),('RULESQUERY',3,2,'后项中的元素数量'),('RULESQUERY',4,6,'最小可信度'),('RULESQUERY',5,6,'最小规则支持度'),('RULESQUERY',6,6,'最小提升度'),('RULESQUERY',7,2,'规则数量'),('BINARYTOSTRING',0,12,'需要被转化的二进制序列'),('CST',1,21,'类型转换'),('INVERSENORMALIZER',1,10,'CSN中元素个数与训练时CSN中元素个数相同');

/*Table structure for table ds_tran_inputs */

DROP TABLE IF EXISTS ds_tran_inputs;

CREATE TABLE ds_tran_inputs (
  ID int(11) NOT NULL AUTO_INCREMENT,
  TRANSFORMATION_ID int(11) NOT NULL,
  RELATIVE_ORDER int(11) NOT NULL,
  SOURCE_TRANSFORM_ID int(11) DEFAULT NULL,
  SOURCE_TRAN_OUTPUT_INDEX int(11) DEFAULT NULL,
  IN_CONDITION varchar(1000) DEFAULT NULL,
  INPUT_VALUE varchar(50) DEFAULT NULL,
  INPUT_DATA_TYPE int(11) DEFAULT NULL,
  DESCRIPTION varchar(500) DEFAULT NULL,
  PRIMARY KEY (ID),
  KEY IDX_TRANS_ID (TRANSFORMATION_ID),
  KEY IDX_TRANS_SOURCE_ID (SOURCE_TRANSFORM_ID)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

/*Data for the table ds_tran_inputs */

/*Table structure for table ds_transformation */

DROP TABLE IF EXISTS ds_transformation;

CREATE TABLE ds_transformation (
  ID int(11) NOT NULL AUTO_INCREMENT,
  SQUID_ID int(11) NOT NULL,
  TRANSFORMATION_TYPE_ID int(11) NOT NULL,
  LOCATION_X int(11) DEFAULT NULL,
  LOCATION_Y int(11) DEFAULT NULL,
  COLUMN_ID int(11) DEFAULT NULL,
  DESCRIPTION varchar(500) DEFAULT NULL,
  NAME varchar(100) DEFAULT NULL,
  OUTPUT_DATA_TYPE int(11) DEFAULT NULL,
  CONSTANT_VALUE varchar(4000) DEFAULT NULL,
  OUTPUT_NUMBER int(11) DEFAULT NULL,
  ALGORITHM int(11) DEFAULT NULL,
  TRAN_CONDITION varchar(500) DEFAULT NULL,
  DIFFERENCE_TYPE int(11) DEFAULT NULL,
  IS_USING_DICTIONARY int(11) DEFAULT NULL,
  DICTIONARY_SQUID_ID int(11) DEFAULT NULL,
  BUCKET_COUNT int(11) DEFAULT NULL,
  MODEL_SQUID_ID int(11) DEFAULT NULL,
  MODEL_VERSION int(11) DEFAULT NULL,
  OPERATOR int(11) DEFAULT NULL,
  DATE_FORMAT varchar(100) DEFAULT NULL,
  INC_UNIT int(11) DEFAULT NULL,
  SPLIT_TYPE int(11) DEFAULT NULL,
  encoding int(2) DEFAULT NULL,
  PRIMARY KEY (ID),
  KEY IDX_TRANSFORMATION_SQUID_ID (SQUID_ID),
  KEY IDX_TRANSFORMATION_TYPE_ID (TRANSFORMATION_TYPE_ID)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

/*Data for the table ds_transformation */

/*Table structure for table ds_transformation_link */

DROP TABLE IF EXISTS ds_transformation_link;

CREATE TABLE ds_transformation_link (
  ID int(11) NOT NULL AUTO_INCREMENT,
  `key` varchar(50) NOT NULL,
  FROM_TRANSFORMATION_ID int(11) NOT NULL,
  TO_TRANSFORMATION_ID int(11) NOT NULL,
  IN_ORDER int(11) DEFAULT NULL,
  PRIMARY KEY (ID),
  KEY IDX_TRANSFORMATION_LINK_FROM_ID (FROM_TRANSFORMATION_ID),
  KEY IDX_TRANSFORMATION_LINK_TO_ID (TO_TRANSFORMATION_ID)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

/*Data for the table ds_transformation_link */

/*Table structure for table ds_transformation_type */

DROP TABLE IF EXISTS ds_transformation_type;

CREATE TABLE ds_transformation_type (
  ID int(11) NOT NULL,
  CODE varchar(50) NOT NULL,
  OUTPUT_DATA_TYPE int(11) DEFAULT NULL,
  DESCRIPTION varchar(500) DEFAULT NULL,
  PRIMARY KEY (ID),
  UNIQUE KEY SYS_IDX_SYS_CT_10460_10464 (CODE),
  UNIQUE KEY SYS_IDX_SYS_PK_10458_10461 (ID)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

/*Data for the table ds_transformation_type */

insert  into ds_transformation_type(ID,CODE,OUTPUT_DATA_TYPE,DESCRIPTION) values (-1,'UNKNOWN',21,''),(0,'VIRTUAL',21,'和对应的column相同'),(1,'UPPER',9,'把一个字符串转换成大写串'),(2,'CONCATENATE',9,'把多个字符串按次序连接成一个字符串'),(3,'LOWER',9,'把一个字符串转换成小写串'),(4,'CONSTANT',21,'定义一个常量，可以是不同类型。'),(5,'CHOICE',21,'根据各个输入对象的逻辑条件取值，输出一个逻辑条件取值为True的输入对象。'),(7,'TERMEXTRACT',9,'从输入字符串的左边开始搜索，提取出和RegExpression匹配的第TermIndex个字串。如果没有，则返回空。'),(8,'SPLIT',9,'对string类型的数据进行变换，把一个string拆分为几个字串，同时输出每一个子串。'),(9,'ASCII',2,'返回一个字符的ASCII数值'),(10,'UNICODE',2,'返回一个字符的Unicode数值'),(11,'SIMILARITY',6,'计算两个字符串的相似度。'),(12,'CHAR',8,'把一个ASCII整型数值转换为一个对应的字符。'),(13,'PATTERNINDEX',2,'搜索一个字符串，返回另一个字符串（可以是正则表达式）在其中第1次出现的位置。如果没有出现返回-1.'),(14,'REPLICATE',9,'重复一个字符串指定次数'),(15,'NUMERICTOSTRING',9,'把一个数值数据转换为字符串。'),(16,'STRINGTONUMERIC',6,'把一个字符串转换为数值数据。'),(17,'REPLACE',9,'在一个字符串中寻找一个子串，并全部替换为另外一个字串。'),(18,'LEFT',9,'从一个字符串的左边中截取特定长度的字串。'),(19,'RIGHT',9,'从一个字符串的右边中截取特定长度的字串。'),(21,'SUBSTRING',9,'从一个字符串中截取特定长度的子串。'),(22,'LENGTH',2,'返回一个字符串的长度'),(23,'REVERSE',9,'把一个字符串倒序后返回'),(24,'LEFTTRIM',9,'去掉一个字符串左边的空格。'),(25,'RIGHTTRIM',9,'去掉一个字符串右边的空格。'),(26,'TRIM',9,'去掉一个字符串左边和右边的空格。'),(27,'ROWNUMBER',2,'返回一行的记录号。'),(28,'SYSTEMDATETIME',13,'返回一个YYYY-MM-DD hh:mm:ss[.nnn]类型的系统当前日期和时间'),(29,'STRINGTODATETIME',13,'把一个YYYY-MM-DD hh:mm:ss[.nnn]字符串转换为日期时间。'),(30,'DATETIMETOSTRING',9,'把一个YYYY-MM-DD hh:mm:ss[.nnn]日期时间转换为字符串。'),(31,'YEAR',2,'把一个YYYY-MM-DD hh:mm:ss[.nnn]日期时间的年部分转换为整型数值。'),(32,'MONTH',2,'把一个YYYY-MM-DD hh:mm:ss[.nnn]日期时间的月部分转换为整型数值。'),(33,'DAY',2,'把一个YYYY-MM-DD hh:mm:ss[.nnn]日期时间的天部分转换为整型数值。'),(34,'DATEDIFF',2,'计算两个日期之间的差。'),(35,'FORMATDATE',13,'把一个日期值格式化为DataShire系统内部统一格式'),(36,'ABS',6,'求绝对值'),(37,'RANDOM',6,'返回0到1之间的一个随机数'),(38,'ACOS',6,'数学函数，返回其余弦是所指定的 float 表达式的角（弧度）；也称为反余弦'),(39,'EXP',6,'返回指定的 float 表达式的指数值'),(40,'ROUND',6,'返回一个舍入到指定的长度的数值。'),(41,'ASIN',6,'反正弦。'),(42,'FLOOR',2,'返回小于或等于指定数值表达式的最大整数'),(43,'SIGN',2,'返回指定表达式的正号 (+1)、零 (0) 或负号 (-1)'),(44,'ATAN',6,'反正切函数'),(45,'LOG',6,'自然对数'),(46,'SIN',6,'返回指定角度（以弧度为单位）的三角正弦值'),(47,'LOG10',6,'返回指定 float 表达式的常用对数'),(48,'SQRT',6,'平方根'),(49,'CEILING',2,'返回大于或等于指定数值表达式的最小整数'),(50,'PI',6,'返回 PI 的常量值'),(51,'SQUARE',6,'返回指定浮点值的平方'),(52,'COS',6,'返回指定表达式中以弧度表示的指定角的三角余弦'),(53,'POWER',6,'返回指定表达式的指定幂的值'),(54,'TAN',6,'正切'),(55,'COT',6,'三角余切'),(56,'RADIANS',6,'对于在数值表达式中输入的度数值返回弧度值'),(57,'CALCULATION',5,'对两个数值进行四则运算'),(58,'MOD',2,'对一个整数取模'),(59,'PROJECTID',2,'返回该Squid所在的Project的ID'),(60,'PROJECTNAME',9,'返回该Squid所在的Project的名字'),(61,'SQUIDFLOWID',2,'返回该Squid所在的SquidFlow的ID'),(62,'SQUIDFLOWNAME',9,'返回该Squid所在的SquidFlow的名字'),(63,'JOBID',2,'返回运行该Squid的Job的ID'),(64,'TOKENIZATION',10,'把文本按照分词规则转化为CSN'),(65,'PREDICT',6,'使用ModelSquid的model给出输入数据的预测值'),(68,'NUMASSEMBLE',10,'把多个浮点值输入组装为一个CSN格式的字符串'),(70,'TRAIN',12,'根据该组件所归属的数据挖据squid，使用训练数据得出特定模型。要求标签在输入CSN的第1个位置'),(75,'NUMERICCAST',6,'数值精度转换'),(76,'INVERSEQUANTIFY',21,'把基于dm squidModelBase model的predict输出，反推得到原始的标签值'),(77,'CSNTOSTRING',9,'把一个CSN序列转换成字符串'),(78,'COUNTRY',9,'例子: “中国河南省郑州市二七区花园路88号”.输出为“中国”'),(79,'PROVINCE',9,'例子: “中国河南省郑州市二七区花园路88号”.输出为“河南省”'),(80,'CITY',9,'例子: “中国河南省郑州市二七区花园路88号”.输出为“郑州市”'),(81,'DISTRICT',9,'例子: “中国河南省郑州市二七区花园路88号”.输出为“二七区”'),(82,'STREET',9,'例子: “中国河南省郑州市二七区花园路88号”.输出为“花园路88号”'),(83,'DATEPART',2,'把一个YYYY-MM-DD hh:mm:ss[.nnn]日期时间的部分转换为整型数值'),(84,'DATETOUNIXTIME',1,'把一个标准的系统日期转换为Unix时间戳数字'),(85,'UNIXTIMETODATE',13,'把一个Unix时间戳数字转换为标准的系统日期'),(86,'DATEINC',13,'对一个系统时间增加/减少特定时间单位'),(87,'TASKID',9,'返回TASK ID'),(88,'MAPPUT',1022,'把一个添加或修改集合中的元素'),(89,'MAPGET',1022,'获取集合中某一个元素'),(90,'MAPREMOVE',1022,'对一个系统时间增加/减少特定时间单位'),(91,'ARRAYPUT',86,'把一个添加或修改集合中的元素'),(92,'ARRAYGET',86,'获取集合中某一个元素'),(93,'ARRAYREMOVE',86,'对一个系统时间增加/减少特定时间单位'),(94,'DATEFORMAT',9,'将时间类型格式化为字符串'),(95,'SPLIT2ARRAY',86,'将字符串分割为数组'),(96,'TERMEXTRACT2ARRAY',86,'提取符合表达式要求的字符串结果集'),(97,'CSVASSEMBLE',15,'把多个字符串拼接成CSV格式的字符串'),(98,'RULESQUERY',86,'用于关联规则查询'),(99,'BINARYTOSTRING',9,'把一个二进制序列转换成字符串'),(100,'CST',0,'类型转换'),(101,'INVERSENORMALIZER',10,'');

/*Table structure for table ds_url */

DROP TABLE IF EXISTS ds_url;

CREATE TABLE ds_url (
  ID int(11) NOT NULL AUTO_INCREMENT,
  SQUID_ID int(11) DEFAULT NULL,
  URL varchar(500) DEFAULT NULL,
  USER_NAME varchar(50) DEFAULT NULL,
  PASSWORD varchar(50) DEFAULT NULL,
  MAX_FETCH_DEPTH int(11) DEFAULT NULL,
  FILTER varchar(200) DEFAULT NULL,
  DOMAIN varchar(50) DEFAULT NULL,
  DOMAIN_LIMIT_FLAG int(11) DEFAULT NULL,
  PRIMARY KEY (ID),
  KEY IDX_URL_SQUID_ID (SQUID_ID)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

/*Data for the table ds_url */

/*Table structure for table ds_userdefined_datamap_column */

DROP TABLE IF EXISTS ds_userdefined_datamap_column;

CREATE TABLE ds_userdefined_datamap_column (
  id int(11) NOT NULL AUTO_INCREMENT,
  squid_id int(11) DEFAULT NULL,
  name varchar(255) DEFAULT NULL,
  type int(11) DEFAULT NULL,
  column_id int(11) DEFAULT NULL,
  scale int(11) DEFAULT NULL,
  `precision` int(11) DEFAULT NULL,
  description varchar(255) DEFAULT NULL,
  PRIMARY KEY (id),
  KEY data_mapping_column_index (id,squid_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

/*Data for the table ds_userdefined_datamap_column */

/*Table structure for table ds_userdefined_parameters_column */

DROP TABLE IF EXISTS ds_userdefined_parameters_column;

CREATE TABLE ds_userdefined_parameters_column (
  id int(11) NOT NULL AUTO_INCREMENT,
  squid_id int(11) DEFAULT NULL,
  name varchar(255) DEFAULT NULL,
  value longtext,
  description varchar(255) DEFAULT NULL,
  PRIMARY KEY (id),
  KEY parameter_column_index (id,squid_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

/*Data for the table ds_userdefined_parameters_column */

/*Table structure for table ds_variable */

DROP TABLE IF EXISTS ds_variable;

CREATE TABLE ds_variable (
  ID int(11) NOT NULL AUTO_INCREMENT,
  PROJECT_ID int(11) DEFAULT NULL,
  SQUID_FLOW_ID int(11) DEFAULT NULL,
  SQUID_ID int(11) DEFAULT NULL,
  VARIABLE_SCOPE int(11) DEFAULT NULL,
  VARIABLE_NAME varchar(200) DEFAULT NULL,
  VARIABLE_TYPE int(11) DEFAULT NULL,
  VARIABLE_LENGTH int(11) DEFAULT NULL,
  VARIABLE_PRECISION int(11) DEFAULT NULL,
  VARIABLE_SCALE int(11) DEFAULT NULL,
  VARIABLE_VALUE varchar(500) DEFAULT NULL,
  ADD_DATE timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (ID)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

/*Data for the table ds_variable */

/*Table structure for table ds_web_connection */

DROP TABLE IF EXISTS ds_web_connection;

CREATE TABLE ds_web_connection (
  ID int(11) NOT NULL AUTO_INCREMENT,
  MAX_THREADS int(11) DEFAULT NULL,
  MAX_FETCH_DEPTH int(11) DEFAULT NULL,
  DOMAIN_LIMIT_FLAG int(11) DEFAULT NULL,
  START_DATA_DATE date DEFAULT NULL,
  END_DATA_DATE date DEFAULT NULL,
  PRIMARY KEY (ID)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

/*Data for the table ds_web_connection */

/*Table structure for table ds_webservice_connection */

DROP TABLE IF EXISTS ds_webservice_connection;

CREATE TABLE ds_webservice_connection (
  ID int(11) NOT NULL AUTO_INCREMENT,
  IS_RESTFUL char(1) DEFAULT NULL,
  ADDRESS varchar(200) DEFAULT NULL,
  PRIMARY KEY (ID)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

/*Data for the table ds_webservice_connection */

/*Table structure for table ds_weibo_connection */

DROP TABLE IF EXISTS ds_weibo_connection;

CREATE TABLE ds_weibo_connection (
  ID int(11) NOT NULL AUTO_INCREMENT,
  USER_NAME varchar(100) DEFAULT NULL,
  PASSWORD varchar(100) DEFAULT NULL,
  START_DATA_DATE date DEFAULT NULL,
  END_DATA_DATE date DEFAULT NULL,
  SERVICE_PROVIDER int(11) DEFAULT NULL,
  PRIMARY KEY (ID)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

/*Data for the table ds_weibo_connection */

/*Table structure for table history_version_data */

DROP TABLE IF EXISTS history_version_data;

CREATE TABLE history_version_data (
  ID int(11) NOT NULL AUTO_INCREMENT,
  SQUIDFLOWHISTORYID int(11) DEFAULT NULL,
  SQUID_FLOW_ID int(11) DEFAULT NULL,
  REPOSITORY_ID int(11) DEFAULT NULL,
  COMMIT_TIME timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  DATA longtext,
  SQUIDTYPE int(11) DEFAULT NULL,
  PROCESSTYPE int(11) DEFAULT NULL,
  SQUID_FLOW_TYPE int(11) DEFAULT NULL,
  PRIMARY KEY (ID),
  UNIQUE KEY VERSION_DATA_ID (ID),
  KEY DATA_FLOWID_REPID_PROTYPE (SQUIDFLOWHISTORYID,SQUID_FLOW_ID,REPOSITORY_ID,PROCESSTYPE,SQUID_FLOW_TYPE)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

/*Data for the table history_version_data */

/*Table structure for table squid_flow_history */

DROP TABLE IF EXISTS squid_flow_history;

CREATE TABLE squid_flow_history (
  ID int(11) NOT NULL AUTO_INCREMENT,
  SQUID_FLOW_ID int(11) DEFAULT NULL,
  SQUID_FLOW_NAME varchar(255) DEFAULT NULL,
  PROJECT_ID int(11) DEFAULT NULL,
  PROJECT_NAME varchar(255) DEFAULT NULL,
  REPOSITORY_ID int(11) DEFAULT NULL,
  USER_ID int(11) DEFAULT NULL,
  COMMIT_TIME timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  COMMENTS varchar(4000) DEFAULT NULL,
  VERSION int(11) DEFAULT NULL,
  SQUID_FLOW_TYPE int(11) DEFAULT NULL,
  SUBMIT_USERNAME varchar(255) DEFAULT NULL,
  FIELD_TYPE int(5) DEFAULT NULL,
  PRIMARY KEY (ID),
  UNIQUE KEY SQUID_FLOW_HISTORY_ID (ID),
  KEY HISTORY_SQUIDFLOWID_PROJECTID_REP_ID (SQUID_FLOW_ID,PROJECT_ID,REPOSITORY_ID)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

/*Data for the table squid_flow_history */

/*Table structure for table third_jar_definition */

DROP TABLE IF EXISTS third_jar_definition;

CREATE TABLE third_jar_definition (
  id int(11) NOT NULL AUTO_INCREMENT,
  alias_name varchar(100) NOT NULL,
  class_name varchar(100) NOT NULL,
  data_mapping longtext,
  parameter longtext,
  output_mapping longtext,
  author varchar(50) DEFAULT NULL COMMENT '作者',
  create_date datetime DEFAULT NULL COMMENT '创建时间',
  update_date datetime DEFAULT NULL COMMENT '更新时间',
  PRIMARY KEY (id),
  UNIQUE KEY alias_name (alias_name),
  KEY third_jar_definition_index (id,alias_name,class_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

/*Data for the table third_jar_definition */

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;
