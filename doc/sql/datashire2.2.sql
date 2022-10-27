UPDATE DS_SYS_SERVER_PARAMETER SET value='2.2.0' where name = 'VERSION';

-- 2017-7-12 增加标准化 PLS等Squid
insert into ds_squid_type(ID,CODE,DESCRIPTION) value (65,67,'NORMALIZER');
insert into ds_squid_type(ID,CODE,DESCRIPTION) value (66,68,'PLS');
insert into ds_squid_type(ID,CODE,DESCRIPTION) value (67,69,'DATAVIEW');


-- 更新表结构

-- 停用外键约束
SET FOREIGN_KEY_CHECKS=0;
ALTER TABLE ds_squid DROP COLUMN `key`;

-- 注释Squid
alter table ds_squid add CONTENT varchar(4000);
alter table ds_squid add CONTENT_FONT_SIZE int(11);
alter table ds_squid add CONTENT_FONT_COLOR varchar(100);
alter table ds_squid add CONTENT_FONT_FAMILY varchar(100);
alter table ds_squid add HORIZONTAL_CONTENT_ALIGNMENT int(11);
alter table ds_squid add VERTICAL_CONTENT_ALIGNMENT int(11);
alter table ds_squid add IS_CONTENT_BOLD char(1);
alter table ds_squid add IS_CONTENT_ITALIC char(1);
alter table ds_squid add IS_CONTENT_UNDERLINE char(1);

replace into ds_squid (ID, CONTENT, CONTENT_FONT_SIZE, CONTENT_FONT_COLOR, CONTENT_FONT_FAMILY,
						HORIZONTAL_CONTENT_ALIGNMENT, VERTICAL_CONTENT_ALIGNMENT, IS_CONTENT_BOLD, IS_CONTENT_ITALIC, IS_CONTENT_UNDERLINE,
						SQUID_FLOW_ID, NAME, DESCRIPTION, SQUID_TYPE_ID,  LOCATION_X, LOCATION_Y, SQUID_HEIGHT, SQUID_WIDTH, TABLE_NAME, IS_SHOW_ALL, SOURCE_IS_SHOW_ALL,  /*这里往下是主表*/
						FILTER, ENCODING, DESIGN_STATUS, MAX_TRAVEL_DEPTH)
                select  B.ID, B.CONTENT, B.CONTENT_FONT_SIZE, B.CONTENT_FONT_COLOR, B.CONTENT_FONT_FAMILY, B.HORIZONTAL_CONTENT_ALIGNMENT,
                        B.VERTICAL_CONTENT_ALIGNMENT, B.IS_CONTENT_BOLD, B.IS_CONTENT_ITALIC, B.IS_CONTENT_UNDERLINE,
                        A.SQUID_FLOW_ID, A.NAME, A.DESCRIPTION, A.SQUID_TYPE_ID, A.LOCATION_X, A.LOCATION_Y, A.SQUID_HEIGHT, A.SQUID_WIDTH, A.TABLE_NAME,  /*这里往下是主表*/
                        A.IS_SHOW_ALL, A.SOURCE_IS_SHOW_ALL, A.FILTER, A.ENCODING, A.DESIGN_STATUS, A.MAX_TRAVEL_DEPTH from ds_annotation_squid as B inner join ds_squid as A on B.ID = A.ID;

-- Cassandra 链接 SquidModelBase
alter table ds_squid add DB_TYPE_ID int(11);
alter table ds_squid add HOST varchar(100);
alter table ds_squid add PORT varchar(10);
alter table ds_squid add USERNAME varchar(100);
alter table ds_squid add PASSWORD varchar(100);
alter table ds_squid add KEYSPACE varchar(500);
alter table ds_squid add CLUSTER varchar(500);
alter table ds_squid add VERIFICATION_MODE int(11);

replace into ds_squid (ID, DB_TYPE_ID, `HOST`, PORT, USERNAME, PASSWORD,
						KEYSPACE, `CLUSTER`, VERIFICATION_MODE,
						SQUID_FLOW_ID, NAME, DESCRIPTION, SQUID_TYPE_ID,  LOCATION_X, LOCATION_Y, SQUID_HEIGHT, SQUID_WIDTH, TABLE_NAME, IS_SHOW_ALL, SOURCE_IS_SHOW_ALL,  /*这里往下是主表*/
						FILTER, ENCODING, DESIGN_STATUS, MAX_TRAVEL_DEPTH)
                select  B.ID, B.DB_TYPE_ID, B.`HOST`, B.PORT, B.USERNAME, B.PASSWORD,
                        B.KEYSPACE, B.`CLUSTER`, B.VERIFICATION_MODE,
                        A.SQUID_FLOW_ID, A.NAME, A.DESCRIPTION, A.SQUID_TYPE_ID, A.LOCATION_X, A.LOCATION_Y, A.SQUID_HEIGHT, A.SQUID_WIDTH, A.TABLE_NAME,  /*这里往下是主表*/
                        A.IS_SHOW_ALL, A.SOURCE_IS_SHOW_ALL, A.FILTER, A.ENCODING, A.DESIGN_STATUS, A.MAX_TRAVEL_DEPTH from ds_cassandra_sql_connection as B inner join ds_squid as A on B.ID = A.ID;

-- Data SquidModelBase
alter table ds_squid add IS_INCREMENTAL char(1);
alter table ds_squid add INCREMENTAL_EXPRESSION varchar(4000);
alter table ds_squid add IS_PERSISTED char(1);

alter table ds_squid add DESTINATION_SQUID_ID int(11);
alter table ds_squid add IS_INDEXED char(1);
alter table ds_squid add TOP_N int(11);
alter table ds_squid add TRUNCATE_EXISTING_DATA_FLAG int(11);
alter table ds_squid add PROCESS_MODE int(2);
alter table ds_squid add CDC int(2);
alter table ds_squid add EXCEPTION_HANDLING_FLAG int(2);
alter table ds_squid add LOG_FORMAT int(2);
alter table ds_squid add POST_PROCESS int(2);
alter table ds_squid add XSD_DTD_FILE varchar(100);
alter table ds_squid add XSD_DTD_PATH varchar(200);
alter table ds_squid add SOURCE_TABLE_ID int(11);
alter table ds_squid add UNION_ALL_FLAG int(2);
alter table ds_squid add IS_DISTINCT int(2);
alter table ds_squid add REF_SQUID_ID int(11);
alter table ds_squid add SPLIT_COL varchar(128);
alter table ds_squid add COLSPLIT_NUMUMN int(11);
alter table ds_squid add SPLIT_NUM int(11);
alter table ds_squid add WINDOW_DURATION bigint(20);
alter table ds_squid add ENABLE_WINDOW char(1);
alter table ds_squid add GROUP_COLUMN longtext;
alter table ds_squid add SORT_COLUMN longtext;
alter table ds_squid add TAGGING_COLUMN longtext;
alter table ds_squid add incremental_mode int(2);
alter table ds_squid add check_column_id int(11);
alter table ds_squid add last_value longtext;

replace into ds_squid (ID, IS_INCREMENTAL, INCREMENTAL_EXPRESSION, IS_PERSISTED, DESTINATION_SQUID_ID,
						IS_INDEXED, TOP_N, TRUNCATE_EXISTING_DATA_FLAG, PROCESS_MODE, CDC, EXCEPTION_HANDLING_FLAG, LOG_FORMAT, POST_PROCESS, XSD_DTD_FILE,
						XSD_DTD_PATH, SOURCE_TABLE_ID, UNION_ALL_FLAG, IS_DISTINCT, REF_SQUID_ID, SPLIT_COL, COLSPLIT_NUMUMN, SPLIT_NUM, WINDOW_DURATION,
						ENABLE_WINDOW, GROUP_COLUMN, SORT_COLUMN, TAGGING_COLUMN, incremental_mode, check_column_id, last_value,
						SQUID_FLOW_ID, NAME, DESCRIPTION, SQUID_TYPE_ID,  LOCATION_X, LOCATION_Y, SQUID_HEIGHT, SQUID_WIDTH, TABLE_NAME, IS_SHOW_ALL, SOURCE_IS_SHOW_ALL,  /*这里往下是主表*/
						FILTER, ENCODING, DESIGN_STATUS, MAX_TRAVEL_DEPTH)
                select  B.ID, B.IS_INCREMENTAL, B.INCREMENTAL_EXPRESSION, B.IS_PERSISTED, B.DESTINATION_SQUID_ID, B.IS_INDEXED,
                        B.TOP_N, B.TRUNCATE_EXISTING_DATA_FLAG, B.PROCESS_MODE, B.CDC, B.EXCEPTION_HANDLING_FLAG, B.LOG_FORMAT, B.POST_PROCESS, B.XSD_DTD_FILE,
						B.XSD_DTD_PATH, B.SOURCE_TABLE_ID, B.UNION_ALL_FLAG, B.IS_DISTINCT, B.REF_SQUID_ID, B.SPLIT_COL, B.COLSPLIT_NUMUMN, B.SPLIT_NUM, B.WINDOW_DURATION,
						B.ENABLE_WINDOW, B.GROUP_COLUMN, B.SORT_COLUMN, B.TAGGING_COLUMN, B.incremental_mode, B.check_column_id, B.last_value,
                        A.SQUID_FLOW_ID, A.NAME, A.DESCRIPTION, A.SQUID_TYPE_ID, A.LOCATION_X, A.LOCATION_Y, A.SQUID_HEIGHT, A.SQUID_WIDTH, A.TABLE_NAME,  /*这里往下是主表*/
                        A.IS_SHOW_ALL, A.SOURCE_IS_SHOW_ALL, A.FILTER, A.ENCODING, A.DESIGN_STATUS, A.MAX_TRAVEL_DEPTH from ds_data_squid as B inner join ds_squid as A on B.ID = A.ID;

-- Cassandra 落地 SquidModelBase
alter table ds_squid add save_type int(2);
alter table ds_squid add dest_squid_id int(11);

replace into ds_squid (ID, save_type, table_name, dest_squid_id,
						SQUID_FLOW_ID, NAME, DESCRIPTION, SQUID_TYPE_ID,  LOCATION_X, LOCATION_Y, SQUID_HEIGHT, SQUID_WIDTH,  IS_SHOW_ALL, SOURCE_IS_SHOW_ALL,  /*这里往下是主表*/
						FILTER, ENCODING, DESIGN_STATUS, MAX_TRAVEL_DEPTH)
                select  B.ID, B.save_type, B.table_name, B.dest_squid_id,
                        A.SQUID_FLOW_ID, A.NAME, A.DESCRIPTION, A.SQUID_TYPE_ID, A.LOCATION_X, A.LOCATION_Y, A.SQUID_HEIGHT, A.SQUID_WIDTH,   /*这里往下是主表*/
                        A.IS_SHOW_ALL, A.SOURCE_IS_SHOW_ALL, A.FILTER, A.ENCODING, A.DESIGN_STATUS, A.MAX_TRAVEL_DEPTH from ds_dest_cassandra_squid as B inner join ds_squid as A on B.ID = A.ID;

-- ES 落地 SquidModelBase
alter table ds_squid add ip varchar(20);
alter table ds_squid add ESINDEX varchar(200);
alter table ds_squid add ESTYPE varchar(200);

replace into ds_squid (ID, ip, ESINDEX, ESTYPE,
						SQUID_FLOW_ID, NAME, DESCRIPTION, SQUID_TYPE_ID,  LOCATION_X, LOCATION_Y, SQUID_HEIGHT, SQUID_WIDTH, TABLE_NAME, IS_SHOW_ALL, SOURCE_IS_SHOW_ALL,  /*这里往下是主表*/
						FILTER, ENCODING, DESIGN_STATUS, MAX_TRAVEL_DEPTH)
                select  B.ID, B.ip, B.ESINDEX, B.ESTYPE,
                        A.SQUID_FLOW_ID, A.NAME, A.DESCRIPTION, A.SQUID_TYPE_ID, A.LOCATION_X, A.LOCATION_Y, A.SQUID_HEIGHT, A.SQUID_WIDTH, A.TABLE_NAME,  /*这里往下是主表*/
                        A.IS_SHOW_ALL, A.SOURCE_IS_SHOW_ALL, A.FILTER, A.ENCODING, A.DESIGN_STATUS, A.MAX_TRAVEL_DEPTH from ds_dest_es_squid as B inner join ds_squid as A on B.ID = A.ID;

-- HDFS 落地 SquidModelBase
-- 重复 alter table ds_squid add HOST varchar(20);
alter table ds_squid add HDFS_PATH varchar(200);
alter table ds_squid add FILE_FORMATE int(2);
alter table ds_squid add ZIP_TYPE int(2);
-- 重复 alter table ds_squid add SAVE_TYPE int(2);
alter table ds_squid add ROW_DELIMITER varchar(500);
alter table ds_squid add COLUMN_DELIMITER varchar(30);

replace into ds_squid (ID, HDFS_PATH, FILE_FORMATE, ZIP_TYPE, SAVE_TYPE, ROW_DELIMITER, COLUMN_DELIMITER,
						SQUID_FLOW_ID, NAME, DESCRIPTION, SQUID_TYPE_ID,  LOCATION_X, LOCATION_Y, SQUID_HEIGHT, SQUID_WIDTH, TABLE_NAME, IS_SHOW_ALL, SOURCE_IS_SHOW_ALL,  /*这里往下是主表*/
						FILTER, ENCODING, DESIGN_STATUS, MAX_TRAVEL_DEPTH)
                select  B.ID, B.HDFS_PATH, B.FILE_FORMATE, B.ZIP_TYPE, B.SAVE_TYPE, B.ROW_DELIMITER, B.COLUMN_DELIMITER,
                        A.SQUID_FLOW_ID, A.NAME, A.DESCRIPTION, A.SQUID_TYPE_ID, A.LOCATION_X, A.LOCATION_Y, A.SQUID_HEIGHT, A.SQUID_WIDTH, A.TABLE_NAME,  /*这里往下是主表*/
                        A.IS_SHOW_ALL, A.SOURCE_IS_SHOW_ALL, A.FILTER, A.ENCODING, A.DESIGN_STATUS, A.MAX_TRAVEL_DEPTH from ds_dest_hdfs_squid as B inner join ds_squid as A on B.ID = A.ID;

-- Hive 落地 SquidModelBase
alter table ds_squid add db_name varchar(100);

replace into ds_squid (ID, save_type, table_name, db_name,
						SQUID_FLOW_ID, NAME, DESCRIPTION, SQUID_TYPE_ID,  LOCATION_X, LOCATION_Y, SQUID_HEIGHT, SQUID_WIDTH, IS_SHOW_ALL, SOURCE_IS_SHOW_ALL,  /*这里往下是主表*/
						FILTER, ENCODING, DESIGN_STATUS, MAX_TRAVEL_DEPTH)
                select  B.ID, B.save_type, B.table_name, B.db_name,
                        A.SQUID_FLOW_ID, A.NAME, A.DESCRIPTION, A.SQUID_TYPE_ID, A.LOCATION_X, A.LOCATION_Y, A.SQUID_HEIGHT, A.SQUID_WIDTH,/*这里往下是主表*/
                        A.IS_SHOW_ALL, A.SOURCE_IS_SHOW_ALL, A.FILTER, A.ENCODING, A.DESIGN_STATUS, A.MAX_TRAVEL_DEPTH from ds_dest_hive_squid as B inner join ds_squid as A on B.ID = A.ID;

-- impala 落地 SquidModelBase
-- 重复 alter table ds_squid add HOST varchar(50);
alter table ds_squid add STORE_NAME varchar(50);
alter table ds_squid add IMPALA_TABLE_NAME varchar(50);
alter table ds_squid add AUTHENTICATION_TYPE int(2);

replace into ds_squid (ID, HOST, STORE_NAME, IMPALA_TABLE_NAME, AUTHENTICATION_TYPE,
						SQUID_FLOW_ID, NAME, DESCRIPTION, SQUID_TYPE_ID,  LOCATION_X, LOCATION_Y, SQUID_HEIGHT, SQUID_WIDTH, TABLE_NAME, IS_SHOW_ALL, SOURCE_IS_SHOW_ALL,  /*这里往下是主表*/
						FILTER, ENCODING, DESIGN_STATUS, MAX_TRAVEL_DEPTH)
                select  B.ID, B.HOST, B.STORE_NAME, B.IMPALA_TABLE_NAME, B.AUTHENTICATION_TYPE,
                        A.SQUID_FLOW_ID, A.NAME, A.DESCRIPTION, A.SQUID_TYPE_ID, A.LOCATION_X, A.LOCATION_Y, A.SQUID_HEIGHT, A.SQUID_WIDTH, A.TABLE_NAME,  /*这里往下是主表*/
                        A.IS_SHOW_ALL, A.SOURCE_IS_SHOW_ALL, A.FILTER, A.ENCODING, A.DESIGN_STATUS, A.MAX_TRAVEL_DEPTH from ds_dest_impala_squid as B inner join ds_squid as A on B.ID = A.ID;

-- Datamining SquidModelBase
alter table ds_squid add TRAINING_PERCENTAGE double(64,15);
alter table ds_squid add VERSIONING int(2);
alter table ds_squid add MIN_BATCH_FRACTION double(64,15);
alter table ds_squid add ITERATION_NUMBER int(2);
alter table ds_squid add STEP_SIZE double(64,15);
alter table ds_squid add SMOOTHING_PARAMETER double(64,15);
alter table ds_squid add REGULARIZATION double(64,15);
alter table ds_squid add K double;
alter table ds_squid add PARALLEL_RUNS double;
alter table ds_squid add INITIALIZATION_MODE double;
alter table ds_squid add IMPLICIT_PREFERENCES double;
alter table ds_squid add CASE_SENSITIVE double;
alter table ds_squid add MIN_VALUE double(64,15);
alter table ds_squid add MAX_VALUE double(64,15);
alter table ds_squid add BUCKET_COUNT int(11);
alter table ds_squid add SEED int(11);
alter table ds_squid add ALGORITHM int(11);
alter table ds_squid add MAX_DEPTH int(11);
alter table ds_squid add IMPURITY int(11);
alter table ds_squid add MAX_BINS int(11);
alter table ds_squid add CATEGORICAL_SQUID int(11);
alter table ds_squid add MIN_SUPPORT double(64,15);
alter table ds_squid add MIN_CONFIDENCE double(64,15);
alter table ds_squid add max_integer_number int(11);
alter table ds_squid add aggregation_depth int(11);
alter table ds_squid add fit_intercept int(11);
alter table ds_squid add solver int(11);
alter table ds_squid add standardization int(11);
alter table ds_squid add tolerance double;
alter table ds_squid add tree_number int(11);
alter table ds_squid add feature_subset_strategy int(11);
alter table ds_squid add min_info_gain double;
alter table ds_squid add subsampling_rate double;
alter table ds_squid add initialweights longtext;
alter table ds_squid add layers longtext;
alter table ds_squid add max_categories longtext;
alter table ds_squid add feature_subset_scale double;

replace into ds_squid (ID, TRAINING_PERCENTAGE, VERSIONING, MIN_BATCH_FRACTION, ITERATION_NUMBER, STEP_SIZE, SMOOTHING_PARAMETER, REGULARIZATION, K, PARALLEL_RUNS, INITIALIZATION_MODE,
						IMPLICIT_PREFERENCES, CASE_SENSITIVE, MIN_VALUE, MAX_VALUE, BUCKET_COUNT, SEED, ALGORITHM, MAX_DEPTH, IMPURITY, MAX_BINS, CATEGORICAL_SQUID, MIN_SUPPORT, MIN_CONFIDENCE, max_integer_number,
						aggregation_depth, fit_intercept, solver, standardization, tolerance, tree_number, feature_subset_strategy, min_info_gain, subsampling_rate, initialweights, layers,
						max_categories, feature_subset_scale,
						SQUID_FLOW_ID, NAME, DESCRIPTION, SQUID_TYPE_ID,  LOCATION_X, LOCATION_Y, SQUID_HEIGHT, SQUID_WIDTH, TABLE_NAME, IS_SHOW_ALL, SOURCE_IS_SHOW_ALL,  /*这里往下是主表*/
						FILTER, ENCODING, DESIGN_STATUS, MAX_TRAVEL_DEPTH)
                select  B.ID, B.TRAINING_PERCENTAGE, B.VERSIONING, B.MIN_BATCH_FRACTION, B.ITERATION_NUMBER, B.STEP_SIZE, B.SMOOTHING_PARAMETER, B.REGULARIZATION, B.K, B.PARALLEL_RUNS, B.INITIALIZATION_MODE,
						B.IMPLICIT_PREFERENCES, B.CASE_SENSITIVE, B.MIN_VALUE, B.MAX_VALUE, B.BUCKET_COUNT, B.SEED, B.ALGORITHM, B.MAX_DEPTH, B.IMPURITY, B.MAX_BINS, B.CATEGORICAL_SQUID, B.MIN_SUPPORT, B.MIN_CONFIDENCE, B.max_integer_number,
						B.aggregation_depth, B.fit_intercept, B.solver, B.standardization, B.tolerance, B.tree_number, B.feature_subset_strategy, B.min_info_gain, B.subsampling_rate, B.initialweights, B.layers,
						B.max_categories, B.feature_subset_scale,
                        A.SQUID_FLOW_ID, A.NAME, A.DESCRIPTION, A.SQUID_TYPE_ID, A.LOCATION_X, A.LOCATION_Y, A.SQUID_HEIGHT, A.SQUID_WIDTH, A.TABLE_NAME,  /*这里往下是主表*/
                        A.IS_SHOW_ALL, A.SOURCE_IS_SHOW_ALL, A.FILTER, A.ENCODING, A.DESIGN_STATUS, A.MAX_TRAVEL_DEPTH from ds_dm_squid as B inner join ds_squid as A on B.ID = A.ID;

-- 文档抽取 SquidModelBase
-- 重复 alter table ds_squid add IS_PERSISTED char(1);
-- 重复 alter table ds_squid add TABLE_NAME varchar(500);
-- 重复 alter table ds_squid add DESTINATION_SQUID_ID int(11);
-- 重复 alter table ds_squid add IS_INDEXED char(1);
-- 重复 alter table ds_squid add TOP_N int(11);
-- 重复 alter table ds_squid add TRUNCATE_EXISTING_DATA_FLAG int(11);
-- 重复 alter table ds_squid add PROCESS_MODE int(11);
-- 重复 alter table ds_squid add CDC int(11);
-- 重复 alter table ds_squid add EXCEPTION_HANDLING_FLAG int(11);
alter table ds_squid add DOC_FORMAT int(2);
alter table ds_squid add ROW_FORMAT int(2);
alter table ds_squid add DELIMITER varchar(300);
alter table ds_squid add FIELD_LENGTH int(11);
alter table ds_squid add HEADER_ROW_NO int(11);
alter table ds_squid add FIRST_DATA_ROW_NO int(11);
-- 重复 alter table ds_squid add ROW_DELIMITER varchar(500);
alter table ds_squid add ROW_DELIMITER_POSITION int(11);
alter table ds_squid add SKIP_ROWS int(11);
-- 重复 alter table ds_squid add POST_PROCESS int(11);
-- 重复 alter table ds_squid add SOURCE_TABLE_ID int(11);
-- 重复 alter table ds_squid add UNION_ALL_FLAG int(11);
alter table ds_squid add COMPRESSICON_CODEC int(11);

replace into ds_squid (ID, IS_PERSISTED, TABLE_NAME, DESTINATION_SQUID_ID, IS_INDEXED, TOP_N, TRUNCATE_EXISTING_DATA_FLAG, PROCESS_MODE, CDC, EXCEPTION_HANDLING_FLAG, DOC_FORMAT,
						ROW_FORMAT, DELIMITER, FIELD_LENGTH, HEADER_ROW_NO, FIRST_DATA_ROW_NO, ROW_DELIMITER, ROW_DELIMITER_POSITION, SKIP_ROWS, POST_PROCESS, SOURCE_TABLE_ID, UNION_ALL_FLAG, COMPRESSICON_CODEC,
						SQUID_FLOW_ID, NAME, DESCRIPTION, SQUID_TYPE_ID,  LOCATION_X, LOCATION_Y, SQUID_HEIGHT, SQUID_WIDTH, IS_SHOW_ALL, SOURCE_IS_SHOW_ALL,  /*这里往下是主表*/
						FILTER, ENCODING, DESIGN_STATUS, MAX_TRAVEL_DEPTH)
                select  B.ID, B.IS_PERSISTED, B.TABLE_NAME, B.DESTINATION_SQUID_ID, B.IS_INDEXED, B.TOP_N, B.TRUNCATE_EXISTING_DATA_FLAG, B.PROCESS_MODE, B.CDC, B.EXCEPTION_HANDLING_FLAG, B.DOC_FORMAT,
						B.ROW_FORMAT, B.DELIMITER, B.FIELD_LENGTH, B.HEADER_ROW_NO, B.FIRST_DATA_ROW_NO, B.ROW_DELIMITER, B.ROW_DELIMITER_POSITION, B.SKIP_ROWS, B.POST_PROCESS, B.SOURCE_TABLE_ID, B.UNION_ALL_FLAG, B.COMPRESSICON_CODEC,
                        A.SQUID_FLOW_ID, A.NAME, A.DESCRIPTION, A.SQUID_TYPE_ID, A.LOCATION_X, A.LOCATION_Y, A.SQUID_HEIGHT, A.SQUID_WIDTH,   /*这里往下是主表*/
                        A.IS_SHOW_ALL, A.SOURCE_IS_SHOW_ALL, A.FILTER, A.ENCODING, A.DESIGN_STATUS, A.MAX_TRAVEL_DEPTH from ds_doc_extract as B inner join ds_squid as A on B.ID = A.ID;


-- 文件夹链接 SquidModelBase
alter table ds_squid add USER_NAME varchar(100);
alter table ds_squid add FILE_PATH varchar(500);
alter table ds_squid add INCLUDING_SUBFOLDERS_FLAG int(2);
alter table ds_squid add UNIONALL_FLAG int(2);

replace into ds_squid (ID, host, USER_NAME, PASSWORD, FILE_PATH, INCLUDING_SUBFOLDERS_FLAG, UNIONALL_FLAG,
						SQUID_FLOW_ID, NAME, DESCRIPTION, SQUID_TYPE_ID,  LOCATION_X, LOCATION_Y, SQUID_HEIGHT, SQUID_WIDTH, IS_SHOW_ALL, SOURCE_IS_SHOW_ALL,  /*这里往下是主表*/
						FILTER, ENCODING, DESIGN_STATUS, MAX_TRAVEL_DEPTH)
                select  B.ID, B.host, B.USER_NAME, B.PASSWORD, B.FILE_PATH, B.INCLUDING_SUBFOLDERS_FLAG, B.UNIONALL_FLAG,
                        A.SQUID_FLOW_ID, A.NAME, A.DESCRIPTION, A.SQUID_TYPE_ID, A.LOCATION_X, A.LOCATION_Y, A.SQUID_HEIGHT, A.SQUID_WIDTH,   /*这里往下是主表*/
                        A.IS_SHOW_ALL, A.SOURCE_IS_SHOW_ALL, A.FILTER, A.ENCODING, A.DESIGN_STATUS, A.MAX_TRAVEL_DEPTH from ds_filefolder_connection as B inner join ds_squid as A on B.ID = A.ID;

-- FTP 链接 SquidModelBase
-- 重复 alter table ds_squid add host varchar(20);
-- 重复 alter table ds_squid add USER_NAME varchar(100);
-- 重复 alter table ds_squid add PASSWORD varchar(100);
-- 重复 alter table ds_squid add FILE_PATH varchar(500);
-- 重复 alter table ds_squid add INCLUDING_SUBFOLDERS_FLAG int(11);
-- 重复 alter table ds_squid add UNIONALL_FLAG int(11);
alter table ds_squid add POSTPROCESS int(11);
alter table ds_squid add PROTOCOL int(11);
alter table ds_squid add ENCRYPTION int(11);
alter table ds_squid add ALLOWANONYMOUS_FLAG int(11);
alter table ds_squid add MAXCONNECTIONS int(11);
alter table ds_squid add TRANSFERMODE_FLAG int(11);

replace into ds_squid (ID, host, USER_NAME, PASSWORD, FILE_PATH, INCLUDING_SUBFOLDERS_FLAG, UNIONALL_FLAG, POSTPROCESS, PROTOCOL, ENCRYPTION, ALLOWANONYMOUS_FLAG, MAXCONNECTIONS, TRANSFERMODE_FLAG,
						SQUID_FLOW_ID, NAME, DESCRIPTION, SQUID_TYPE_ID,  LOCATION_X, LOCATION_Y, SQUID_HEIGHT, SQUID_WIDTH, IS_SHOW_ALL, SOURCE_IS_SHOW_ALL,  /*这里往下是主表*/
						FILTER, ENCODING, DESIGN_STATUS, MAX_TRAVEL_DEPTH)
                select  B.ID, B.host, B.USER_NAME, B.PASSWORD, B.FILE_PATH, B.INCLUDING_SUBFOLDERS_FLAG, B.UNIONALL_FLAG,
						B.POSTPROCESS, B.PROTOCOL, B.ENCRYPTION, B.ALLOWANONYMOUS_FLAG, B.MAXCONNECTIONS, B.TRANSFERMODE_FLAG,
                        A.SQUID_FLOW_ID, A.NAME, A.DESCRIPTION, A.SQUID_TYPE_ID, A.LOCATION_X, A.LOCATION_Y, A.SQUID_HEIGHT, A.SQUID_WIDTH,   /*这里往下是主表*/
                        A.IS_SHOW_ALL, A.SOURCE_IS_SHOW_ALL, A.FILTER, A.ENCODING, A.DESIGN_STATUS, A.MAX_TRAVEL_DEPTH from ds_ftp_connection as B inner join ds_squid as A on B.ID = A.ID;


-- HBase 链接 SquidModelBase
alter table ds_squid add URL varchar(200);

replace into ds_squid (ID, URL,
						SQUID_FLOW_ID, NAME, DESCRIPTION, SQUID_TYPE_ID,  LOCATION_X, LOCATION_Y, SQUID_HEIGHT, SQUID_WIDTH, IS_SHOW_ALL, SOURCE_IS_SHOW_ALL,  /*这里往下是主表*/
						FILTER, ENCODING, DESIGN_STATUS, MAX_TRAVEL_DEPTH)
                select  B.ID, B.URL,
                        A.SQUID_FLOW_ID, A.NAME, A.DESCRIPTION, A.SQUID_TYPE_ID, A.LOCATION_X, A.LOCATION_Y, A.SQUID_HEIGHT, A.SQUID_WIDTH,   /*这里往下是主表*/
                        A.IS_SHOW_ALL, A.SOURCE_IS_SHOW_ALL, A.FILTER, A.ENCODING, A.DESIGN_STATUS, A.MAX_TRAVEL_DEPTH from ds_hbase_connection as B inner join ds_squid as A on B.ID = A.ID;

-- HBase 抽取 SquidModelBase
-- 重复 alter table ds_squid add IS_PERSISTED varchar(200);
-- 重复 alter table ds_squid add TABLE_NAME int(11);
-- 重复 alter table ds_squid add DESTINATION_SQUID_ID int(11);
-- 重复 alter table ds_squid add TOP_N int(11);
-- 重复 alter table ds_squid add TRUNCATE_EXISTING_DATA_FLAG int(11);
-- 重复 alter table ds_squid add PROCESS_MODE int(11);
-- 重复 alter table ds_squid add CDC int(11);
-- 重复 alter table ds_squid add EXCEPTION_HANDLING_FLAG int(11);
-- 重复 alter table ds_squid add SOURCE_TABLE_ID int(11);
alter table ds_squid add FILTER_TYPE int(11);
alter table ds_squid add SCAN text;
alter table ds_squid add CODE text;

replace into ds_squid (ID, IS_PERSISTED, TABLE_NAME, DESTINATION_SQUID_ID, TOP_N, TRUNCATE_EXISTING_DATA_FLAG, PROCESS_MODE, CDC, EXCEPTION_HANDLING_FLAG, SOURCE_TABLE_ID,
						FILTER_TYPE, SCAN, CODE,
						SQUID_FLOW_ID, NAME, DESCRIPTION, SQUID_TYPE_ID,  LOCATION_X, LOCATION_Y, SQUID_HEIGHT, SQUID_WIDTH, IS_SHOW_ALL, SOURCE_IS_SHOW_ALL,  /*这里往下是主表*/
						FILTER, ENCODING, DESIGN_STATUS, MAX_TRAVEL_DEPTH)
                select  B.ID, B.IS_PERSISTED, B.TABLE_NAME, B.DESTINATION_SQUID_ID, B.TOP_N, B.TRUNCATE_EXISTING_DATA_FLAG, B.PROCESS_MODE, B.CDC, B.EXCEPTION_HANDLING_FLAG, B.SOURCE_TABLE_ID,
						B.FILTER_TYPE, B.SCAN, B.CODE,
                        A.SQUID_FLOW_ID, A.NAME, A.DESCRIPTION, A.SQUID_TYPE_ID, A.LOCATION_X, A.LOCATION_Y, A.SQUID_HEIGHT, A.SQUID_WIDTH,   /*这里往下是主表*/
                        A.IS_SHOW_ALL, A.SOURCE_IS_SHOW_ALL, A.FILTER, A.ENCODING, A.DESIGN_STATUS, A.MAX_TRAVEL_DEPTH from ds_hbase_extract as B inner join ds_squid as A on B.ID = A.ID;

-- HDFS 链接 SquidModelBase
-- 重复 alter table ds_squid add HOST varchar(20);
-- 重复 alter table ds_squid add USER_NAME varchar(100);
-- 重复 alter table ds_squid add PASSWORD varchar(100);
-- 重复 alter table ds_squid add FILE_PATH varchar(100);
-- 重复 alter table ds_squid add UNIONALL_FLAG int(11);
-- 重复 alter table ds_squid add INCLUDING_SUBFOLDERS_FLAG int(11);

replace into ds_squid (ID, HOST, USER_NAME, PASSWORD, FILE_PATH, UNIONALL_FLAG, INCLUDING_SUBFOLDERS_FLAG,
						SQUID_FLOW_ID, NAME, DESCRIPTION, SQUID_TYPE_ID,  LOCATION_X, LOCATION_Y, SQUID_HEIGHT, SQUID_WIDTH, IS_SHOW_ALL, SOURCE_IS_SHOW_ALL,  /*这里往下是主表*/
						FILTER, ENCODING, DESIGN_STATUS, MAX_TRAVEL_DEPTH)
                select  B.ID, B.HOST, B.USER_NAME, B.PASSWORD, B.FILE_PATH, B.UNIONALL_FLAG, B.INCLUDING_SUBFOLDERS_FLAG,
                        A.SQUID_FLOW_ID, A.NAME, A.DESCRIPTION, A.SQUID_TYPE_ID, A.LOCATION_X, A.LOCATION_Y, A.SQUID_HEIGHT, A.SQUID_WIDTH,   /*这里往下是主表*/
                        A.IS_SHOW_ALL, A.SOURCE_IS_SHOW_ALL, A.FILTER, A.ENCODING, A.DESIGN_STATUS, A.MAX_TRAVEL_DEPTH from ds_hdfs_connection as B inner join ds_squid as A on B.ID = A.ID;

-- HTTP 链接 SquidModelBase  作废

-- Kafka 链接 SquidModelBase
alter table ds_squid add ZKQUORUM varchar(200);

replace into ds_squid (ID, ZKQUORUM,
						SQUID_FLOW_ID, NAME, DESCRIPTION, SQUID_TYPE_ID,  LOCATION_X, LOCATION_Y, SQUID_HEIGHT, SQUID_WIDTH, IS_SHOW_ALL, SOURCE_IS_SHOW_ALL,  /*这里往下是主表*/
						FILTER, ENCODING, DESIGN_STATUS, MAX_TRAVEL_DEPTH)
                select  B.ID, B.ZKQUORUM,
                        A.SQUID_FLOW_ID, A.NAME, A.DESCRIPTION, A.SQUID_TYPE_ID, A.LOCATION_X, A.LOCATION_Y, A.SQUID_HEIGHT, A.SQUID_WIDTH,   /*这里往下是主表*/
                        A.IS_SHOW_ALL, A.SOURCE_IS_SHOW_ALL, A.FILTER, A.ENCODING, A.DESIGN_STATUS, A.MAX_TRAVEL_DEPTH from ds_kafka_connection as B inner join ds_squid as A on B.ID = A.ID;


-- Kafka 抽取 SquidModelBase
-- 重复 alter table ds_squid add IS_PERSISTED char(1);
-- 重复 alter table ds_squid add TABLE_NAME varchar(100);
-- 重复 alter table ds_squid add DESTINATION_SQUID_ID int(11);
alter table ds_squid add NUMPARTITIONS int(11);
alter table ds_squid add GROUP_ID varchar(100);
-- 重复 alter table ds_squid add SOURCE_TABLE_ID int(11);

replace into ds_squid (ID, IS_PERSISTED, TABLE_NAME, DESTINATION_SQUID_ID, NUMPARTITIONS, GROUP_ID, SOURCE_TABLE_ID,
						SQUID_FLOW_ID, NAME, DESCRIPTION, SQUID_TYPE_ID,  LOCATION_X, LOCATION_Y, SQUID_HEIGHT, SQUID_WIDTH, IS_SHOW_ALL, SOURCE_IS_SHOW_ALL,  /*这里往下是主表*/
						FILTER, ENCODING, DESIGN_STATUS, MAX_TRAVEL_DEPTH)
                select  B.ID, B.IS_PERSISTED, B.TABLE_NAME, B.DESTINATION_SQUID_ID, B.NUMPARTITIONS, B.GROUP_ID, B.SOURCE_TABLE_ID,
                        A.SQUID_FLOW_ID, A.NAME, A.DESCRIPTION, A.SQUID_TYPE_ID, A.LOCATION_X, A.LOCATION_Y, A.SQUID_HEIGHT, A.SQUID_WIDTH,   /*这里往下是主表*/
                        A.IS_SHOW_ALL, A.SOURCE_IS_SHOW_ALL, A.FILTER, A.ENCODING, A.DESIGN_STATUS, A.MAX_TRAVEL_DEPTH from ds_kafka_extract as B inner join ds_squid as A on B.ID = A.ID;

-- MongoDB 链接 SquidModelBase
-- 重复 alter table ds_squid add DB_TYPE_ID int(11);
-- 重复 alter table ds_squid add HOST varchar(100);
-- 重复 alter table ds_squid add PORT varchar(10);
-- 重复 alter table ds_squid add USER_NAME varchar(100);
-- 重复 alter table ds_squid add PASSWORD varchar(100);
alter table ds_squid add DATABASE_NAME varchar(100);

replace into ds_squid (ID, DB_TYPE_ID, HOST, PORT, USER_NAME, PASSWORD, DATABASE_NAME,
						SQUID_FLOW_ID, NAME, DESCRIPTION, SQUID_TYPE_ID,  LOCATION_X, LOCATION_Y, SQUID_HEIGHT, SQUID_WIDTH, IS_SHOW_ALL, SOURCE_IS_SHOW_ALL,  /*这里往下是主表*/
						FILTER, ENCODING, DESIGN_STATUS, MAX_TRAVEL_DEPTH)
                select  B.ID, B.DB_TYPE_ID, B.HOST, B.PORT, B.USER_NAME, B.PASSWORD, B.DATABASE_NAME,
                        A.SQUID_FLOW_ID, A.NAME, A.DESCRIPTION, A.SQUID_TYPE_ID, A.LOCATION_X, A.LOCATION_Y, A.SQUID_HEIGHT, A.SQUID_WIDTH,   /*这里往下是主表*/
                        A.IS_SHOW_ALL, A.SOURCE_IS_SHOW_ALL, A.FILTER, A.ENCODING, A.DESIGN_STATUS, A.MAX_TRAVEL_DEPTH from ds_no_sql_connection as B inner join ds_squid as A on B.ID = A.ID;


-- DB Connection SquidModelBase
-- 重复 alter table ds_squid add DB_TYPE_ID int;
-- 重复 alter table ds_squid add HOST int;
-- 重复 alter table ds_squid add PORT int;
-- 重复 alter table ds_squid add USER_NAME int;
-- 重复 alter table ds_squid add PASSWORD int;
-- 重复 alter table ds_squid add DATABASE_NAME int;

replace into ds_squid (ID, DB_TYPE_ID, HOST, PORT, USER_NAME, PASSWORD, DATABASE_NAME,
						SQUID_FLOW_ID, NAME, DESCRIPTION, SQUID_TYPE_ID,  LOCATION_X, LOCATION_Y, SQUID_HEIGHT, SQUID_WIDTH, IS_SHOW_ALL, SOURCE_IS_SHOW_ALL,  /*这里往下是主表*/
						FILTER, ENCODING, DESIGN_STATUS, MAX_TRAVEL_DEPTH)
                select  B.ID, B.DB_TYPE_ID, B.HOST, B.PORT, B.USER_NAME, B.PASSWORD, B.DATABASE_NAME,
                        A.SQUID_FLOW_ID, A.NAME, A.DESCRIPTION, A.SQUID_TYPE_ID, A.LOCATION_X, A.LOCATION_Y, A.SQUID_HEIGHT, A.SQUID_WIDTH,   /*这里往下是主表*/
                        A.IS_SHOW_ALL, A.SOURCE_IS_SHOW_ALL, A.FILTER, A.ENCODING, A.DESIGN_STATUS, A.MAX_TRAVEL_DEPTH from ds_sql_connection as B inner join ds_squid as A on B.ID = A.ID;



-- Report SquidModelBase 作废

-- Statistics SquidModelBase
alter table ds_squid add statistics_name varchar(100);

replace into ds_squid (ID, statistics_name,
						SQUID_FLOW_ID, NAME, DESCRIPTION, SQUID_TYPE_ID,  LOCATION_X, LOCATION_Y, SQUID_HEIGHT, SQUID_WIDTH, IS_SHOW_ALL, SOURCE_IS_SHOW_ALL,  /*这里往下是主表*/
						FILTER, ENCODING, DESIGN_STATUS, MAX_TRAVEL_DEPTH)
                select  B.ID, B.statistics_name,
                        A.SQUID_FLOW_ID, A.NAME, A.DESCRIPTION, A.SQUID_TYPE_ID, A.LOCATION_X, A.LOCATION_Y, A.SQUID_HEIGHT, A.SQUID_WIDTH,   /*这里往下是主表*/
                        A.IS_SHOW_ALL, A.SOURCE_IS_SHOW_ALL, A.FILTER, A.ENCODING, A.DESIGN_STATUS, A.MAX_TRAVEL_DEPTH from ds_statistics_squid as B inner join ds_squid as A on B.ID = A.ID;


-- web 链接 SquidModelBase 作废

-- web service 链接 SquidModelBase 作废

-- weibo 链接 SquidModelBase 作废

alter table ds_squid add selectClassName varchar(255);

replace into ds_squid (ID, selectClassName,
						SQUID_FLOW_ID, NAME, DESCRIPTION, SQUID_TYPE_ID,  LOCATION_X, LOCATION_Y, SQUID_HEIGHT, SQUID_WIDTH, IS_SHOW_ALL, SOURCE_IS_SHOW_ALL,  /*这里往下是主表*/
						FILTER, ENCODING, DESIGN_STATUS, MAX_TRAVEL_DEPTH)
                select  B.ID, B.selectClassName,
                        A.SQUID_FLOW_ID, A.NAME, A.DESCRIPTION, A.SQUID_TYPE_ID, A.LOCATION_X, A.LOCATION_Y, A.SQUID_HEIGHT, A.SQUID_WIDTH,   /*这里往下是主表*/
                        A.IS_SHOW_ALL, A.SOURCE_IS_SHOW_ALL, A.FILTER, A.ENCODING, A.DESIGN_STATUS, A.MAX_TRAVEL_DEPTH from ds_userdefined_squid as B inner join ds_squid as A on B.ID = A.ID;


-- 开启外键约束
SET FOREIGN_KEY_CHECKS=1;


drop table ds_annotation_squid;
drop table ds_cassandra_sql_connection;
drop table ds_data_squid;
drop table ds_dest_cassandra_squid;
drop table ds_dest_es_squid;
drop table ds_dest_hdfs_squid;
drop table ds_dest_hive_squid;
drop table ds_dm_squid;
drop table ds_doc_extract;
drop table ds_filefolder_connection;
drop table ds_ftp_connection;
drop table ds_hbase_connection;
drop table ds_hbase_extract;
drop table ds_hdfs_connection;
drop table ds_kafka_connection;
drop table ds_kafka_extract;
drop table ds_no_sql_connection;
drop table ds_statistics_squid;
drop table ds_userdefined_squid;
drop TABLE ds_sql_connection;



-- 2017-6-27
alter table ds_squid MODIFY k int;
alter table ds_squid MODIFY parallel_runs int;
alter table ds_squid MODIFY INITIALIZATION_MODE int(2);
alter table ds_squid MODIFY implicit_preferences int(2);
alter table ds_squid MODIFY case_sensitive int(2);
alter table ds_squid MODIFY bucket_count int;

-- 2017-6-30
alter table ds_squid ADD x_model_squid_id int;
alter table ds_squid ADD y_model_squid_id int;

-- 2017-7-5
alter table ds_squid ADD method int;

-- 2017-7-12（153上的操作）
alter table ds_transformation DROP `KEY`;
alter table ds_squid_type modify column CODE int(30);

-- 2017-7-13（153上的操作）
alter table ds_reference_column_group DROP `KEY`;

-- 2017-7-25
update ds_tran_input_definition set INPUT_ORDER=1 where CODE = 'INVERSENORMALIZER';

-- 2017-7-31 新版本
UPDATE DS_SYS_SERVER_PARAMETER SET value='2.2.1' where name = 'VERSION';

-- 2017-8-2
INSERT INTO ds_squid_type (ID, CODE, DESCRIPTION) VALUE (68, 70, 'COEFFICIENT');

-- 2017-8-11 新版本
UPDATE DS_SYS_SERVER_PARAMETER SET value='2.2.2' where name = 'VERSION';

-- 2017-8-15
INSERT INTO ds_squid_type (ID, CODE, DESCRIPTION) VALUE (69, 71, 'DECISIONTREEREGRESSION');
INSERT INTO ds_squid_type (ID, CODE, DESCRIPTION) VALUE (70, 72, 'DECISIONTREECLASSIFICATION');

ALTER TABLE ds_squid
  ADD elastic_net_param DOUBLE;
ALTER TABLE ds_squid
  ADD min_instances_per_node INT(11);
ALTER TABLE ds_squid
  ADD family INT(11);

-- 2017-8-21
ALTER TABLE ds_squid
  ADD threshold VARCHAR(100);
