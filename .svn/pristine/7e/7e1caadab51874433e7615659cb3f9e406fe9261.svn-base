drop function if exists validateIsolateSquid;
drop function if exists validateIsolateTransformation;
drop function if exists validateIsolateColumn;
drop function if exists validateDestinationTableName;
drop function if exists validateSquidIndexes;
drop function if exists validateHostAddress;
drop function if exists validateRDBMSType;
drop function if exists validateDataBaseName;
drop function if exists validateFilePath;
drop function if exists validateServiceProvider;
drop function if exists validateURLList;
drop function if exists validateDelimiter;
drop function if exists validateFieldLength;
drop function if exists validateWebLogExtractSquidSourceData;
drop function if exists validatePublishingFolder;
drop function if exists validateTemplateDefinition;
drop function if exists validateTrainingPercentage;
drop function if exists validatePredictModelSquid;
drop function if exists validateReportSquidColumnsByDEL;
drop function if exists validateColumnDataType;
drop function if exists validateAggregationOrGroup;
 
 -- 3.校验Squid是否孤立
 create function validateIsolateSquid(squidId int, childId int, squidName varchar(200), code int) returns Table(t_id int, t_name varchar(200), status boolean)
  READS SQL DATA
  begin 
   atomic
     Declare table v_table (t_id int, t_name varchar(200), status boolean);
     DECLARE countN INTEGER;
     --DECLARE squidName VARCHAR(200);
     DECLARE flag Boolean;
     --DECLARE to_squid_id INTEGER;
     delete from v_table;  
    
      if squidId != 0 then set countN = select count(*) from ds_squid_link where from_squid_id = squidId or to_squid_id = squidId;
      if countN>0 then insert into v_table values squidId, squidName, true; 
      else insert into v_table values squidId, squidName, false; 
      end if;
     else set flag = true;
     end if;
     for_loop: 
    for select id, name from ds_squid where squid_flow_id = childId and id not in (select dsl.from_squid_id as s_id from ds_squid_link dsl, ds_squid dss where dss.id = dsl.from_squid_id and dsl.squid_flow_id = childId union select dsl.to_squid_id as s_id from ds_squid_link dsl, ds_squid dss where dss.id = dsl.to_squid_id and dsl.squid_flow_id = childId) DO

         if flag = true then
         insert into v_table values id, name, false;
         end if;
     end for for_loop;
   

     return Table(select t_id, t_name, status from v_table);

     end
     
      -- 4.校验Transformation是否孤立
 create function validateIsolateTransformation(squidId int, childId int, squidName varchar(200), code int) returns Table(t_id int, t_c_id int, t_name varchar(200), status boolean)
  READS SQL DATA
  begin 
   atomic
     Declare table v_table (t_id int, t_c_id int, t_name varchar(200), status boolean);
     DECLARE countN INTEGER;
     --DECLARE squidName VARCHAR(200);
     DECLARE flag Boolean;
     --DECLARE to_squid_id INTEGER;
     delete from v_table; 
 
     if squidId != 0 then set countN = select count(*) from ds_transformation dst left join ds_transformation_link dtl on dst.id = dtl.from_transformation_id or dst.id = dtl.to_transformation_id  where dst.id = childId and dtl.id is null and dst.transformation_type_id != 0;
       if countN>0 then insert into v_table values squidId, childId, squidName, false; 
       else insert into v_table values squidId, childId, squidName, true; 
       end if;       
     else set flag = true;
     end if;
     for_loop: 
    --for select distinct dss.id as id, dss.name as name from ds_squid dss, ds_transformation dst, ds_transformation_link dtl where dss.squid_flow_id = childId and dss.id = dst.squid_id and dst.id not in ( select dtl.from_transformation_id as tranId from ds_squid dss, ds_transformation dst, ds_transformation_link dtl where dss.squid_flow_id = childId and dss.id = dst.squid_id and dst.id = dtl.from_transformation_id union select dtl.to_transformation_id as tranId from ds_squid dss, ds_transformation dst, ds_transformation_link dtl where dss.squid_flow_id = childId and dss.id = dst.squid_id and dst.id = dtl.to_transformation_id) DO
    --for select gh.id as id, gh.did as did, gh.dsd as dsd, dsc.name as name from (select dss.id as id, dtl.id as did, dst.id as dsd, dst.column_id as column_id from ds_squid dss left join ds_transformation dst on dss.id = dst.squid_id left join ds_transformation_link dtl on dst.id = dtl.from_transformation_id or dst.id = dtl.to_transformation_id where dss.squid_flow_id = childId and dst.id is not null and dtl.id is null) as gh inner join ds_column dsc on gh.column_id = dsc.id do
    for select dss.id as id, dst.id as c_id, dst.name as name, dtl.id as l_id from ds_squid dss right join ds_transformation dst on dss.id = dst.squid_id left join ds_transformation_link dtl on dst.id = dtl.from_transformation_id or dst.id = dtl.to_transformation_id where dss.squid_flow_id = childId and dst.column_id = 0 and dst.transformation_type_id != 0 do
         if flag = true and l_id is null then
         insert into v_table values id, c_id, name, false;
         end if;
     end for for_loop;  

     return Table(select t_id, t_c_id, t_name, status from v_table);
  
     end
     
     
          -- 5.StageSquid中没定义transformation的column
 create function validateIsolateColumn(squidId int, childId int, squidName varchar(200), code int) returns Table(t_id int, t_name varchar(200), status boolean)
  READS SQL DATA
  begin 
   atomic
     Declare table v_table (t_id int, t_name varchar(200), status boolean);
     DECLARE countN INTEGER;
     --DECLARE squidName VARCHAR(200);
     DECLARE flag Boolean;
     --DECLARE to_squid_id INTEGER;
     delete from v_table;      
     if squidId != 0 then set countN = select count(*) from ds_transformation dst, ds_column dsc where dsc.squid_id = squidId and dsc.squid_id = dst.squid_id and dsc.id = childId and dst.column_id is null;
       if countN>0 then insert into v_table values squidId, squidName, false; 
       else insert into v_table values squidId, squidName, true; 
       end if;       
     else set flag = true;
     end if;     
     
     for_loop: 
     for select dss.id as id, dsc.name as name from ds_transformation dst, ds_column dsc, ds_squid dss where dss.squid_flow_id = childId and dss.squid_type_id = 3 and dss.id = dsc.squid_id  and dsc.squid_id = dst.squid_id and dst.column_id is null DO

         if flag = true then
         insert into v_table values id, name, false;
         end if;
     end for for_loop;  

     return Table(select t_id, t_name, status from v_table);
  
     end
     
     
              -- 6.stageSquid的persist属性为true，但table_name为空
 create function validateDestinationTableName(squidId int, childId int, squidName varchar(200), code int) returns Table(t_id int, t_name varchar(200), status boolean)
  READS SQL DATA
  begin 
   atomic
     Declare table v_table (t_id int, t_name varchar(200), status boolean);
     DECLARE countN INTEGER;
     --DECLARE squidName VARCHAR(200);
     DECLARE flag Boolean;
     --DECLARE to_squid_id INTEGER;
     delete from v_table;      
     if squidId != 0 then set countN = select count(*) from ds_data_squid where is_persisted = 'Y' and (table_name is null or trim(table_name) = '');
       if countN>0 then insert into v_table values squidId, squidName, false; 
       else insert into v_table values squidId, squidName, true; 
       end if;       
     else set flag = true;
     end if;     
     
     for_loop: 
     
     
     for  select dds.id as id, dss.name as name from ds_data_squid dds, ds_squid dss where dss.squid_flow_id = childId and dss.id = dds.id and dds.is_persisted = 'Y' and (dds.table_name is null or trim(dds.table_name) = '') DO

         if flag = true then
         insert into v_table values id, name, false;
         end if;
     end for for_loop;  

     return Table(select t_id, t_name, status from v_table);
  
     end
     
     
                  -- 8.stageSquid的persist属性为true，但table_name为空
 create function validateSquidIndexes(squidId int, childId int, squidName varchar(200), code int) returns Table(t_id int, t_name varchar(200), status boolean)
  READS SQL DATA
  begin 
   atomic
     Declare table v_table (t_id int, t_name varchar(200), status boolean);
     DECLARE countN INTEGER;
     --DECLARE squidName VARCHAR(200);
     DECLARE flag Boolean;
     --DECLARE to_squid_id INTEGER;
     DECLARE iid INTEGER DEFAULT NULL;
     DECLARE iindexed VARCHAR(200);
     DECLARE iindexid INTEGER DEFAULT NULL; 
     delete from v_table; 
    
     
    --if squidId != 0 then select gh.id, gh.indexed, gh.indexid into iid, iindexed, iindexid from (select dds.id as id, dds.is_indexed as indexed, dsi.id as indexid from ds_data_squid dds left join ds_indexes dsi on dds.id = dsi.squid_id where dds.id = squidId 
     --        union select dde.id as id, dde.is_indexed as indexed, dsi.id as indexid from ds_doc_extract dde left join ds_indexes dsi on dde.id = dsi.squid_id where dde.id = squidId) as gh;
     -- if iindexed = 'Y' and iindexid is null then insert into v_table values iindexid, iindexed, false; 
      -- elseif iindexed = 'Y' and iindexid is not null then insert into v_table values iindexid, iindexed, true; 
      -- else insert into v_table values iid, iindexed, true; 
      -- end if;
    -- else set flag = true;
    -- end if;
          if squidId != 0 then set countN = select count(*) from (select dds.id as id, dds.is_indexed as indexed, dsi.id as indexid from ds_data_squid dds left join ds_indexes dsi on dds.id = dsi.squid_id where dds.id = squidId and ((dds.is_indexed = 'Y' and dsi.id is not null) or dds.is_indexed = 'N')
            union select dde.id as id, dde.is_indexed as indexed, dsi.id as indexid from ds_doc_extract dde left join ds_indexes dsi on dde.id = dsi.squid_id where dde.id = squidId and ((dde.is_indexed = 'Y' and dsi.id is not null) or dde.is_indexed = 'N'));
       if countN>0 then insert into v_table values squidId, squidName, true; 
       else insert into v_table values squidId, squidName, false; 
       end if;       
     else set flag = true;
     end if;
     
     for_loop:
     for  select gh.id as id, dsi.squid_id as sid, dsi.index_name as name from (select dss.id as id from ds_squid dss left join ds_data_squid dds on dss.id = dds.id where dss.squid_flow_id = childId and dds.is_indexed = 'Y') gh left join ds_indexes dsi on gh.id = dsi.squid_id 
          union select gh.id, dsi.squid_id as sid, dsi.index_name from (select dss.id as id from ds_squid dss left join ds_doc_extract dde on dss.id = dde.id where dss.squid_flow_id = childId and dde.is_indexed = 'Y') gh left join ds_indexes dsi on gh.id = dsi.squid_id  DO

         if flag = true and sid is null then
         insert into v_table values id, name, false;
         end if;
     end for for_loop;  

     return Table(select t_id, t_name, status from v_table);
  
     end
     
     -- 10.14.17.20.校验HostAddress
      create function validateHostAddress(squidId int, childId int, squidName varchar(200), code int) returns Table(t_id int, t_name varchar(200), status boolean)
  READS SQL DATA
  begin 
   atomic
     Declare table v_table (t_id int, t_name varchar(200), status boolean);
     DECLARE countN INTEGER;
     --DECLARE squidName VARCHAR(200);
     DECLARE flag Boolean;
     --DECLARE to_squid_id INTEGER;
     delete from v_table;        
     if squidId != 0 then set countN = select count(*) from (
     select host from ds_sql_connection where id = squidId and (host is null or trim(host) = '') union
     select host from ds_filefolder_connection where id = squidId and (host is null or trim(host) = '') union
     select host from ds_ftp_connection where id = squidId and (host is null or trim(host) = '') union
     select host from ds_hdfs_connection where id = squidId and (host is null or trim(host) = ''));
       if countN>0 then insert into v_table values squidId, squidName, false; 
       else insert into v_table values squidId, squidName, true; 
       end if;       
     else set flag = true;
     end if;     
     
     for_loop:
     
     for select dss.id as id, dss.name as name from ds_sql_connection dsc, ds_squid dss where dss.squid_flow_id = childId and dss.id = dsc.id and (dsc.host is null or trim(dsc.host) = '') union    
     select dss.id as id, dss.name as name from ds_filefolder_connection dfc, ds_squid dss where dss.squid_flow_id = childId and dss.id = dfc.id and (dfc.host is null or trim(dfc.host) = '') union     
      select dss.id as id, dss.name as name from ds_ftp_connection dftc, ds_squid dss where dss.squid_flow_id = childId and dss.id = dftc.id and (dftc.host is null or trim(dftc.host) = '') union   
     select dss.id as id, dss.name as name from ds_hdfs_connection dhc, ds_squid dss where dss.squid_flow_id = childId and dss.id = dhc.id and (dhc.host is null or trim(dhc.host) = '') DO
         if flag = true then
         insert into v_table values id, name, false;
         end if;
     end for for_loop;  

     return Table(select t_id, t_name, status from v_table);
  
     end
     
     
         -- 11.校验RDBMSType
      create function validateRDBMSType(squidId int, childId int, squidName varchar(200), code int) returns Table(t_id int, t_name varchar(200), status boolean)
  READS SQL DATA
  begin 
   atomic
     Declare table v_table (t_id int, t_name varchar(200), status boolean);
     DECLARE countN INTEGER;
     --DECLARE squidName VARCHAR(200);
     DECLARE flag Boolean;
     --DECLARE to_squid_id INTEGER;
     delete from v_table;        
     if squidId != 0 then set countN = select count(*) from ds_sql_connection where id = squidId and db_type_id is null;
       if countN>0 then insert into v_table values squidId, squidName, false; 
       else insert into v_table values squidId, squidName, true; 
       end if;       
     else set flag = true;
     end if;     
     
     for_loop:
     for select dss.id as id, dss.name as name from ds_sql_connection dsc, ds_squid dss where dss.squid_flow_id = childId and dss.id = dsc.id and dsc.db_type_id is null DO
         if flag = true then
         insert into v_table values id, name, false;
         end if;
     end for for_loop;  

     return Table(select t_id, t_name, status from v_table);
  
     end
     
     
             -- 12.校验DataBaseName
      create function validateDataBaseName(squidId int, childId int, squidName varchar(200), code int) returns Table(t_id int, t_name varchar(200), status boolean)
  READS SQL DATA
  begin 
   atomic
     Declare table v_table (t_id int, t_name varchar(200), status boolean);
     DECLARE countN INTEGER;
     --DECLARE squidName VARCHAR(200);
     DECLARE flag Boolean;
     --DECLARE to_squid_id INTEGER;
     delete from v_table;        
     if squidId != 0 then set countN = select count(*) from ds_sql_connection where id = squidId and (database_name is null or trim(database_name) = '');
       if countN>0 then insert into v_table values squidId, squidName, false; 
       else insert into v_table values squidId, squidName, true; 
       end if;       
     else set flag = true;
     end if;     
     
     for_loop:
     for select dss.id as id, dss.name as name from ds_sql_connection dsc, ds_squid dss where dss.squid_flow_id = childId and dss.id = dsc.id and (dsc.database_name is null or trim(database_name) = '') DO
         if flag = true then
         insert into v_table values id, name, false;
         end if;
     end for for_loop;  

     return Table(select t_id, t_name, status from v_table);
  
     end
     
         -- 15.18.21.校验FilePath
      create function validateFilePath(squidId int, childId int, squidName varchar(200), code int) returns Table(t_id int, t_name varchar(200), status boolean)
  READS SQL DATA
  begin 
   atomic
     Declare table v_table (t_id int, t_name varchar(200), status boolean);
     DECLARE countN INTEGER;
     --DECLARE squidName VARCHAR(200);
     DECLARE flag Boolean;
     --DECLARE to_squid_id INTEGER;
     delete from v_table;        
     if squidId != 0 then set countN = select count(*) from (    
     select id from ds_filefolder_connection where id = squidId and (file_path is null or trim(file_path) = '') union
     select id from ds_ftp_connection where id = squidId and (file_path is null or trim(file_path) = '') union
     select id from ds_hdfs_connection where id = squidId and (file_path is null or trim(file_path) = ''));
       if countN>0 then insert into v_table values squidId, squidName, false; 
       else insert into v_table values squidId, squidName, true; 
       end if;       
     else set flag = true;
     end if;     
     
     for_loop:
     
     for  select dss.id as id, dss.name as name from ds_filefolder_connection dfc, ds_squid dss where dss.squid_flow_id = childId and dss.id = dfc.id and (dfc.file_path is null or trim(dfc.file_path) = '') union  
      select dss.id as id, dss.name as name from ds_ftp_connection dftc, ds_squid dss where dss.squid_flow_id = childId and dss.id = dftc.id and (dftc.file_path is null or trim(dftc.file_path) = '') union     
     select dss.id as id, dss.name as name from ds_hdfs_connection dhc, ds_squid dss where dss.squid_flow_id = childId and dss.id = dhc.id and (dhc.file_path is null or trim(dhc.file_path) = '') DO
         if flag = true then
         insert into v_table values id, name, false;
         end if;
     end for for_loop;  

     return Table(select t_id, t_name, status from v_table);
  
     end
     
             -- 23.校验ServiceProvider微博提供商设置
      create function validateServiceProvider(squidId int, childId int, squidName varchar(200), code int) returns Table(t_id int, t_name varchar(200), status boolean)
  READS SQL DATA
  begin 
   atomic
     Declare table v_table (t_id int, t_name varchar(200), status boolean);
     DECLARE countN INTEGER;
     --DECLARE squidName VARCHAR(200);
     DECLARE flag Boolean;
     --DECLARE to_squid_id INTEGER;
     delete from v_table;        
     if squidId != 0 then set countN = select count(*) from ds_weibo_connection where id = squidId and service_provider is null;
       if countN>0 then insert into v_table values squidId, squidName, false; 
       else insert into v_table values squidId, squidName, true; 
       end if;       
     else set flag = true;
     end if;     
     
     for_loop:
     
     for  select dss.id as id, dss.name as name from ds_squid dss, ds_weibo_connection dwc where dss.squid_flow_id = childId and  dss.id = dwc.id and dwc.service_provider is null DO
         if flag = true then
         insert into v_table values id, name, false;
         end if;
     end for for_loop;  

     return Table(select t_id, t_name, status from v_table);
  
     end
     
     
                 -- 25.校验WebConnection的URL未设置
      create function validateURLList(squidId int, childId int, squidName varchar(200), code int) returns Table(t_id int, t_name varchar(200), status boolean)
  READS SQL DATA
  begin 
   atomic
     Declare table v_table (t_id int, t_name varchar(200), status boolean);
     DECLARE countN INTEGER;
     --DECLARE squidName VARCHAR(200);
     DECLARE flag Boolean;
     --DECLARE to_squid_id INTEGER;
     delete from v_table;        
     if squidId != 0 then set countN = select count(*) from ds_web_connection dwc left join ds_url dsu on dwc.id = dsu.squid_id and (dsu.url is not null or trim(dsu.url) != '') where dwc.id = squidId and dsu.id is not null;
       if countN>0 then insert into v_table values squidId, squidName, true; 
       else insert into v_table values squidId, squidName, false; 
       end if;
     else set flag = true;
     end if;
     
     for_loop:
     
    for select gh.id as id,gh.name as name, dsu.id as dsuid, dsu.url as url from (select dss.id as id, dss.name as name from ds_squid dss left join ds_web_connection dwc on dss.id = dwc.id where dss.squid_flow_id = childId and dwc.id is not null ) gh left join ds_url dsu on gh.id = dsu.squid_id Do
    --for select dss.id as id, dss.name as name from ds_squid dss, ds_web_connection dwc, ds_url dsu where dss.squid_flow_id = childId and dss.id = dwc.id and dwc.id = dsu.squid_id and (dsu.url is null or trim(dsu.url) = '') DO
         if flag = true and (dsuid is null or url is null or trim(url) = '') then
         insert into v_table values id, name, false;
         end if;
     end for for_loop;  

     return Table(select t_id, t_name, status from v_table);
  
     end
     
             -- 34.文档抽取Squid(DocExtractSquid)验证Delimiter
      create function validateDelimiter(squidId int, childId int, squidName varchar(200), code int) returns Table(t_id int, t_name varchar(200), status boolean)
  READS SQL DATA
  begin 
   atomic
     Declare table v_table (t_id int, t_name varchar(200), status boolean);
     DECLARE countN INTEGER;
     --DECLARE squidName VARCHAR(200);
     DECLARE flag Boolean;
     --DECLARE to_squid_id INTEGER;
     delete from v_table;        
     if squidId != 0 then set countN = select count(*) from ds_doc_extract where id = squidId and row_format = 0 and (delimiter is null or trim(delimiter) = '');
       if countN>0 then insert into v_table values squidId, squidName, false; 
       else insert into v_table values squidId, squidName, true; 
       end if;       
     else set flag = true;
     end if;     
     
     for_loop:   
     for  select dss.id as id, dss.name as name from ds_squid dss, ds_doc_extract dde where dss.squid_flow_id = childId and dss.id = dde.id and dde.row_format = 0 and (dde.delimiter is null or trim(dde.delimiter) = '') DO
         if flag = true then
         insert into v_table values id, name, false;
         end if;
     end for for_loop;  

     return Table(select t_id, t_name, status from v_table);
  
     end
     
                 -- 35.FieldLength字段长度设置非法
      create function validateFieldLength(squidId int, childId int, squidName varchar(200), code int) returns Table(t_id int, t_name varchar(200), status boolean)
  READS SQL DATA
  begin 
   atomic
     Declare table v_table (t_id int, t_name varchar(200), status boolean);
     DECLARE countN INTEGER;
     --DECLARE squidName VARCHAR(200);
     DECLARE flag Boolean;
     --DECLARE to_squid_id INTEGER;
     delete from v_table;        
     if squidId != 0 then set countN = select count(*) from ds_doc_extract where id = squidId and row_format = 1 and field_length = 0;
       if countN>0 then insert into v_table values squidId, squidName, false; 
       else insert into v_table values squidId, squidName, true; 
       end if;       
     else set flag = true;
     end if;     
     
     for_loop:   
     for  select dss.id as id, dss.name as name from ds_squid dss, ds_doc_extract dde where dss.squid_flow_id = childId and dss.id = dde.id and dde.row_format = 1 and dde.field_length = 0 DO
         if flag = true then
         insert into v_table values id, name, false;
         end if;
     end for for_loop;  

     return Table(select t_id, t_name, status from v_table);
  
     end
     
     -- 36.校验WebLogExtractSquid是否完成获取元数据
      create function validateWebLogExtractSquidSourceData(squidId int, childId int, squidName varchar(200), code int) returns Table(t_id int, t_name varchar(200), status boolean)
  READS SQL DATA
  begin 
   atomic
     Declare table v_table (t_id int, t_name varchar(200), status boolean);
     DECLARE countN INTEGER;
     --DECLARE squidName VARCHAR(200);
     DECLARE flag Boolean;
     --DECLARE to_squid_id INTEGER;
     delete from v_table;        
     if squidId != 0 then set countN = select count(*) from ds_reference_column where reference_squid_id = squidId;
       if countN>0 then insert into v_table values squidId, squidName, true; 
       else insert into v_table values squidId, squidName, false; 
       end if;       
     else set flag = true;
     end if;     
     
     for_loop:   
     --for  select dss.id as id, dss.name as name from ds_squid dss, ds_reference_column drc where dss.squid_flow_id = childId and drc.reference_squid_id != dss.id and dss.squid_type_id = 9 DO
         for select distinct dss.id as id, dss.name as name, drc.reference_squid_id as rid from ds_squid dss left join ds_reference_column drc on dss.id = drc.reference_squid_id where dss.squid_flow_id = childId  and dss.squid_type_id = 9 DO
         if flag = true and rid is null then
         insert into v_table values id, name, false;
         end if;
     end for for_loop;  

     return Table(select t_id, t_name, status from v_table);
  
     end
     
     
     
         -- 43.校验发布路径未设置
      create function validatePublishingFolder(squidId int, childId int, squidName varchar(200), code int) returns Table(t_id int, t_name varchar(200), status boolean)
  READS SQL DATA
  begin 
   atomic
     Declare table v_table (t_id int, t_name varchar(200), status boolean);
     DECLARE countN INTEGER;
     --DECLARE squidName VARCHAR(200);
     DECLARE flag Boolean;
     --DECLARE to_squid_id INTEGER;
     delete from v_table;        
     if squidId != 0 then set countN = select count(*) from ds_report_squid where id = squidId and folder_id = 0;
       if countN>0 then insert into v_table values squidId, squidName, false; 
       else insert into v_table values squidId, squidName, true; 
       end if;       
     else set flag = true;
     end if;     
     
     for_loop:   
     for  select dss.id as id, dss.name as name from ds_squid dss, ds_report_squid drs where dss.squid_flow_id = childId and dss.id = drs.id and drs.folder_id = 0 DO
         if flag = true then
         insert into v_table values id, name, false;
         end if;
     end for for_loop;  

     return Table(select t_id, t_name, status from v_table);
  
     end
     
             -- 44.校验发布路径未设置
      create function validateTemplateDefinition(squidId int, childId int, squidName varchar(200), code int) returns Table(t_id int, t_name varchar(200), status boolean)
  READS SQL DATA
  begin 
   atomic
     Declare table v_table (t_id int, t_name varchar(200), status boolean);
     DECLARE countN INTEGER;
     --DECLARE squidName VARCHAR(200);
     DECLARE flag Boolean;
     --DECLARE to_squid_id INTEGER;
     delete from v_table;        
     if squidId != 0 then set countN = select count(*) from ds_report_squid where id = squidId and report_template is null;
       if countN>0 then insert into v_table values squidId, squidName, false; 
       else insert into v_table values squidId, squidName, true; 
       end if;       
     else set flag = true;
     end if;     
     
     for_loop:   
     for  select dss.id as id, dss.name as name from ds_squid dss, ds_report_squid drs where dss.squid_flow_id = childId and dss.id = drs.id and dss.squid_type_id = 6 and drs.report_template is null DO
         if flag = true then
         insert into v_table values id, name, false;
         end if;
     end for for_loop;  

     return Table(select t_id, t_name, status from v_table);
  
     end
     
                 -- 47.校验Training_percentage训练集所占比例
      create function validateTrainingPercentage(squidId int, childId int, squidName varchar(200), code int) returns Table(t_id int, t_name varchar(200), status boolean)
  READS SQL DATA
  begin 
   atomic
     Declare table v_table (t_id int, t_name varchar(200), status boolean);
     DECLARE countN INTEGER;
     --DECLARE squidName VARCHAR(200);
     DECLARE flag Boolean;
     --DECLARE to_squid_id INTEGER;
     delete from v_table;        
     if squidId != 0 then set countN = select count(*) from ds_dm_squid dds inner join ds_squid dss on dds.id = dss.id where dds.id = squidId and dds.training_percentage < 0.5 and dss.squid_type_id not in (28, 29);
       if countN>0 then insert into v_table values squidId, squidName, false; 
       else insert into v_table values squidId, squidName, true; 
       end if;       
     else set flag = true;
     end if;
     
     for_loop:   
     for  select dss.id as id, dss.name as name from ds_squid dss, ds_dm_squid dds where dss.squid_flow_id = childId and dss.id = dds.id and dds.training_percentage < 0.5 and dss.squid_type_id not in (28, 29) DO
         if flag = true then
         insert into v_table values id, name, false;
         end if;
     end for for_loop;  

     return Table(select t_id, t_name, status from v_table);
  
     end

     -- 48.校验ModelSquidId
      create function validatePredictModelSquid(squidId int, childId int, squidName varchar(200), code int) returns Table(t_id int, t_name varchar(200), status boolean)
  READS SQL DATA
  begin 
   atomic
     Declare table v_table (t_id int, t_name varchar(200), status boolean);
     DECLARE countN INTEGER;
     --DECLARE squidName VARCHAR(200);
     DECLARE flag Boolean;
     --DECLARE to_squid_id INTEGER;
     delete from v_table;        
     if squidId != 0 then set countN = select count(*) from ds_squid dss, ds_transformation dst where dst.squid_id = squidId and dst.model_squid_id = dss.id and dst.transformation_type_id = 65;
       if countN>0 then insert into v_table values squidId, squidName, false; 
       else insert into v_table values squidId, squidName, true; 
       end if;       
     else set flag = true;
     end if;
     
     for_loop:   
     for  select dss.id as id, dss.name as name from ds_squid dss, ds_dm_squid drs where dss.squid_flow_id = childId and drs.training_percentage < 0.5 DO
         if flag = true then
         insert into v_table values id, name, false;
         end if;
     end for for_loop;  

     return Table(select t_id, t_name, status from v_table);
  
     end
     
          -- 49.校验报表squid的源数据是否被删除
      create function validateReportSquidColumnsByDEL(squidId int, childId int, squidName varchar(200), code int) returns Table(t_id int, t_name varchar(200), status boolean)
  READS SQL DATA
  begin 
   atomic
     Declare table v_table (t_id int, t_name varchar(200), status boolean);
     DECLARE countN INTEGER;
     --DECLARE squidName VARCHAR(200);
     DECLARE flag Boolean;
     --DECLARE to_squid_id INTEGER;
     delete from v_table;
     
     for_loop:   
     for select dss.id as id, dss.name as name from ds_squid dss inner join ds_report_squid drs on dss.id = drs.id where dss.squid_flow_id = childId and dss.design_status = 1 DO
         insert into v_table values id, name, false;
     end for for_loop;  

     return Table(select t_id, t_name, status from v_table);
  
     end
     
     
         -- 50.校验Column的DataType
      create function validateColumnDataType(squidId int, childId int, squidName varchar(200), code int) returns Table(t_id int, t_c_id int, t_name varchar(200), status boolean)
  READS SQL DATA
  begin 
   atomic
     Declare table v_table (t_id int, t_c_id int, t_name varchar(200), status boolean);
     DECLARE countN INTEGER;
     --DECLARE squidName VARCHAR(200);
     DECLARE flag Boolean;
     --DECLARE to_squid_id INTEGER;
     delete from v_table; 
 
     if squidId != 0 then set countN = select count(*) from ds_column where id = childId and squid_id = squidId and data_type = 0;
       if countN>0 then insert into v_table values squidId, childId, squidName, false; 
       else insert into v_table values squidId, childId, squidName, true; 
       end if;       
     else set flag = true;
     end if;
     for_loop: 

    for select dss.id as id, dsc.id as c_id, dsc.name as name from ds_squid dss inner join ds_column dsc on dss.id = dsc.squid_id where dss.squid_flow_id = childId and dsc.data_type = 0 do
         if flag = true then
         insert into v_table values id, c_id, name, false;
         end if;
     end for for_loop;  

     return Table(select t_id, t_c_id, t_name, status from v_table);
  
     end
     
              -- 51.校验Aggregation和Group
      create function validateAggregationOrGroup(squidId int, childId int, squidName varchar(200), code int) returns Table(t_id int, t_c_id int, t_name varchar(200), status boolean)
  READS SQL DATA
  begin 
   atomic
     Declare table v_table (t_id int, t_c_id int, t_name varchar(200), status boolean);
     DECLARE countN INTEGER;
     --DECLARE squidName VARCHAR(200);
     DECLARE flag Boolean;
     --DECLARE to_squid_id INTEGER;
     delete from v_table; 
 
     if squidId != 0 then set countN = select count(*) from ds_column where id = childId and squid_id = squidId and is_groupby = 'N' and aggregation_type = -1;
       if countN>0 then insert into v_table values squidId, childId, squidName, false; 
       else insert into v_table values squidId, childId, squidName, true; 
       end if;       
     else set flag = true;
     end if;
     for_loop: 

    for select dss.id as id, dsc.id as c_id, dsc.name as name from ds_squid dss inner join ds_column dsc on dss.id = dsc.squid_id where dss.squid_type_id = 3 and dss.squid_flow_id = childId and dsc.is_groupby = 'N' and dsc.aggregation_type = -1 do
         if flag = true then
         insert into v_table values id, c_id, name, false;
         end if;
     end for for_loop;

     return Table(select t_id, t_c_id, t_name, status from v_table);
  
     end
