package com.eurlanda.datashire.sprint7.service.squidflow.subservice;

import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.adapter.db.AdapterDataSourceManager;
import com.eurlanda.datashire.adapter.db.INewDBAdapter;
import com.eurlanda.datashire.entity.*;
import com.eurlanda.datashire.entity.operation.TableIndex;
import com.eurlanda.datashire.enumeration.DataBaseType;
import com.eurlanda.datashire.enumeration.MessageBubbleCode;
import com.eurlanda.datashire.enumeration.SquidTypeEnum;
import com.eurlanda.datashire.sprint7.service.squidflow.AbstractRepositoryService;
import com.eurlanda.datashire.sprint7.service.squidflow.ManagerSquidFlowProcess;
import com.eurlanda.datashire.utility.CommonConsts;
import com.eurlanda.datashire.utility.MessageCode;
import com.eurlanda.datashire.utility.ReturnValue;
import com.eurlanda.datashire.utility.StringUtils;
import com.eurlanda.datashire.utility.SysConf;
import com.eurlanda.datashire.validator.SquidValidationTask;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SquidIndexsServiceSub extends AbstractRepositoryService {
    public SquidIndexsServiceSub(String token) {
        super(token);
    }

    public SquidIndexsServiceSub(IRelationalDataManager adapter) {
        super(adapter);
    }

    public SquidIndexsServiceSub(String token, IRelationalDataManager adapter) {
        super(token, adapter);
    }

    /**
     * 根据squid获取squidIndexsList
     * 
     * @param squidId
     * @return
     * @author bo.dang
     * @date 2014年5月15日
     */
    public List<SquidIndexes> getSquidIndexsListBySquidId(int squidId) {
        Map<String, Object> paramsMap = new HashMap<String, Object>();
        paramsMap.put("squid_id", squidId);
        List<SquidIndexes> squidIndexsList = adapter.query2List2(false,
                paramsMap, SquidIndexes.class);
        return squidIndexsList;
    }

    /**
     * 根据squidFlowId取得SquidIndex信息
     * 
     * @param squidFlowId
     * @param type
     * @param out
     * @return
     * @author bo.dang
     * @date 2014年5月22日
     */
    public List<SquidIndexes> getAllSquidIndexedList(Integer squidFlowId,
            Integer type, ReturnValue out) {
        String sql = null;
        if (StringUtils.isNull(type)) {
            sql = "select * from ds_squid where squid_flow_id = " + squidFlowId
                    + "and squid_type_id in (" + SquidTypeEnum.EXTRACT.value()
                    + ", " + SquidTypeEnum.DOC_EXTRACT.value() + ", "
                    + SquidTypeEnum.XML_EXTRACT.value() + ", "
                    + SquidTypeEnum.WEBLOGEXTRACT.value() + ", "
                    + SquidTypeEnum.WEIBOEXTRACT.value() + ", "
                    + SquidTypeEnum.WEBEXTRACT.value() + ")";
        } else {
            sql = "select * from ds_squid where squid_flow_id = " + squidFlowId
                    + "and squid_type_id =" + type;
        }
        List<Squid> squidList = adapter.query2List(sql, null, Squid.class);
        if (StringUtils.isNull(squidList)) {
            return null;
        }
        int squidId = 0;
        List<SquidIndexes> squidIndexesResultList = new ArrayList<SquidIndexes>();
        for (int i = 0; i < squidList.size(); i++) {
            squidId = squidList.get(i).getId();
            // 根据squidId取Squidindexes
            List<SquidIndexes> squidIndexsList = getSquidIndexsListBySquidId(squidId);
            if (StringUtils.isNotNull(squidIndexsList)) {
                squidIndexesResultList.addAll(squidIndexsList);
            }
        }
        out.setMessageCode(MessageCode.SUCCESS);
        return squidIndexesResultList;
    }

    /**
     * 更新SquidIndexs
     * 
     * @param squidIndexsList
     * @return
     * @author bo.dang
     * @date 2014年5月16日
     */
    public List<Integer> updateSquidIndexs(List<SquidIndexes> squidIndexsList) {
        if (StringUtils.isNull(squidIndexsList)) {
            return null;
        }
        List<Integer> squidIndexsIdList = new ArrayList<Integer>();
        try {
            adapter.openSession();
            SquidIndexes squidIndexs = null;
            for (int i = 0; i < squidIndexsList.size(); i++) {
                squidIndexs = squidIndexsList.get(i);
                adapter.update2(squidIndexs);
                squidIndexsIdList.add(squidIndexs.getId());
            }
        } catch (SQLException e) {
            logger.error("更新SquidIndexs异常", e);
            try {
                adapter.rollback();
            } catch (SQLException e1) { // 数据库回滚失败（程序不能处理该异常）！
                logger.fatal("rollback err!", e1);
            }
        } finally {
            adapter.closeSession();
        }

        return squidIndexsIdList;
    }

    /**
     *
	 * @param indexId
     * @author bo.dang
     * @date 2014年5月17日
     */
    public Boolean deleteSquidIndexs(int indexId, ReturnValue out) {
        Boolean deleteFlag = false;
        try {
            adapter.openSession();
            Map<String, String> paramMap = new HashMap<String,String>();
            paramMap.put("id", Integer.toString(indexId, 10));
            SquidIndexes squidIndexs = adapter.query2Object2(true, paramMap, SquidIndexes.class);
            if (squidIndexs!=null){
				// -------  修复BUG，删除索引记录时把数据库索引删除  -- zhengpeng.deng
            	//dropSquidIndexsFromDB(adapter ,squidIndexs, out);    //删除数据库里面的字段
				// ------------------------------------------------------------

				deleteFlag = adapter.delete(paramMap, SquidIndexes.class)>0?true:false;//删除索引号
            }
        } catch (Exception e) {
            logger.error("删除SquidIndexs异常", e);
            try {
                adapter.rollback();
            } catch (SQLException e1) { // 数据库回滚失败（程序不能处理该异常）！
                logger.fatal("rollback err!", e1);
            }
        } finally {
            adapter.closeSession();
        }
        return deleteFlag;
    }

    /**
     * 更新SquidIndexs
     * 
     * @param squidIndexsList
     * @return
     * @author bo.dang
     * @date 2014年5月16日
     */
    public List<Integer> createSquidIndexsToDB(
            List<SquidIndexes> squidIndexsList, ReturnValue out) {
        if (StringUtils.isNull(squidIndexsList)) {
            return null;
        }
        List<Integer> squidIndexsIdList = new ArrayList<Integer>();
        try {
            adapter.openSession();
            return this.createSquidIndexsToDB(adapter, squidIndexsList, out);
        } catch (Exception e) {
            logger.error("更新SquidIndexs异常", e);
            try {
                adapter.rollback();
            } catch (SQLException e1) { // 数据库回滚失败（程序不能处理该异常）！
                logger.fatal("rollback err!", e1);
            }
            out.setMessageCode(MessageCode.SQL_ERROR);
        } finally {
            adapter.closeSession();
        }
        return squidIndexsIdList;
    }
    
    /**
     * 更新SquidIndexs
     * 
     * @param squidIndexsList
     * @return
     * @author bo.dang
     * @date 2014年5月16日
     */
    public List<Integer> createSquidIndexsToDB(IRelationalDataManager adapter, List<SquidIndexes> squidIndexsList, ReturnValue out) throws Exception{
    	if (StringUtils.isNull(squidIndexsList)) {
            return null;
        }
        List<Integer> squidIndexsIdList = new ArrayList<Integer>();
        SquidIndexes squidIndexs = null;
        int squidId = 0;
        String tableName = null;
        for (int i = 0; i < squidIndexsList.size(); i++) {
            squidIndexs = squidIndexsList.get(i);
            squidId = squidIndexs.getSquid_id();
            String sql = "select * from ds_squid where id="+squidId;
			Squid squid = adapter.query2Object(true, sql, null, Squid.class);
			if (squid==null){
				continue;
			}
            // 已经落地的squid
            Class c = SquidTypeEnum.classOfValue(squid.getSquid_type());
			boolean is_persisted = false;
			int destination_squid_id = 0;
			if (c==DataSquid.class
					||c==StageSquid.class
					||c==ExceptionSquid.class
					||c==StreamStageSquid.class){
				sql = "select * from ds_squid where id="+squidId;
				DataSquid dataSquid = adapter.query2Object(true, 
						sql, null, DataSquid.class);
				if (dataSquid!=null){
					is_persisted = dataSquid.isIs_persisted();
					destination_squid_id = dataSquid.getDestination_squid_id();
					tableName = dataSquid.getTable_name();
				}
			}else if (c==DocExtractSquid.class){
				sql = "select * from ds_squid where id="+squidId;
				DocExtractSquid docSquid = adapter.query2Object(true, 
						sql, null, DocExtractSquid.class);
				if (docSquid!=null){
					is_persisted = docSquid.isIs_persisted();
					destination_squid_id = docSquid.getDestination_squid_id();
					tableName = docSquid.getTable_name();
				}
			}
			else if (c==HBaseExtractSquid.class){
				sql = "select * from ds_squid where id="+squidId;
				HBaseExtractSquid HBaseSquid = adapter.query2Object(true,
						sql, null, HBaseExtractSquid.class);
				if (HBaseSquid!=null){
					is_persisted = HBaseSquid.isIs_persisted();
					destination_squid_id = HBaseSquid.getDestination_squid_id();
					tableName = HBaseSquid.getTable_name();
				}
			}
			if (StringUtils.isNull(tableName)) {
                continue;
            }
			if (is_persisted){
				if(destination_squid_id>0){
					
					Squid dbSquid = adapter.query2Object(true,"select * from ds_squid ds where ds.id="+destination_squid_id,null,DbSquid.class);
					INewDBAdapter adaoterSource=null;
					DBConnectionInfo dbs = null;
					if(dbSquid.getSquid_type()==SquidTypeEnum.DBSOURCE.value()
							||dbSquid.getSquid_type()==SquidTypeEnum.CLOUDDB.value() || dbSquid.getSquid_type()==SquidTypeEnum.TRAININGDBSQUID.value()){
						sql = "select * from ds_squid where id="+destination_squid_id;
						DbSquid db = adapter.query2Object(true, sql, null, DbSquid.class);
						if (db!=null){
							// 获取数据源
							dbs = DBConnectionInfo.fromDBSquid(db);
							adaoterSource= AdapterDataSourceManager.getAdapter(dbs);
							if (dbs.getDbType()==DataBaseType.HANA&&!tableName.contains(".")){
								tableName = dbs.getDbName()+"."+tableName;
							}
							boolean isExists = this.isExistsSquidTableByTbName(tableName, adaoterSource);
							if (!isExists){
								out.setMessageCode(MessageCode.ERR_PERSISTTABLE_NOT_TABLENAME);
								return null;
							}
							TableIndex index = this.getColumnNameByIndex(adapter, dbs.getDbType().value(), squidIndexs);
							if (index!=null){
								try {
									adaoterSource.deleteIndex(tableName, index.getIndexName());
								} catch (Exception e) {
									logger.error("删除未存在的index："+index.getIndexName());
								}
								adaoterSource.addIndex(tableName, index);
							}
			                squidIndexsIdList.add(squidIndexs.getId());
			                adaoterSource.close();
						}
					}else if(dbSquid.getSquid_type()==SquidTypeEnum.MONGODB.value()){
//						sql = "select * from ds_squid ds inner join ds_sql_connection dsc " +
//								" on ds.id=dsc.id where ds.id="+destination_squid_id;
						sql = "select * from ds_squid where id="+destination_squid_id;
						NOSQLConnectionSquid nosql = adapter.query2Object(true, sql, null, NOSQLConnectionSquid.class);
						if (nosql!=null){
							// 获取数据源
							dbs = DBConnectionInfo.fromNOSQLSquid(nosql);
							adaoterSource= AdapterDataSourceManager.getAdapter(dbs);
							int cnt = adaoterSource.getRecordCount(tableName);
							if (cnt==0){
								out.setMessageCode(MessageCode.ERR_PERSISTTABLE_NOT_TABLENAME);
								return null;
							}
							TableIndex index = this.getColumnNameByIndex(adapter, squidIndexs, 1);
							if (index!=null){
								try {
									adaoterSource.deleteIndex(tableName, index);
								} catch (Exception e) {
									logger.error("删除未存在的index："+index.getIndexName());
								}
								adaoterSource.addIndex(tableName, index);
							}
			                squidIndexsIdList.add(squidIndexs.getId());
			                adaoterSource.close();
						}
					}
				}else{
					DBConnectionInfo dbs = new DBConnectionInfo();
			        dbs.setHost(SysConf.getValue("hbase.host"));
			        dbs.setPort(Integer.parseInt(SysConf.getValue("hbase.port")));
			        dbs.setDbType(DataBaseType.HBASE_PHOENIX);
					INewDBAdapter adaoterSource= AdapterDataSourceManager.getAdapter(dbs);
					boolean isExists = this.isExistsSquidTableByTbName(tableName, adaoterSource);
					if (!isExists){
						out.setMessageCode(MessageCode.ERR_PERSISTTABLE_NOT_TABLENAME);
						return null;
					}
					TableIndex index = this.getColumnNameByIndex(adapter, dbs.getDbType().value(), squidIndexs);
					if (index!=null){
						try {
							adaoterSource.deleteIndex(tableName, index.getIndexName());
						} catch (Exception e) {
							logger.error("删除未存在的index："+index.getIndexName());
						}
						adaoterSource.addIndex(tableName, index);
					}
	                squidIndexsIdList.add(squidIndexs.getId());
	                adaoterSource.close();
				}
				// 消除未创建落地索引消息泡
				CommonConsts.addValidationTask(new SquidValidationTask(token, MessageBubbleService.setMessageBubble(squidId, squidId, null, MessageBubbleCode.ERROR_INDEXES_NOT_SYNCHRONIZED.value()), true, "索引未同步"));
			}else{
				out.setMessageCode(MessageCode.ERR_PERSISTTABLE_NOT_CHECKED);
				return null;
			}
        }
        return squidIndexsIdList;
    }
    
    /**
     * 根据SquidIndexs对象获取相关的TableIndex初始化设置
     * @param adapter
     * @param squidIndexs
     * @return
     */
    public TableIndex getColumnNameByIndex(IRelationalDataManager adapter,int dbType, SquidIndexes squidIndexs){
    	TableIndex index = new TableIndex();
    	String indexName = squidIndexs.getIndex_name();
/*        int columnId1 = squidIndexs.getColumn_id1();
        int columnId2 = squidIndexs.getColumn_id2();
        int columnId3 = squidIndexs.getColumn_id3();
        int columnId4 = squidIndexs.getColumn_id4();
        int columnId5 = squidIndexs.getColumn_id5();
        int columnId6 = squidIndexs.getColumn_id6();
        int columnId7 = squidIndexs.getColumn_id7();
        int columnId8 = squidIndexs.getColumn_id8();
        int columnId9 = squidIndexs.getColumn_id9();
        int columnId10 = squidIndexs.getColumn_id10();
        String sql = "select * from ds_column where id in ("
                + columnId1 + "," + columnId2 + "," + columnId3 + ","
                + columnId4 + "," + columnId5 + "," + columnId6 + ","
                + columnId7 + "," + columnId8 + "," + columnId9 + ","
                + columnId10 + ")";
        List<Column> columnList = adapter.query2List(true, sql, null,
                Column.class);
        String columnName = "";
        StringBuffer colName = new StringBuffer();
        if (columnList!=null&&columnList.size()>0){
	        for (Column column : columnList) {
				if (StringUtils.isNotNull(column.getName())){
					String name = this.initColumnNameByDbType(dbType, column.getName());
					colName.append(",").append(name);
				}
			}
	        if (colName.length()>0){
	        	columnName = colName.toString().substring(1);
	        }else{
	        	return null;
	        }
        }
        index.setColumnName(columnName);
        index.setIndexName(indexName);*/
    	
    	ManagerSquidFlowProcess.setColumnName(adapter, squidIndexs);
        String columnName = "";
        StringBuffer colName = new StringBuffer();
        String tempName = null;
		if (StringUtils.isNotNull(squidIndexs.getColumn_name_1())){
			tempName = this.initColumnNameByDbType(dbType, squidIndexs.getColumn_name_1());
			colName.append(",").append(tempName);
		}
		if (StringUtils.isNotNull(squidIndexs.getColumn_name_2())){
			tempName = this.initColumnNameByDbType(dbType, squidIndexs.getColumn_name_2());
			colName.append(",").append(tempName);
		}
		if (StringUtils.isNotNull(squidIndexs.getColumn_name_3())){
			tempName = this.initColumnNameByDbType(dbType, squidIndexs.getColumn_name_3());
			colName.append(",").append(tempName);
		}
		if (StringUtils.isNotNull(squidIndexs.getColumn_name_4())){
			tempName = this.initColumnNameByDbType(dbType, squidIndexs.getColumn_name_4());
			colName.append(",").append(tempName);
		}
		if (StringUtils.isNotNull(squidIndexs.getColumn_name_5())){
			tempName = this.initColumnNameByDbType(dbType, squidIndexs.getColumn_name_5());
			colName.append(",").append(tempName);
		}
		if (StringUtils.isNotNull(squidIndexs.getColumn_name_6())){
			tempName = this.initColumnNameByDbType(dbType, squidIndexs.getColumn_name_6());
			colName.append(",").append(tempName);
		}
		if (StringUtils.isNotNull(squidIndexs.getColumn_name_7())){
			tempName = this.initColumnNameByDbType(dbType, squidIndexs.getColumn_name_7());
			colName.append(",").append(tempName);
		}
		if (StringUtils.isNotNull(squidIndexs.getColumn_name_8())){
			tempName = this.initColumnNameByDbType(dbType, squidIndexs.getColumn_name_8());
			colName.append(",").append(tempName);
		}
		if (StringUtils.isNotNull(squidIndexs.getColumn_name_9())){
			tempName = this.initColumnNameByDbType(dbType, squidIndexs.getColumn_name_9());
			colName.append(",").append(tempName);
		}
		if (StringUtils.isNotNull(squidIndexs.getColumn_name_10())){
			tempName = this.initColumnNameByDbType(dbType, squidIndexs.getColumn_name_10());
			colName.append(",").append(tempName);
		}
        if (colName.length()>0){
        	columnName = colName.toString().substring(1);
        }else{
        	return null;
        }
        index.setColumnName(columnName);
        index.setIndexName(indexName);
        return index;
    }
    
    /**
     * 根据SquidIndexs对象获取相关的TableIndex初始化设置
     * @param adapter
     * @param squidIndexs
     * @return
     */
    public TableIndex getColumnNameByIndex(IRelationalDataManager adapter, SquidIndexes squidIndexs, int isnosql){
    	TableIndex index = new TableIndex();
    	String indexName = squidIndexs.getIndex_name();
    	ManagerSquidFlowProcess.setColumnName(adapter, squidIndexs);
        String columnName = "";
        StringBuffer colName = new StringBuffer();
        String tempName = null;
		if (StringUtils.isNotNull(squidIndexs.getColumn_name_1())){
			colName.append(",").append(squidIndexs.getColumn_name_1());
		}
		if (StringUtils.isNotNull(squidIndexs.getColumn_name_2())){
			colName.append(",").append(squidIndexs.getColumn_name_2());
		}
		if (StringUtils.isNotNull(squidIndexs.getColumn_name_3())){
			colName.append(",").append(squidIndexs.getColumn_name_3());
		}
		if (StringUtils.isNotNull(squidIndexs.getColumn_name_4())){
			colName.append(",").append(squidIndexs.getColumn_name_4());
		}
		if (StringUtils.isNotNull(squidIndexs.getColumn_name_5())){
			colName.append(",").append(squidIndexs.getColumn_name_5());
		}
		if (StringUtils.isNotNull(squidIndexs.getColumn_name_6())){
			colName.append(",").append(squidIndexs.getColumn_name_6());
		}
		if (StringUtils.isNotNull(squidIndexs.getColumn_name_7())){
			colName.append(",").append(squidIndexs.getColumn_name_7());
		}
		if (StringUtils.isNotNull(squidIndexs.getColumn_name_8())){
			colName.append(",").append(squidIndexs.getColumn_name_8());
		}
		if (StringUtils.isNotNull(squidIndexs.getColumn_name_9())){
			colName.append(",").append(squidIndexs.getColumn_name_9());
		}
		if (StringUtils.isNotNull(squidIndexs.getColumn_name_10())){
			colName.append(",").append(squidIndexs.getColumn_name_10());
		}
        if (colName.length()>0){
        	columnName = colName.toString().substring(1);
        }else{
        	return null;
        }
        index.setColumnName(columnName);
        index.setIndexName(indexName);
        return index;
    }

    //是否存在表名
    private boolean isExistsSquidTableByTbName(String tableName, INewDBAdapter adaoterSource) {
		boolean is_exists = true;
		try {
			adaoterSource.getRecordCount(tableName);
		} catch (Exception e) {
			//e.printStackTrace();
			logger.error(e.getMessage());
			is_exists = false;
		}
		return is_exists;
	}
    
    /**
     * 删除索引
     * 
     * @param squidIndexs
     * @author bo.dang
     * @throws Exception 
     * @date 2014年5月17日
     */
    public Boolean dropSquidIndexsFromDB(SquidIndexes squidIndexs, ReturnValue out){
    	Boolean deleteFlag = false;
        try {
            adapter.openSession();
            deleteFlag  = this.dropSquidIndexsFromDB(adapter, squidIndexs, out);
        } catch (Exception e) {
            logger.error("删除SquidIndexs异常", e);
            try {
                adapter.rollback();
            } catch (SQLException e1) { // 数据库回滚失败（程序不能处理该异常）！
                logger.fatal("rollback err!", e1);
            }
        } finally {
            adapter.closeSession();
        }
        return deleteFlag;
    }
    /**
     * 删除索引
     * 
     * @param squidIndexs
     * @author bo.dang
     * @throws Exception 
     * @date 2014年5月17日
     */
    public Boolean dropSquidIndexsFromDB(IRelationalDataManager adapter, SquidIndexes squidIndexs, ReturnValue out) throws Exception {
    	Boolean dropFlag = false;
        int squidId = squidIndexs.getSquid_id();
        String sql = "select * from ds_squid where id="+squidId;
		Squid squid = adapter.query2Object(true, sql, null, Squid.class);
		if (squid==null){
			out.setMessageCode(MessageCode.NODATA);
			return false;
		}
		String tableName = "";
        // 已经落地的squid
        Class c = SquidTypeEnum.classOfValue(squid.getSquid_type());
		boolean is_persisted = false;
		int destination_squid_id = 0;
		if (c==DataSquid.class
				||c==StageSquid.class
				||c==ExceptionSquid.class
				||c==StreamStageSquid.class){
			sql = "select * from ds_squid where id="+squidId;
			DataSquid dataSquid = adapter.query2Object(true,
					sql, null, DataSquid.class);
			if (dataSquid!=null){
				is_persisted = dataSquid.isIs_persisted();
				destination_squid_id = dataSquid.getDestination_squid_id();
				tableName = dataSquid.getTable_name();
			}
		}else if (c==DocExtractSquid.class){
			sql = "select * from ds_squid where id="+squidId;
			DocExtractSquid docSquid = adapter.query2Object(true, 
					sql, null, DocExtractSquid.class);
			if (docSquid!=null){
				is_persisted = docSquid.isIs_persisted();
				destination_squid_id = docSquid.getDestination_squid_id();
				tableName = docSquid.getTable_name();
			}
		} else if (c==HBaseExtractSquid.class){
			sql = "select * from ds_squid where id="+squidId;
			HBaseExtractSquid HBaseSquid = adapter.query2Object(true,
					sql, null, HBaseExtractSquid.class);
			if (HBaseSquid!=null){
				is_persisted = HBaseSquid.isIs_persisted();
				destination_squid_id = HBaseSquid.getDestination_squid_id();
				tableName = HBaseSquid.getTable_name();
			}
		}
		if (StringUtils.isNull(tableName)) {
			out.setMessageCode(MessageCode.NODATA);
			return false;
        }
		if (is_persisted){
			if(destination_squid_id>0){
				Squid dbSquid = adapter.query2Object(true,"select * from ds_squid ds where ds.id="+destination_squid_id,null,DbSquid.class);
				INewDBAdapter adaoterSource=null;
				DBConnectionInfo dbs = null;
				if(dbSquid.getSquid_type()==SquidTypeEnum.DBSOURCE.value()
						||dbSquid.getSquid_type()==SquidTypeEnum.CLOUDDB.value() || dbSquid.getSquid_type()==SquidTypeEnum.TRAININGDBSQUID.value()){
					sql = "select * from ds_squid where id="+destination_squid_id;
					DbSquid db = adapter.query2Object(true, sql, null, DbSquid.class);
					if (db!=null){
						// 获取数据源
						dbs = DBConnectionInfo.fromDBSquid(db);
						adaoterSource = AdapterDataSourceManager.getAdapter(dbs);
						boolean isExists = this.isExistsSquidTableByTbName(tableName, adaoterSource);
						if (!isExists){
							out.setMessageCode(MessageCode.ERR_PERSISTTABLE_NOT_TABLENAME);
							return false;
						}
						TableIndex index = this.getColumnNameByIndex(adapter, dbs.getDbType().value(), squidIndexs);
						if (index!=null){
							try {
								adaoterSource.deleteIndex(tableName, index.getIndexName());
								dropFlag = true;
							} catch (Exception e) {
								logger.error("删除失败index："+e.getMessage());
							}
						}
		                adaoterSource.close();
					}
				}else if (dbSquid.getSquid_type()==SquidTypeEnum.MONGODB.value()){
					sql = "select * from ds_squid where id="+destination_squid_id;
					NOSQLConnectionSquid nosql = adapter.query2Object(true, sql, null, NOSQLConnectionSquid.class);
					if (nosql!=null){
						// 获取数据源
						dbs = DBConnectionInfo.fromNOSQLSquid(nosql);
						adaoterSource= AdapterDataSourceManager.getAdapter(dbs);
						int cnt = adaoterSource.getRecordCount(tableName);
						if (cnt==0){
							out.setMessageCode(MessageCode.ERR_PERSISTTABLE_NOT_TABLENAME);
							return false;
						}
						TableIndex index = this.getColumnNameByIndex(adapter, squidIndexs, 1);
						if (index!=null){
							try {
								adaoterSource.deleteIndex(tableName, index);
								dropFlag = true;
							} catch (Exception e) {
								logger.error("删除未存在的index："+index.getIndexName());
							}
						}
		                adaoterSource.close();
					}
				}
			}else{
				DBConnectionInfo dbs = new DBConnectionInfo();
		        dbs.setHost(SysConf.getValue("hbase.host"));
		        dbs.setPort(Integer.parseInt(SysConf.getValue("hbase.port")));
		        dbs.setDbType(DataBaseType.HBASE_PHOENIX);
				INewDBAdapter adaoterSource= AdapterDataSourceManager.getAdapter(dbs);
				boolean isExists = this.isExistsSquidTableByTbName(tableName, adaoterSource);
				if (!isExists){
					out.setMessageCode(MessageCode.ERR_PERSISTTABLE_NOT_TABLENAME);
					return false;
				}
				TableIndex index = this.getColumnNameByIndex(adapter, dbs.getDbType().value(), squidIndexs);
				if (index!=null){
					try {
						adaoterSource.deleteIndex(tableName, index.getIndexName());
						dropFlag = true;
					} catch (Exception e) {
						logger.error("删除失败index："+e.getMessage());
					}
				}
                adaoterSource.close();
			}
		}else{
			out.setMessageCode(MessageCode.ERR_PERSISTTABLE_NOT_CHECKED);
			return false;
		}
        return dropFlag;
    }
    
    private String initColumnNameByDbType(int dbType, String columnName){
		String temp = "";
		DataBaseType dataBaseType = DataBaseType.parse(dbType);
		switch (dataBaseType) {
		case SQLSERVER:
			temp = "["+columnName+"]";
			break;
		case MYSQL:
			temp = "`"+columnName+"`";
			break;
		case ORACLE:
		case DB2:
			temp = "\""+columnName+"\"";
			break;
		default:
			temp = columnName;
			break;
		}
		return temp;
	}
}
