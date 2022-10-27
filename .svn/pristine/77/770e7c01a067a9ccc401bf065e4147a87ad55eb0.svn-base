package com.eurlanda.datashire.adapter.db;

import com.eurlanda.datashire.entity.DBConnectionInfo;
import com.eurlanda.datashire.entity.operation.BasicTableInfo;
import com.eurlanda.datashire.entity.operation.ColumnInfo;
import com.eurlanda.datashire.entity.operation.TableIndex;
import com.eurlanda.datashire.exception.SystemErrorException;
import com.eurlanda.datashire.utility.MessageCode;
import com.eurlanda.datashire.utility.NoSqlConnectionUtil;
import com.eurlanda.datashire.utility.ReturnValue;
import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoTimeoutException;

import java.util.ArrayList;
import java.util.List;

/**
 * Mongodb数据库adapter
 *
 * @author yi.zhou
 */
public class MongodbAdapter extends AbsDBAdapter implements INewDBAdapter {
    private DB mongoDBAdapter;

    public MongodbAdapter(DBConnectionInfo info) {
        super();
        if (mongoDBAdapter == null) {
            mongoDBAdapter = NoSqlConnectionUtil.createMongoDBConnection(info);
        }
//		DBCollection dbCollection = d.getCollection(dbSquid.getTable_name());
        // TODO Auto-generated constructor stub
    }

    @Override
    public void deleteTable(String tableName) {
        this.mongoDBAdapter.getCollection(tableName).drop();
    }

    @Override
    public boolean deleteMongoTable(String tableName) {
        try {
            boolean flag=this.mongoDBAdapter.collectionExists(tableName);
            if(flag){
                this.mongoDBAdapter.getCollection(tableName).drop();
            } else {
                return false;
            }

        } catch(Exception e){
            throw e;
        }
        return true;
    }

    /**
     * 取得数据库中所有的表
     */
    @Override
    public List<BasicTableInfo> getAllTables(final String filter) {
        final List<BasicTableInfo> tables = new ArrayList<BasicTableInfo>();

        return tables;
    }

    @Override
    public void beginTransaction() {
        try {
//            this.connection.setAutoCommit(false);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    @Override
    public void commit() {
//        try {
//            this.connection.commit();
//        } catch (SQLException e) {
//            e.printStackTrace();
//        }
    }
    @Override
    public void rollback(){

    }
    @Override
    public void createTable(BasicTableInfo table, ReturnValue out) {
        beginTransaction();
        try {
//			List<String> list = new ArrayList<String>();
            List<String> uList = new ArrayList<String>();
            String tableName = table.getTableName();
//			String sql = "create table " + table.getTableName();

            if (table == null || table.getColumnList() == null || table.getColumnList().size() == 0) {
                throw new SystemErrorException(MessageCode.ERR_CREATE_TABLE, "创建表失败，表名错误或者列为空。");
            }
            if (tableName.indexOf("$") == 0) {//mongoDB 可以使用点但不可以使用 $。这是保留字符
                out.setMessageCode(MessageCode.ERR_ORACLE_TABLENAME);
                return;
            }
            BasicDBObjectBuilder basicDBObjectBuilder = BasicDBObjectBuilder.start();
            DBObject dbobject = basicDBObjectBuilder.get();
            DBCollection dbCollection=null;
            //先判断集合是否存在
            if(mongoDBAdapter.collectionExists(tableName)){
                out.setMessageCode(MessageCode.WARN_PERSISTTABLEALREADYEXIST);
                return;
            } else {
                dbCollection = mongoDBAdapter.createCollection(tableName, dbobject);
            }

//			sql += "(";
            for (ColumnInfo col : table.getColumnList()) {
//				sql += "\n\t" + this.buildColumnSql(table.getDbType().value(), col) + ",";
                if (col.isPrimary()) {
                    uList.add(col.getName());
                } else if (col.isUniqueness()) {
                    uList.add(col.getName());
                }
                for (int i = 0; i < table.getTableIndexList().size(); i++) {
                    if (table.getTableIndexList().get(i).getColumnName().equals(col.getName())) {
                        table.getTableIndexList().remove(i);
                        i--;
                    }
                }
            }

            //所有唯一全是索引
//			if (list.size()>0){//主键list
//				String keyName = "pk_" + tableName + "_temp";
//				String tempName = "";
//				for (String key : list) {
//					tempName += "," + initColumnNameByDbType(table.getDbType().value(), key) + "";
//				}
//				if (tempName!=""){
//					tempName = tempName.substring(1);
//				}
////				sql += "\n\tCONSTRAINT " + keyName + " PRIMARY KEY (" + tempName + "),";
//			}//唯一list
            if (uList.size() > 0) {
				String keyName = "uq_" + tableName + "_temp";
				String tempName = "";
                for (String key : uList) {
                    dbobject.put(key,1);
                    dbCollection.createIndex(dbobject, keyName+"_"+key, true);
                }
//				if (tempName!=""){
//					tempName = tempName.substring(1);
//				}
//				sql += "\n\tCONSTRAINT " + keyName + " UNIQUE (" + tempName + "),";
            }
//			sql += "\n\t)";
//			logger.debug("建表:" + sql);
//			System.out.println(sql);
//			this.jdbcTemplate.update(sql);
            // 外键
            // 索引
            for (TableIndex key : table.getTableIndexList()) {
//				this.addIndex(table.getTableName(), key);
                dbCollection.createIndex(null, key.getColumnName(), false);
            }
            this.commit();
        } catch (Exception e) {
            e.printStackTrace();
            this.rollback();
            if(e instanceof  MongoTimeoutException){
                out.setMessageCode(MessageCode.ERR_DBSOURCE_CONNECTION);
            } else {
                out.setMessageCode(MessageCode.SQL_ERROR);
            }

        }
    }

    @Override
    public Integer getRecordCount(String tableName) {
//        DBCollection dbCollection = mongoDBAdapter.getCollection(tableName);
        boolean b = mongoDBAdapter.collectionExists(tableName);
//        int count = this.jdbcTemplate.queryForInt(sql);
        if(b) {
            return 1;
        }else{
            return 0;
        }
    }

    @Override
    public void addIndex(String tableName, TableIndex index) {
    	DBCollection coll = this.mongoDBAdapter.getCollection(tableName);
        DBObject obj = new BasicDBObject();
        String[] ss = index.getColumnName().split(",");
        if (ss!=null&&ss.length>0){
        	for (int i = 0; i < ss.length; i++) {
				obj.put(ss[i], 1);
			}
        }
        coll.createIndex(obj, index.getIndexName());
    }
    
    @Override
    public void deleteIndex(String tableName, TableIndex index) {
    	DBCollection coll = this.mongoDBAdapter.getCollection(tableName);
    	coll.dropIndex(index.getIndexName());
    }

    public static void main(String[] args) {
        /*
		Map<String, Object> map = new HashMap<String, Object>();
		map.put("name", "张三");  //name
		map.put("age", 27);  //age
		*/
    }
}
