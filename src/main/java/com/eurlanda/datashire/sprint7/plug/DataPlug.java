package com.eurlanda.datashire.sprint7.plug;

import com.eurlanda.datashire.adapter.DataAdapterFactory;
import com.eurlanda.datashire.adapter.IDBAdapter;
import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.entity.Column;
import com.eurlanda.datashire.entity.DBSourceTable;
import com.eurlanda.datashire.entity.DbSquid;
import com.eurlanda.datashire.entity.SourceColumn;
import com.eurlanda.datashire.entity.SourceTable;
import com.eurlanda.datashire.entity.Squid;
import com.eurlanda.datashire.sprint7.service.squidflow.ConnectProcess;
import com.eurlanda.datashire.sprint7.service.squidflow.DBSquidService;
import com.eurlanda.datashire.utility.MessageCode;
import com.eurlanda.datashire.utility.ReturnValue;
import com.eurlanda.datashire.utility.StringUtils;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Data处理类
 * 数据处理类，获得目标数据的数据信息：例如目标数据库的所有表信息，表的相关列信息；以及相应表的数据信息
 * <p>
 * Title : 
 * </p>
 * <p>
 * Description: 
 * </p>
 * <p>
 * Author :Cheryl 2013-8-22
 * </p>
 * <p>
 * update :Cheryl 2013-8-22
 * </p>
 * <p>
 * Department :  JAVA后端研发部
 * </p>
 * Copyright : ©2012-2013 悦岚（上海）数据服务有限公司
 * </p>
 */
public class DataPlug extends SupportPlug{
	static Logger logger = Logger.getLogger(DataPlug.class);// 记录日志

	public DataPlug(String token) {
		super(token);
	}
	
	public DataPlug(IDBAdapter adapter) {
		this.adapter=adapter;
	}
	public DataPlug(String token, IDBAdapter adapter) {
		this.token = token;
		this.adapter=adapter;
	}
	/**
	 * 根据squidID获得表信息
	 * <p>
	 * 作用描述：
	 * 根据squidID查询连接信息切换到相对应的数据库获得该数据库下所有表名并把表的列信息查询封装返回
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 *@param source_squid_id
	 *@param out 异常处理
	 *@return
	 */
	public List<SourceTable> getSourceTable(int source_squid_id, ReturnValue out,IRelationalDataManager adapter) {
		logger.debug(String.format("getTables-id=%s", source_squid_id));
		List<SourceTable> sourceTables = new ArrayList<SourceTable>();
		ConnectProcess connectProcess=null;
		IRelationalDataManager adapter3 = null;
		DataAdapterFactory dataFactory = null;
		List<SourceColumn> allColumns =null;
		//Map<Integer,List<SourceColumn>> sourceColumnMap= new HashMap<Integer,List<SourceColumn>>();
		if (null != adapter) {
			connectProcess = new ConnectProcess(adapter);
			adapter3 = adapter;
		} else {
			connectProcess = new ConnectProcess(token);
			dataFactory = DataAdapterFactory.newInstance();
			adapter3 = dataFactory.getDataManagerByToken(token);
			adapter3.openSession();
		}
		try {
			// 根据id获取所有的表
			List<DBSourceTable> dbSourceTables = connectProcess.getDBSourceTable(source_squid_id, out);
			if (dbSourceTables.size() > 0) {
				// 获取所有表的所有列集合
				String ids = this.getAllIds(dbSourceTables);
				String sql = "SELECT * FROM DS_SOURCE_COLUMN  WHERE source_table_id in ("
						+ ids + ")";
				allColumns = adapter3.query2List(true, sql, null,SourceColumn.class);
			}
			for(int i=0;i<dbSourceTables.size();i++)
			{
				SourceTable sourceTable=new SourceTable();
				sourceTable.setId(dbSourceTables.get(i).getId());
				sourceTable.setTableName(dbSourceTables.get(i).getTable_name());
				sourceTable.setIs_extracted(dbSourceTables.get(i).isIs_extracted());
				//String sql = "SELECT * FROM DS_SOURCE_COLUMN  WHERE source_table_id="+dbSourceTables.get(i).getId();
				//List<SourceColumn> sourceColumns = adapter3.query2List(true, sql, null, SourceColumn.class);
				//List<SourceColumn> sourceColumns=connectProcess.getSourceColumn(dbSourceTables.get(i).getId(), out);
				Map<String,Column> map = new HashMap<String, Column>();
				//Map<String,String> map = new HashMap<String, String>(); // TODO 和前台联调sprint6时再改
				for(int j=0;j<allColumns.size();j++)
				{
					if(allColumns.get(j).getSource_table_id()==dbSourceTables.get(i).getId())
					{
						Column column=new Column();
						column.setId(allColumns.get(j).getId());
						column.setData_type(allColumns.get(j).getData_type());
						column.setPrecision(allColumns.get(j).getPrecision());
						column.setLength(allColumns.get(j).getLength());
						column.setNullable(allColumns.get(j).isNullable());
						column.setName(allColumns.get(j).getName());
						map.put(allColumns.get(j).getName(), column);
					}
				}
/*				for(int j=0;j<allColumns.size();j++){
					Column column=new Column();
					column.setId(allColumns.get(j).getId());
					column.setData_type(allColumns.get(j).getData_type());
					column.setPrecision(allColumns.get(j).getPrecision());
					column.setLength(allColumns.get(j).getLength());
					column.setNullable(allColumns.get(j).isNullable());
					column.setName(allColumns.get(j).getName());
//					DataStatusEnum colType = DataStatusEnum.parse(sourceColumns.get(j).getData_type());
//					String dataType = colType==null?"":colType.toString();
//					if (sourceColumns.get(j).getLength() > 0 && colType != DataStatusEnum.DATE) {
//						dataType += "(" + sourceColumns.get(j).getLength();
//						if(sourceColumns.get(j).getPrecision()>0){
//							dataType += ", " + sourceColumns.get(j).getPrecision();
//						}
//						dataType += ")";
//					}
					map.put(allColumns.get(j).getName(), column);
				}*/
				sourceTable.setColumnInfo(map);
				sourceTables.add(sourceTable);
			}
		}catch(Exception e){
			logger.error("error",e);
			out.setMessageCode(MessageCode.SQL_ERROR);
		}finally{
			if(null==adapter)
			{
				adapter3.closeSession();
			}
		}
		return sourceTables;
	}

	/**
	 * 表结构落地操作
	 * <p>
	 * 作用描述：
	 * 根据squidID　创建表结构落地
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 *@param fromSquidId　来源squidID
	 *@param toSquidId 目标SquidID
	 *@param out 异常处理
	 *@return
	 */
	public boolean createTable(int fromSquidId, int toSquidId, ReturnValue out) {
		IDBAdapter adaoterSource = null;
		try{
			// 从FromSquidId获得列的集合
			ColumnPlug columnPlug = new ColumnPlug(adapter);
			List<Column> columnList = columnPlug.getColumns(fromSquidId, out);
			//来源列信息为空
			if(columnList == null){
				//异常处理
				out.setMessageCode(MessageCode.ERR_SOURCECOLUMN_NULL);
				return false;
			}
			//从sourceSquid那获得表名
			SquidPlug squidPlug = new SquidPlug(adapter);
			Squid sourectSquid = squidPlug.getSquid(fromSquidId, out);
			if(sourectSquid == null || StringUtils.isNull(sourectSquid.getName())){
				//异常处理 表名为空
				out.setMessageCode(MessageCode.ERR_TABLENAME_NULL);
				return false;
			}
			DbSquid s2 = new DBSquidService(token).getDBSquid(toSquidId);
			adaoterSource = adapterFactory.makeAdapter(s2);
			//创建表结构
			return adaoterSource.createTable(sourectSquid.getTable_name(), columnList, out);
		}catch(Exception e){
			logger.error("error",e);
			out.setMessageCode(MessageCode.SQL_ERROR);
		}finally{
			if(adaoterSource!=null)adaoterSource.commitAdapter();
		}
		return false;
	}
    /**
     * 获取所有的DS_SOURCE_TABLE id
     * @param dbSourceTables
     * @return
     */
	private String getAllIds(List<DBSourceTable> dbSourceTables) {
		String ids = "";
		for (int i = 0; i < dbSourceTables.size(); i++) {
			ids += String.valueOf(dbSourceTables.get(i).getId());
			if (i != dbSourceTables.size() - 1) {
				ids += ",";
			}
		}
		return ids;
	}
	
}

