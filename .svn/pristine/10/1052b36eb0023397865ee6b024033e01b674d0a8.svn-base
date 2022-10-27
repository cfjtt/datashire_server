package com.eurlanda.datashire.dao.impl;

import cn.com.jsoft.jframe.utils.ValidateUtils;
import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.annotation.MultitableMapping;
import com.eurlanda.datashire.dao.ISquidDao;
import com.eurlanda.datashire.entity.*;
import com.eurlanda.datashire.enumeration.SquidTypeEnum;
import com.eurlanda.datashire.utility.DatabaseException;
import com.eurlanda.datashire.utility.JsonUtil;
import com.eurlanda.datashire.utility.StringUtils;
import org.apache.log4j.Logger;

import java.sql.Clob;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Squid简易的实现类
 * @author eurlanda
 *
 */
public class SquidDaoImpl extends BaseDaoImpl implements ISquidDao {
	
	private static Logger logger = Logger.getLogger(SquidDaoImpl.class);// 记录日志
	
	public SquidDaoImpl(){
	}
	
	public SquidDaoImpl(IRelationalDataManager adapter){
		this.adapter = adapter;
	}

	@Override
	public <T> Map<String, Object> getMapForCond(int squidId, Class<T> c) throws Exception{
		String tableName="";
		MultitableMapping tm = c.newInstance().getClass().getAnnotation(MultitableMapping.class);
		if(tm!=null&&tm.name()!=null&&tm.name().length>0){
			int i = tm.name().length - 1;
			if (!ValidateUtils.isEmpty(tm.name()[i])){
				tableName = tm.name()[i];
			}
		}
//		String sql = "select * from ds_squid ds inner join "+tableName+" dc on ds.id=dc.id where ds.id="+squidId;
		String sql = "select * from ds_squid where id="+squidId;

		return adapter.query2Object(true, sql, null);
	}
	
	@Override
	public <T> T getSquidForCond(int squidId, Class<T> c) throws Exception{
		/*String tableName="";
		MultitableMapping tm = c.newInstance().getClass().getAnnotation(MultitableMapping.class);
		if(tm!=null&&tm.name()!=null&&tm.name().length>0){
			int i = tm.name().length - 1;
			if (!ValidateUtils.isEmpty(tm.name()[i])){
				tableName = tm.name()[i];
			}
		}*/
		String sql = "select * from ds_squid ds  where ds.id="+squidId;
		return adapter.query2Object(true, sql, null, c);
	}

	@Override
	public <T> List<T> getNextSquidsForSquidID(int squidID, Class<T> c) throws Exception{
		String tableName="";
		MultitableMapping tm = c.newInstance().getClass().getAnnotation(MultitableMapping.class);
		if(tm!=null&&tm.name()!=null&&tm.name().length>0){
			int i = tm.name().length - 1;
			if (!ValidateUtils.isEmpty(tm.name()[i])){
				tableName = tm.name()[i];
			}
		}
		String sql = "select s.*,dc.* from ds_squid s inner join " +
				tableName + " dc on dc.ID = s.ID inner join ds_squid_link l on l.TO_SQUID_ID = dc.ID where l.FROM_SQUID_ID = " +
				squidID;
		return adapter.query2List(true, sql, null, c);
	}

	@Override
	public <T> List<T> getSquidListForParams(int squidType, int squidFlowId,
			String squidIds, Class<T> c) throws Exception {
		String tableName="";
		MultitableMapping tm = c.newInstance().getClass().getAnnotation(MultitableMapping.class);
		if(tm!=null&&tm.name()!=null&&tm.name().length>0){
			int i = tm.name().length - 1;
			if (!ValidateUtils.isEmpty(tm.name()[i])){
				tableName = tm.name()[i];
			}
		}
		StringBuffer sql = new StringBuffer(); 
		sql.append("select * from ds_squid ds ");
		/*if (c!=SquidModelBase.class){
			sql.append(" inner join "+tableName+" dc on ds.id=dc.id ");
		}*/
		sql.append(" where ds.squid_flow_id="+squidFlowId);
		if (squidType>=0){
			sql.append(" and ds.squid_type_id="+squidType);
		}
		if (!ValidateUtils.isEmpty(squidIds)){
			sql.append(" and ds.id in ("+squidIds+")");
        }
		List<T> list = adapter.query2List(true, sql.toString(), null, c);
		//logger.info("squidType:"+squidType+", squidCount:"+list.size());
		return list;
	}

	@Override
	public Squid getSquidByName(int squidFlowId, String name){
		Map<String, Object> params = new HashMap<String, Object>();
		params.put("name", name);
		params.put("squid_flow_id", squidFlowId);
		return adapter.query2Object(true, params, Squid.class);
	}
	
	/**
     * 获取所有孤立的squid且不是特殊的类型(特殊包括：Extract、Exception、Report)
     * @param squidFlowId
     * @return
     * @throws SQLException 
     */
	@Override
    public List<Integer> getSquidIdsForNoLinkBySquidFlow(int squidFlowId) throws SQLException{
    	StringBuffer sqlStr = new StringBuffer();
    	sqlStr.append("select id from ds_squid where squid_flow_id="+squidFlowId+" and id not in (");
    	sqlStr.append("select from_squid_id from ds_squid_link where squid_flow_id="+squidFlowId+" union ");
    	sqlStr.append("select to_squid_id from ds_squid_link where squid_flow_id="+squidFlowId+")");
    	List<Integer> list  = new ArrayList<Integer>();
    	List<Map<String, Object>> mapList = adapter.query2List(true, sqlStr.toString(), null);
    	if(mapList!=null&&mapList.size()>0){
    		for (Map<String, Object> map : mapList) {
				if (map!=null&&map.containsKey("ID")){
					list.add(Integer.parseInt(map.get("ID")+""));
				}
			}
    	}
    	return list;
    }
	
	/**
     * 修改squid属性
     * @param squid
     * @return
     * @throws SQLException
     */
	@Override
	public int updateDataSquid(DataSquid squid) throws SQLException{
    	String sql = "update ds_squid set process_mode=?,is_persisted=?,truncate_existing_data_flag=?,cdc=?,Is_indexed=? where id=?"; //增加了对清空数据和索引的更新---dengzhengpeng
    	List<Object> params = new ArrayList<Object>();
		params.add(squid.getProcess_mode());             //取消落地，全量恢复为增量
    	params.add(squid.isIs_persisted()?"Y":"N");
		params.add(squid.isTruncate_existing_data_flag());  //取消落地时同时取消清空数据
    	params.add(squid.getCdc());
		params.add(squid.isIs_indexed()?"Y":"N");    //取消落地时同事取消索引
		params.add(squid.getId());
    	int cnt = adapter.execute(sql, params);
    	return cnt;
    }
	
	@Override
	public String getSquidNameByColumnId(int columnId) throws SQLException{
		String sql  = "select ds.* from ds_squid ds inner join ds_column dc on ds.id=dc.squid_id where dc.id="+columnId;
		Squid squid = adapter.query2Object(true, sql, null, Squid.class);
		if (squid!=null){
			return squid.getName();
		}
		return "";
	}
	
	@Override
	public int setPersistenceByIds(String ids) throws DatabaseException{
		int count = 0;
		String sql = "update ds_squid set is_persisted='Y' where id in ("+ids+");";
		count += adapter.execute(sql);
		return count;
	}

	@Override
	public int setDestSquidByIds(int destSquidId,String ids) throws Exception {
		String sql = "update ds_squid set destination_squid_id="+destSquidId+" where id in ("+ids+");";
		int count = adapter.execute(sql);
		return count;
	}

	@SuppressWarnings("unchecked")
	public List<Map<String, Object>> getFromSquidListByChildId(int childId) throws Exception{
		List<Map<String, Object>> mapList = new ArrayList<Map<String, Object>>();
		String sql = "select t1.* from ds_squid t1 inner join ds_squid_link t2 on t1.id=t2.from_squid_id where t2.to_squid_id ="+childId;
		List<Squid> squidList = adapter.query2List(true, sql, null, Squid.class);
		if (squidList!=null&&squidList.size()>0){
			for (Squid squid : squidList) {
				Class c = SquidTypeEnum.classOfValue(squid.getSquid_type());
				Map<String, Object> map = this.getMapForCond(squid.getId(), c);
				mapList.add(map);
			}
		}
		return mapList;
	}
	
	/**
	 * 获取抓取信息
	 */
	@Override
	public List<Url> getDSUrlsBySquidId(int squidId){
		String sql = "select * from ds_url where squid_id="+squidId;
		return adapter.query2List(true, sql, null, Url.class);
	}
	
	/**
	 * 更新落地引用squid属性
	 * @param squidId
	 * @param squidMap
	 * @return
	 * @throws Exception
	 */
	@Override
	public int modifyDestinationBySquidId(int squidId, 
			Map<Integer, Integer> squidMap) throws Exception{
		DataSquid dataSquid = this.getSquidForCond(squidId, DataSquid.class);
		if (dataSquid!=null&&dataSquid.getDestination_squid_id()>0){
			int oldSquidId =  dataSquid.getDestination_squid_id();
			if (squidMap.containsKey(oldSquidId)){
				dataSquid.setDestination_squid_id(squidMap.get(oldSquidId));
				adapter.update2(dataSquid);
			}
		}else{
			DocExtractSquid squid = this.getSquidForCond(squidId, DocExtractSquid.class);
			if (squid!=null&&squid.getDestination_squid_id()>0){
				int oldSquidId =  squid.getDestination_squid_id();
				if (squidMap.containsKey(oldSquidId)){
					squid.setDestination_squid_id(squidMap.get(oldSquidId));
					adapter.update2(squid);
				}
			}
		}
		return 0;
	}
	
	@Override
	public int modifyTemplateBySquidId(int squidId, int oldSquidId, Map<String, Integer> columnMap) throws Exception{
		GISMapSquid mapSquid = this.getSquidForCond(squidId, GISMapSquid.class);
		if (mapSquid!=null&&StringUtils.isNotNull(mapSquid.getMap_template())){
			ReportMapParams rm = JsonUtil.json2Object(mapSquid.getMap_template(), ReportMapParams.class);
			int keyColumnId = rm.getKey_column_id();
			int valColumnId = rm.getVal_column_id();
			int leftPointId = rm.getLeft_point_id();
			int rightPointId = rm.getRight_point_id();
			if (keyColumnId>0&&columnMap.containsKey(oldSquidId+"_"+keyColumnId)){
				rm.setKey_column_id(columnMap.get(oldSquidId+"_"+keyColumnId));
			}
			if (valColumnId>0&&columnMap.containsKey(oldSquidId+"_"+valColumnId)){
				rm.setVal_column_id(columnMap.get(oldSquidId+"_"+valColumnId));
			}
			if (leftPointId>0&&columnMap.containsKey(oldSquidId+"_"+leftPointId)){
				rm.setLeft_point_id(columnMap.get(oldSquidId+"_"+leftPointId));
			}
			if (rightPointId>0&&columnMap.containsKey(oldSquidId+"_"+rightPointId)){
				rm.setRight_point_id(columnMap.get(oldSquidId+"_"+rightPointId));
			}
			String template = JsonUtil.toGsonString(rm); 
			System.out.println(template);
			mapSquid.setMap_template(template);
			String strsql = "update DS_GIS_MAP_SQUID set map_template=? where id=?";
			Clob c = new javax.sql.rowset.serial.SerialClob(mapSquid.getMap_template().toCharArray());
			List<Object> list = new ArrayList<Object>();
			list.add(c);
			list.add(mapSquid.getId());
			adapter.execute(strsql, list);
		}
		return 0;
	}
	
	@Override
	public int modifyTemplateImportBySquidId(int squidId, Map<Integer, Integer> columnMap) throws Exception{
		GISMapSquid mapSquid = this.getSquidForCond(squidId, GISMapSquid.class);
		if (mapSquid!=null&&StringUtils.isNotNull(mapSquid.getMap_template())){
			ReportMapParams rm = JsonUtil.json2Object(mapSquid.getMap_template(), ReportMapParams.class);
			int keyColumnId = rm.getKey_column_id();
			int valColumnId = rm.getVal_column_id();
			int leftPointId = rm.getLeft_point_id();
			int rightPointId = rm.getRight_point_id();
			if (keyColumnId>0&&columnMap.containsKey(keyColumnId)){
				rm.setKey_column_id(columnMap.get(keyColumnId));
			}
			if (valColumnId>0&&columnMap.containsKey(valColumnId)){
				rm.setVal_column_id(columnMap.get(valColumnId));
			}
			if (leftPointId>0&&columnMap.containsKey(leftPointId)){
				rm.setLeft_point_id(columnMap.get(leftPointId));
			}
			if (rightPointId>0&&columnMap.containsKey(rightPointId)){
				rm.setRight_point_id(columnMap.get(rightPointId));
			}
			mapSquid.setMap_template(JsonUtil.toGsonString(rm));
			String strsql = "update DS_GIS_MAP_SQUID set map_template=? where id=?";
			Clob c = new javax.sql.rowset.serial.SerialClob(mapSquid.getMap_template().toCharArray());
			List<Object> list = new ArrayList<Object>();
			list.add(c);
			list.add(mapSquid.getId());
			adapter.execute(strsql, list);
		}
		return 0;
	}
	
	@Override
	public int modifyDataMiningBySquidId(int squidId, Map<Integer, Integer> squidMap) throws Exception{
		DataMiningSquid dataMining = getSquidForCond(squidId, DataMiningSquid.class);
		if (dataMining!=null&&dataMining.getCategorical_squid()>0){
			int oldSquidId = dataMining.getCategorical_squid();
			if (oldSquidId>0 && squidMap.containsKey(oldSquidId)){
				dataMining.setCategorical_squid(squidMap.get(oldSquidId));
				String sql ="update ds_squid set categorical_squid=? where id=?";
				List<Object> list = new ArrayList<Object>();
				list.add(dataMining.getCategorical_squid());
				list.add(squidId);
				adapter.execute(sql, list);
			}
		}
		return 0;
	}
	
	@Override
	public int modifySourceTableIdBySquidId(int squidId, Map<Integer, Integer> sourceTableMap,Class<?> cz) throws Exception{
		//这里有一个问题，查找都是ds_data_squid表始终是查不到数据的，所以导致这里的更新source_table_id
		//的方法始终不执行。目前还没有找到具体在哪边添加的ds_data_squid数据，采取的方案是更新相对应的ExtractSquid表
		//获取表名
		MultitableMapping tm = cz.newInstance().getClass().getAnnotation(MultitableMapping.class);
		String className=cz.getSimpleName();
		System.out.println("类的名字: "+className);
		DataSquid dataSquid=getSquidForCond(squidId,DataSquid.class);
		if(dataSquid!=null&&dataSquid.getSource_table_id()>0){
			int oldSourceTableId = dataSquid.getSource_table_id();
			if (oldSourceTableId>0 && sourceTableMap.containsKey(oldSourceTableId)){
				dataSquid.setSource_table_id(sourceTableMap.get(oldSourceTableId));
				//String sql ="update ds_data_squid set source_table_id=? where id=?";
				List<Object> list = new ArrayList<Object>();
				list.add(dataSquid.getSource_table_id());
				list.add(squidId);
				String sql="update ds_squid set source_table_id=? where id=?";
				//String docSql="update  set source_table_id=? where id=?";
				adapter.execute(sql, list);
				//adapter.execute(docSql,list);
			}
		}
		//这里只更新相对应的ExtractSquid类
		if(className.equals("DocExtractSquid")
				|| className.equals("HBaseExtractSquid")
				|| className.equals("KafkaExtractSquid")){

			DataSquid dataDocSquid = (DataSquid) getSquidForCond(squidId,cz);
			if(dataDocSquid!=null&&dataDocSquid.getSource_table_id()>0){
				int oldSourceTableId = dataDocSquid.getSource_table_id();
				if (oldSourceTableId>0 && sourceTableMap.containsKey(oldSourceTableId)){
					dataDocSquid.setSource_table_id(sourceTableMap.get(oldSourceTableId));
					//String sql ="update ds_data_squid set source_table_id=? where id=?";
					List<Object> list = new ArrayList<Object>();
					list.add(dataDocSquid.getSource_table_id());
					list.add(squidId);
					//String sql="update ds_data_squid set source_table_id=? where id=?";
					String docSql="update ds_squid set source_table_id=? where id=?";
					adapter.execute(docSql, list);
					//adapter.execute(docSql,list);
				}
			}
		}


		return 0;
	}

	@Override
	public List<Integer> getNextSquidIdsById(int squidId) throws SQLException {
		String sql = "select link.to_squid_id from DS_SQUID_LINK link where link.from_squid_id=" + squidId;
		List<Map<String, Object>> maps =adapter.query2List(true,sql, null);

		List<Integer> squidIds = new ArrayList<>();
		if(maps != null && maps.size()>0) {
			for(Map<String, Object> m : maps) {
				squidIds.add((Integer)m.get("TO_SQUID_ID"));
			}
		}
		return squidIds;
	}

	@Override
	public List<Squid> getUpSquidIdsById(int squidId) throws SQLException {
		String sql="SELECT " +
						"* " +
					"FROM " +
						"ds_squid ds " +
					"WHERE " +
						"ds.id IN (" +
							"SELECT " +
								"dslink2.FROM_SQUID_ID " +
							"FROM " +
								"ds_squid_link dslink2 " +
							"WHERE " +
								"dslink2.to_squid_id  ="+squidId+")";
		List<Squid> list=adapter.query2List(true,sql,null,Squid.class);
		return list;
	}
}
