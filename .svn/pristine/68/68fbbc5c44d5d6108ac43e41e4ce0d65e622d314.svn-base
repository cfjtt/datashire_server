package com.eurlanda.datashire.dao.impl;

import cn.com.jsoft.jframe.utils.ValidateUtils;
import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.annotation.MultitableMapping;
import com.eurlanda.datashire.dao.IRepositoryDao;
import com.eurlanda.datashire.entity.FindModel;
import com.eurlanda.datashire.entity.FindResult;
import com.eurlanda.datashire.entity.PathItem;
import com.eurlanda.datashire.entity.Repository;
import com.eurlanda.datashire.enumeration.DSObjectType;
import com.eurlanda.datashire.enumeration.SquidTypeEnum;
import com.eurlanda.datashire.utility.StringUtils;

import java.sql.SQLException;
import java.util.*;

public class RepositoryDaoImpl extends BaseDaoImpl implements IRepositoryDao {
	
	public RepositoryDaoImpl(){
	}
	
	public RepositoryDaoImpl(IRelationalDataManager adapter){
		this.adapter = adapter;
	}
	
	/**
     * 查询squid对应的repository_id
     * @param
     * @param squidId
     * @return
     * @throws SQLException
     */
    public int getRepositoryIdBySquid(int squidId) throws SQLException{
    	StringBuffer sql = new StringBuffer();
    	sql.append("select dp.repository_id from ds_squid ds ");
		sql.append("inner join ds_squid_flow dsf on ds.squid_flow_id=dsf.id ");
		sql.append("inner join ds_project dp on dsf.project_id=dp.id ");
		sql.append("where ds.id = " + squidId);
		Map<String, Object> map = adapter.query2Object(true, sql.toString(), null);
		if (map!=null&&map.containsKey("REPOSITORY_ID")){
			return Integer.parseInt(map.get("REPOSITORY_ID")+"");
		}
		return 0;
    }

	/**
	 * 根据squidId查找出对应的projectId
	 * @param squidId
	 * @return
	 * @throws SQLException
     */
	@Override
	public int getProjectIdBySquid(int squidId) throws SQLException {

		String sql="select dp.id as project_id from ds_squid_flow dsf inner join ds_squid ds on dsf.id = ds.squid_flow_id inner join ds_project dp on dsf.project_id = dp.id where ds.id="+squidId;
		Map<String,Object> map = adapter.query2Object(true,sql,null);
		if (map!=null&&map.containsKey("PROJECT_ID")){
			return Integer.parseInt(map.get("PROJECT_ID")+"");
		}
		return 0;
	}

	/**
	 * 获取所有的DM squid
	 * @param repositoryId
	 * @param filterIds
	 * @return
	 * @throws SQLException
     */
	@Override
	public List<Map<String, Object>> getAllDataMiningSquidInRepository(
			boolean isFromCurrentProject,int repositoryId, String filterIds) throws SQLException {
		StringBuffer sql = new StringBuffer();
		sql.append("select dq.id,concat_ws('/',dsr.name,dp.name,dsf.name,dq.name) as name,");
		sql.append("(select output_data_type from ds_transformation where id=dtl.from_transformation_id) inputdatatype,dq.squid_type_id from ds_squid dq ");
		sql.append("inner join ds_squid_flow dsf on dq.squid_flow_id=dsf.id and squid_type_id in (21,22,23,24,25,26,27,28,29,30,48,61,63,64,65,66,67,71,72,75) ");
		sql.append("inner join ds_project dp on dsf.project_id=dp.id ");
		sql.append("inner join ds_sys_repository dsr on dsr.id=dp.repository_id ");
		sql.append("inner join ds_transformation dt on dq.id=dt.squid_id and dt.transformation_type_id=70 ");
		sql.append("inner join ds_transformation_link dtl on dt.id=dtl.to_transformation_id ");
		if(isFromCurrentProject){
			sql.append("where dp.id=" + repositoryId + " ");
		} else {
			sql.append("where dp.repository_id=" + repositoryId + " ");
		}
		if (!ValidateUtils.isEmpty(filterIds)){
			sql.append("and dq.id not in ("+filterIds+")");
		}
		sql.append(" order by dsr.name,dp.id,dsf.id,dq.id");
		List<Map<String,Object>> maps =adapter.query2List(true, sql.toString(), null);
		//获取PLS模型（要把重复的去掉，Train只连一个线的也要去掉）
		sql = new StringBuffer();
		sql.append("select dq.id,concat_ws('/',dsr.name,dp.name,dsf.name,dq.name) as name,");
		sql.append("(select output_data_type from ds_transformation where id=dtl.from_transformation_id) inputdatatype,dq.squid_type_id from ds_squid dq ");
		sql.append("inner join ds_squid_flow dsf on dq.squid_flow_id=dsf.id and squid_type_id in (68) ");
		sql.append("inner join ds_project dp on dsf.project_id=dp.id ");
		sql.append("inner join ds_sys_repository dsr on dsr.id=dp.repository_id ");
		sql.append("inner join ds_transformation dt on dq.id=dt.squid_id and dt.transformation_type_id=70 ");
		sql.append("inner join ds_transformation_link dtl on dt.id=dtl.to_transformation_id ");
		if(isFromCurrentProject){
			sql.append("where dp.id=" + repositoryId + " ");
		} else {
			sql.append("where dp.repository_id=" + repositoryId + " ");
		}
		if (!ValidateUtils.isEmpty(filterIds)){
			sql.append("and dq.id not in ("+filterIds+")");
		}
		sql.append(" order by dsr.name,dp.id,dsf.id,dq.id");
		List<Map<String,Object>> mapList=adapter.query2List(true, sql.toString(), null);
		List<Map<String,Object>> mapArrayList=new ArrayList<>();
		Set hashSets= new HashSet(mapList);
		mapArrayList.addAll(hashSets);
		for (Map<String,Object> objectMap: mapArrayList){
			int i= Collections.frequency(mapList,objectMap);
			if(i>1){
				maps.add(objectMap);
			}
		}
		return maps;
	}
	
	public List<Map<String, Object>> getAllQuantifySquidInRepository(
			boolean isFromCurrentProject,int repositoryId, String filterIds) throws SQLException {
		StringBuffer sql = new StringBuffer();
		sql.append("select dq.id,concat_ws('/',dsr.name,dp.name,dsf.name,dq.name) as name,");
		sql.append("(select output_data_type from ds_transformation where id=dtl.from_transformation_id) inputdatatype from ds_squid dq ");
//		sql.append("inner join ds_dm_squid dmq on dq.id=dmq.id ");
		sql.append("inner join ds_squid_flow dsf on dq.squid_flow_id=dsf.id and squid_type_id = 28 ");
		sql.append("inner join ds_project dp on dsf.project_id=dp.id ");
		sql.append("inner join ds_sys_repository dsr on dsr.id=dp.repository_id ");
		sql.append("inner join ds_transformation dt on dq.id=dt.squid_id and dt.transformation_type_id=70 ");
		sql.append("inner join ds_transformation_link dtl on dt.id=dtl.to_transformation_id ");
		if(isFromCurrentProject){
			sql.append("where dp.id="+repositoryId+" ");
		} else {
			sql.append("where dp.repository_id="+repositoryId+" ");
		}
		if (!ValidateUtils.isEmpty(filterIds)){
			sql.append("and dq.id not in ("+filterIds+")");
		}
		sql.append(" order by dsr.name,dp.id,dsf.id,dq.id");
		return adapter.query2List(true, sql.toString(), null);
	}

	@Override
	public List<Repository> getAllRepository() throws SQLException {
		String sql="select * from DS_SYS_REPOSITORY";
		return adapter.query2List(true,sql, null, Repository.class);
	}

	@SuppressWarnings({ "incomplete-switch", "rawtypes" })
	@Override
	public List<FindResult> getFindResultByParams(FindModel fm) throws Exception {
		List<FindResult> list = new ArrayList<FindResult>();
		DSObjectType objType = DSObjectType.parse(fm.getObjectType());
		DSObjectType itemObjType = DSObjectType.parse(fm.getSourceItem().getType());
		Class c = DSObjectType.getClassForType(objType);
		if (c!=null){
			String tableName="";
			MultitableMapping tm = c.newInstance().getClass().getAnnotation(MultitableMapping.class);
			if(tm!=null&&tm.name()!=null&&tm.name().length>0){
				int i = tm.name().length - 1;
				if (!ValidateUtils.isEmpty(tm.name()[i])){
					tableName = tm.name()[i];
				}
			}
			String sql1 = "select t1.id id0 ,t1.name name0,"+fm.getObjectType()+" type0,";
			if (fm.getObjectType()==DSObjectType.VARIABLE.value()){
				sql1 = "select t1.id id0,t1.variable_name name0,"+fm.getObjectType()+" type0,"
						+ "ds.id id1,ds.name name1,"+DSObjectType.SQUID.value()+" type1,"
						+ "df.id id2,df.name name2,"+DSObjectType.SQUID_FLOW.value()+" type2,"
						+ "dp.id id3,dp.name name3,"+DSObjectType.PROJECT.value()+" type3,"
						+ "dsr.id id4,dsr.name name4, "+DSObjectType.REPOSITORY.value()+" type4 ";
			}
			String sql2 = " from " + tableName + " t1 ";
			String sql = "";
			if (fm.getObjectType()==DSObjectType.VARIABLE.value()){
				sql = getFilteroinForVariable(itemObjType, fm.getSourceItem().getId());
			}else {
				if (objType != DSObjectType.PROJECT
					&& objType != DSObjectType.SQUID_FLOW
					&& objType != DSObjectType.SQUID){
					sql1 += " ds.id id1,ds.name name1,"+DSObjectType.SQUID.value()+" type1,"
							+ "df.id id2,df.name name2,"+DSObjectType.SQUID_FLOW.value()+" type2,"
							+ "dp.id id3,dp.name name3,"+DSObjectType.PROJECT.value()+" type3,"
							+ "dsr.id id4,dsr.name name4, "+DSObjectType.REPOSITORY.value()+" type4 ";
					sql = getFilterForInnerJoin(itemObjType, fm.getSourceItem().getId());
				}else{
					if (objType==DSObjectType.PROJECT
						&& itemObjType==DSObjectType.REPOSITORY){
						sql1 += "dsr.id id1,dsr.name name1,"+DSObjectType.REPOSITORY.value()+" type1 ";
						sql += " inner join DS_SYS_REPOSITORY dsr on dsr.id=t1.repository_id and dsr.id="+fm.getSourceItem().getId();
					}else if (objType==DSObjectType.SQUID_FLOW){
						sql1 += " dp.id id1,dp.name name1,"+DSObjectType.PROJECT.value()+" type1,"
							+ "dsr.id id2,dsr.name name2,"+DSObjectType.REPOSITORY.value()+" type2 ";
						if (itemObjType==DSObjectType.PROJECT){
							sql += " inner join DS_PROJECT dp on dp.id=t1.project_id and dp.id="+fm.getSourceItem().getId();
							sql += " inner join DS_SYS_REPOSITORY dsr on dsr.id=dp.repository_id ";
						}else if (itemObjType==DSObjectType.REPOSITORY){
							sql += " inner join DS_PROJECT dp on dp.id=t1.project_id ";
							sql += " inner join DS_SYS_REPOSITORY dsr on dsr.id=dp.repository_id and dsr.id="+fm.getSourceItem().getId();
						}
					}else if (objType==DSObjectType.SQUID){
						sql1 += " df.id id1,df.name name1,"+DSObjectType.SQUID_FLOW.value()+" type1,"
								+ "dp.id id2,dp.name name2,"+DSObjectType.PROJECT.value()+" type2,"
								+ "dsr.id id3,dsr.name name3,"+DSObjectType.REPOSITORY.value()+" type3 ";
						if (itemObjType==DSObjectType.REPOSITORY){
							sql += " inner join DS_SQUID_FLOW df on df.id=t1.squid_flow_id and t1.squid_type_id != "+SquidTypeEnum.ANNOTATION.value();
							sql += " inner join DS_PROJECT dp on dp.id=df.project_id and dp.repository_id="+fm.getSourceItem().getId();
							sql += " inner join DS_SYS_REPOSITORY dsr on dsr.id=dp.repository_id ";
						}else if (itemObjType==DSObjectType.PROJECT){
							sql += " inner join DS_SQUID_FLOW df on df.id=t1.squid_flow_id and t1.squid_type_id != "+SquidTypeEnum.ANNOTATION.value() +" and df.project_id="+fm.getSourceItem().getId();
							sql += " inner join DS_PROJECT dp on dp.id=df.project_id ";
							sql += " inner join DS_SYS_REPOSITORY dsr on dsr.id=dp.repository_id ";
						}else if (itemObjType==DSObjectType.SQUID_FLOW){
							sql += " inner join DS_SQUID_FLOW df on df.id=t1.squid_flow_id and t1.squid_type_id != "+SquidTypeEnum.ANNOTATION.value() +" and df.id="+fm.getSourceItem().getId();
							sql += " inner join DS_PROJECT dp on dp.id=df.project_id ";
							sql += " inner join DS_SYS_REPOSITORY dsr on dsr.id=dp.repository_id ";
						}
					}
				}
			}
			//拼接where条件
			if (fm.getObjectType()==DSObjectType.VARIABLE.value()){
				sql += getFilterWhereForVariable(fm);
			}else{
				sql += getFilterWhereForOther(fm);
			}
			String strSql = sql1+sql2+sql;
			logger.info(strSql);
			boolean IgnoreCase=fm.isIgnoreCase();
			List<Map<String, Object>> result = adapter.query2List4(true, strSql, null);
			if (result!=null){
				for (Map<String, Object> map : result) {
					list.add(getFilterForList(map,IgnoreCase,fm));
				}
			}
		}
		return list;
	}
	
	/**
	 * 获取过滤的InnerJoin的String
	 * @param itemObjType
	 * @param vid
	 * @return
	 */
	@SuppressWarnings("incomplete-switch")
	private String getFilterForInnerJoin(DSObjectType  itemObjType, int vid){
		String sql = "";
		switch (itemObjType) {
			case REPOSITORY:
				sql += " inner join DS_SQUID ds on ds.id=t1.squid_id ";
				sql += " inner join DS_SQUID_FLOW df on df.id=ds.squid_flow_id ";
				sql += " inner join DS_PROJECT dp on dp.id=df.project_id ";
				sql += " inner join DS_SYS_REPOSITORY dsr on dsr.id=dp.repository_id and dsr.id="+vid;
				break;
			case PROJECT:
				sql += " inner join DS_SQUID ds on ds.id=t1.squid_id ";
				sql += " inner join DS_SQUID_FLOW df on df.id=ds.squid_flow_id ";
				sql += " inner join DS_PROJECT dp on dp.id=df.project_id and dp.id="+vid;
				sql += " inner join DS_SYS_REPOSITORY dsr on dsr.id=dp.repository_id ";
				break;
			case SQUID_FLOW:
				sql += " inner join DS_SQUID ds on ds.id=t1.squid_id and ds.squid_flow_id="+vid;
				sql += " inner join DS_SQUID_FLOW df on df.id=ds.squid_flow_id ";
				sql += " inner join DS_PROJECT dp on dp.id=df.project_id ";
				sql += " inner join DS_SYS_REPOSITORY dsr on dsr.id=dp.repository_id ";
				break;
			case SQUID:
				sql += " inner join DS_SQUID ds on ds.id=t1.squid_id and ds.id="+vid;
				sql += " inner join DS_SQUID_FLOW df on df.id=ds.squid_flow_id ";
				sql += " inner join DS_PROJECT dp on dp.id=df.project_id ";
				sql += " inner join DS_SYS_REPOSITORY dsr on dsr.id=dp.repository_id ";
				break;
		}
		return sql;
	}
	
	/**
	 * 获取过滤的InnerJoin的String
	 * @param itemObjType
	 * @param vid
	 * @return
	 */
	@SuppressWarnings("incomplete-switch")
	private String getFilteroinForVariable(DSObjectType  itemObjType, int vid){
		String sql = "";
		switch (itemObjType) {
			case REPOSITORY:
				sql += " left join DS_SQUID ds on ds.id=t1.squid_id ";
				sql += " left join DS_SQUID_FLOW df on df.id=t1.squid_flow_id ";
				sql += " left join DS_PROJECT dp on dp.id=t1.project_id ";
				sql += " left join DS_SYS_REPOSITORY dsr on dsr.id=dp.repository_id and dsr.id="+vid;
				break;
			case PROJECT:
				sql += " left join DS_SQUID ds on ds.id=t1.squid_id ";
				sql += " left join DS_SQUID_FLOW df on df.id=t1.squid_flow_id ";
				sql += " left join DS_PROJECT dp on dp.id=t1.project_id and dp.id="+vid;
				sql += " left join DS_SYS_REPOSITORY dsr on dsr.id=dp.repository_id ";
				break;
			case SQUID_FLOW:
				sql += " left join DS_SQUID ds on ds.id=t1.squid_id and ds.squid_flow_id="+vid;
				sql += " left join DS_SQUID_FLOW df on df.id=t1.squid_flow_id ";
				sql += " left join DS_PROJECT dp on dp.id=t1.project_id ";
				sql += " left join DS_SYS_REPOSITORY dsr on dsr.id=dp.repository_id ";
				break;
			case SQUID:
				sql += " left join DS_SQUID ds on ds.id=t1.squid_id and ds.id="+vid;
				sql += " left join DS_SQUID_FLOW df on df.id=t1.squid_flow_id ";
				sql += " left join DS_PROJECT dp on dp.id=t1.project_id ";
				sql += " left join DS_SYS_REPOSITORY dsr on dsr.id=dp.repository_id ";
				break;
		}
		return sql;
	}
	
	/**
	 * 拼接除变量外的其他类型的where条件
	 * @param fm
	 * @return
	 */
	private String getFilterWhereForOther(FindModel fm){
		//是否忽略大小写
		String sql = " where 1=1 ";
		//转意通配符 下划线
		String condition = fm.getCondition();
		if (condition.contains("_")&&!fm.isWholeWord()){
			condition = condition.replaceAll("_", "|_");
		}
		//单引号转换成俩个单引号
		if (condition.contains("'")&&!fm.isWholeWord()){
			condition = condition.replaceAll("'", "''");
		}

		if (fm.isIgnoreCase()){
			//是否全字匹配
			if (fm.isWholeWord()){
				sql += " and upper(t1.name) = '"+condition.toUpperCase()+"'";
			}else{
				sql += " and upper(t1.name) like '%"+condition.toUpperCase()+"%'";
			}
		}else{
			if (fm.isWholeWord()){
				sql += " and t1.name = '"+condition+"'";
			}else{
				sql += " and t1.name like '%"+condition+"%'";
			}
		}
		if (condition.contains("_")&&!fm.isWholeWord()){
			sql += " escape '|'";
		}
		sql += " order by t1.id";
		return sql;
	}
	
	/**
	 * 拼接变量的where条件
	 * @param fm
	 * @return
	 */
	private String getFilterWhereForVariable(FindModel fm){
		//是否忽略大小写
		String sql = " where 1=1 ";
		//转意通配符 下划线
		String condition = fm.getCondition();
		if (condition.contains("_")&&!fm.isWholeWord()){
			condition = condition.replaceAll("_", "|_");
		}
		//单引号转换成俩个单引号
		if (condition.contains("'")&&!fm.isWholeWord()){
			condition = condition.replaceAll("'", "''");
		}
		if (fm.isIgnoreCase()){
			//是否全字匹配
			if (fm.isWholeWord()){
				sql += " and upper(t1.variable_name) = '"+condition.toUpperCase()+"'";
			}else{
				sql += " and upper(t1.variable_name) like '%"+condition.toUpperCase()+"%'";
			}
		}else{
			if (fm.isWholeWord()){
				sql += " and t1.variable_name = '"+condition+"'";
			}else{
				sql += " and t1.variable_name like '%"+condition+"%'";
			}
		}
		if (condition.contains("_")&&!fm.isWholeWord()){
			sql += " escape '|'";
		}
		switch (DSObjectType.parse(fm.getSourceItem().getType())) {
			case REPOSITORY:
				sql += " and dsr.id="+fm.getSourceItem().getId();
				break;
			case PROJECT:
				sql += " and dp.id="+fm.getSourceItem().getId();
				break;
			case SQUID_FLOW:
				sql += " and df.id="+fm.getSourceItem().getId();
				break;
			case SQUID:
				sql += " and ds.id="+fm.getSourceItem().getId();
				break;
		}
		sql += " order by t1.id";
		return sql;
	}
	
	// 返回拼接的对象集合
	public FindResult getFilterForList(Map<String, Object> map,boolean ignoreCase,FindModel fm){
		FindResult rs = new FindResult();
		List<PathItem> list = new ArrayList<PathItem>();
		if (map!=null&&map.keySet().size()>0){
			int cnt = map.keySet().size() / 3;
			int i = 0;
			if(ignoreCase){
				while (i<cnt) {
					PathItem item = new PathItem();
					if (StringUtils.isNotNull(map.get("id"+i))
							&&StringUtils.isNotNull(map.get("name"+i))
							&&StringUtils.isNotNull(map.get("type"+i))){
						item.setId(Integer.parseInt(map.get("id"+i)+""));
						item.setName(map.get("name"+i)+"");
						int type = Integer.parseInt(map.get("type"+i)+"");
						item.setType(type);
						list.add(item);
					}
					i++;
				}
			}else {
				/* 这里逻辑比较混乱 必须先根据第一个name判断是否符合条件，然后再把属于该内容的上级全部增加到集合内 */
				PathItem item = new PathItem();
				if (StringUtils.isNotNull(map.get("id"+i))
						&&StringUtils.isNotNull(map.get("name"+i))
						&&StringUtils.isNotNull(map.get("type"+i))) {
					if (map.get("name"+i).toString().contains(fm.getCondition()) && map.get("type" + i).toString().equals(fm.getObjectType() + "")){
						item.setId(Integer.parseInt(map.get("id"+i)+""));
						item.setName(map.get("name"+i)+"");
						item.setType(Integer.parseInt(map.get("type"+i)+""));
						list.add(item);
						/* 当第一个内容符合条件后，剩余的上级节点直接加到集合里面
						主要是因为mysql现在的配置是大小写不敏感，导致无法在SQL内直接进行大小写判断*/
						i++; /*第一级节点增加完毕后索引发生变化*/
						while (i<cnt) {
							if (StringUtils.isNotNull(map.get("id"+i))
									&&StringUtils.isNotNull(map.get("name"+i))
									&&StringUtils.isNotNull(map.get("type"+i))) {
								item = new PathItem(); /*重新创建对象*/
								item.setId(Integer.parseInt(map.get("id"+i)+""));
								item.setName(map.get("name"+i)+"");
								item.setType(Integer.parseInt(map.get("type"+i)+""));
								list.add(item);
							}
							i++; /* 剩余级别的节点增加完毕后在这里更改索引 */
						}

					}
				}

			}
			rs.setPathItems(list);
		}
		return rs;
	}

	@Override
	public void saveLicenseKey(String info) throws Exception {
		String sql = "update DS_SYS_SERVER_PARAMETER set value=? where name='LicenseKey'";
		List<Object> params = new ArrayList<Object>();
		params.add(info);
		adapter.execute(sql, params);
	}
	
	@Override
	public String getLicenseKey() throws Exception{
		String value = "";
		String sql = "select value from DS_SYS_SERVER_PARAMETER where name='LicenseKey'";
		Map<String, Object> map = adapter.query2Object(true, sql, null);
		if (map!=null&&map.containsKey("VALUE")){
			value = String.valueOf(map.get("VALUE"));
		}
		return value;
	}
}
