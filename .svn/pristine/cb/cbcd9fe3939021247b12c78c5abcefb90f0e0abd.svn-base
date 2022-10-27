package com.eurlanda.datashire.dao.impl;

import cn.com.jsoft.jframe.utils.ValidateUtils;
import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.annotation.MultitableMapping;
import com.eurlanda.datashire.dao.IVariableDao;
import com.eurlanda.datashire.entity.DSVariable;
import com.eurlanda.datashire.entity.FindResult;
import com.eurlanda.datashire.entity.PathItem;
import com.eurlanda.datashire.entity.Squid;
import com.eurlanda.datashire.entity.SquidJoin;
import com.eurlanda.datashire.entity.ThirdPartyParams;
import com.eurlanda.datashire.entity.Transformation;
import com.eurlanda.datashire.entity.TransformationInputs;
import com.eurlanda.datashire.enumeration.DSObjectType;
import com.eurlanda.datashire.utility.JsonUtil;
import com.eurlanda.datashire.utility.StringUtils;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class VariableDaoImpl extends BaseDaoImpl implements IVariableDao {

	public VariableDaoImpl(){
	}
	
	public VariableDaoImpl(IRelationalDataManager adapter){
		this.adapter = adapter;
	}
	
	@Override
	public boolean getExistsNameByObject(DSVariable variable) throws SQLException {
		String sql = "select * from ds_variable where 1=1 and variable_name=?";
		if (variable.getVariable_scope()==0){
			sql += " and project_id="+variable.getProject_id();
		}else if (variable.getVariable_scope()==1){
			sql += " and (squid_flow_id="+variable.getSquid_flow_id()
					+ " or (project_id="+variable.getProject_id()+" and variable_scope=0))";
		}else{
			sql += " and (squid_id="+variable.getSquid_id()
					+ " or (squid_flow_id="+variable.getSquid_flow_id()+" and variable_scope=1)"
					+ " or (project_id="+variable.getProject_id()+" and variable_scope=0))";
		}
		List<Object> params = new ArrayList<Object>();
		params.add(variable.getVariable_name());
		List<Map<String, Object>> list = adapter.query2List(true, sql, params);
		if (list!=null&&list.size()>0){
			return true;
		}
		return false;
	}
	
	@Override
	public boolean getExistsNameByObject(DSVariable variable, int type) throws SQLException {
		String sql = "select * from ds_variable where 1=1 and variable_name=? and id!=?";
		if (variable.getVariable_scope()==0){
			sql += " and project_id="+variable.getProject_id();
		}else if (variable.getVariable_scope()==1){
			sql += " and (squid_flow_id="+variable.getSquid_flow_id()
					+ " or (project_id="+variable.getProject_id()+" and variable_scope=0))";
		}else{
			sql += " and (squid_id="+variable.getSquid_id()
					+ " or (squid_flow_id="+variable.getSquid_flow_id()+" and variable_scope=1)"
					+ " or (project_id="+variable.getProject_id()+" and variable_scope=0))";
		}
		List<Object> params = new ArrayList<Object>();
		/*params.add(variable.getProject_id());
		params.add(variable.getSquid_flow_id());
		params.add(variable.getSquid_id());*/
		params.add(variable.getVariable_name());
		params.add(variable.getId());
		List<Map<String, Object>> list = adapter.query2List(true, sql, params);
		if (list!=null&&list.size()>0){
			return true;
		}
		return false;
	}
	
	@Override
	public String getExistsNameForPath(DSVariable variable) throws SQLException{
		String path = "";
		String sql = "select t1.id id0,t1.variable_name name0,"+DSObjectType.VARIABLE.value()+" type0,"
				+ "ds.id id1,ds.name name1,"+DSObjectType.SQUID.value()+" type1,"
				+ "df.id id2,df.name name2,"+DSObjectType.SQUID_FLOW.value()+" type2,"
				+ "dp.id id3,dp.name name3,"+DSObjectType.PROJECT.value()+" type3,"
				+ "dsr.id id4,dsr.name name4, "+DSObjectType.REPOSITORY.value()+" type4 "
				+ "from ds_variable t1 "
				+ " left join ds_project dp on t1.project_id=dp.id  "
				+ " left join ds_squid_flow df on t1.squid_flow_id=df.id  "
				+ " left join ds_squid ds on t1.squid_id=ds.id "
				+ " left join DS_SYS_REPOSITORY dsr on dsr.id=dp.repository_id   "
				+ " where t1.variable_name='"+variable.getVariable_name()+"' ";
		if (variable.getVariable_scope()==0){
			sql += " and t1.project_id="+variable.getProject_id();
		}else if (variable.getVariable_scope()==1){
			sql += " and (t1.squid_flow_id="+variable.getSquid_flow_id()
					+ " or (t1.project_id="+variable.getProject_id()+" and variable_scope=0))";
		}else{
			sql += " and (t1.squid_id="+variable.getSquid_id()
					+ " or (t1.squid_flow_id="+variable.getSquid_flow_id()+" and variable_scope=1)"
					+ " or (t1.project_id="+variable.getProject_id()+" and variable_scope=0))";
		}
		Map<String, Object> map = adapter.query2Object4(true, sql, null);
		if (map!=null&&map.keySet().size()>0){
			int cnt = map.keySet().size() / 3;
			int i = 0;
			while (i<cnt) {
				if (StringUtils.isNotNull(map.get("NAME"+i))){
					path = "/" + map.get("NAME"+i) + path;
				}
				i++;
			}
			if (path!=""){
				path = path.substring(1);
			}
		}
		return path;
	}

	
	/**
	 * 根据作用域查询变量集合
	 * @param scope
	 * @return
	 * @throws SQLException
	 */
	public List<DSVariable> getDSVariableByScope(int scope, int id) throws SQLException {
		Map<String, Object> params = new HashMap<String, Object>();
		params.put("variable_scope", scope);
		if (scope==0){
			params.put("project_id", id);
		}else if (scope==1){
			params.put("squid_flow_id", id);
		}else if (scope==2){
			params.put("squid_id", id);
		}
		List<DSVariable> list = adapter.query2List2(true, params, DSVariable.class);
		return list;
	}
	
	/**
	 * 根据作用域查询变量集合
	 * @param scope
	 * @return
	 * @throws SQLException
	 */
	public List<DSVariable> getDSVariableByScope(int scope, int id, String ids) throws SQLException {
		String sql = "select * from ds_variable where variable_scope="+scope;
		if (scope==0){
			sql += " and project_id="+id;
		}else if (scope==1){
			sql += " and squid_flow_id="+id;
		}else if (scope==2){
			sql += " and squid_id="+id;
		}
		if (ids!=""){
			sql += " and squid_id in ("+ids+")";
		}
		List<DSVariable> list = adapter.query2List(true, sql, null, DSVariable.class);
		return list;
	}

	/**
	 * 检索变量引用
	 * @throws Exception 
	 */
	@Override
	public List<FindResult> findVariableById(int variableId) throws Exception{
		List<FindResult> list = new ArrayList<FindResult>();
		Map<String, Object> paramsMap = new HashMap<String, Object>();
		paramsMap.put("id", variableId);
		DSVariable variable = adapter.query2Object(true, paramsMap, DSVariable.class);
		if (variable==null){
			return null;
		}
		
		List<String> squids = new ArrayList<String>();
		//squid的检索字段
		List<String> filterLists = new ArrayList<String>();
		filterLists.add("id");
		filterLists.add("filter");
		List<Map<String, Object>> squidList = getObjectForMap(Squid.class, filterLists, variable);
		if (squidList!=null&&squidList.size()>0){
			for (Map<String, Object> map : squidList) {
				List<String> ids = this.filterListForVariable(variable.getVariable_name(), map, filterLists);
				if (StringUtils.isNotNull(ids)) squids.addAll(ids);
			}
		}
		list.addAll(this.getFindResultForType(DSObjectType.SQUID, squids));
		
		List<String> trans = new ArrayList<String>();
		//trans的检索字段
		filterLists.clear();
		filterLists.add("id");
		filterLists.add("description");
		filterLists.add("constant_value");
		filterLists.add("tran_condition");
		
		List<Map<String, Object>> transList = this.getObjectForMap(Transformation.class, filterLists, variable);
		if (transList!=null&&transList.size()>0){
			for (Map<String, Object> map : transList) {
				List<String> ids = this.filterListForVariable(variable.getVariable_name(), map, filterLists);
				if (StringUtils.isNotNull(ids)) trans.addAll(ids);
			}
		}
		list.addAll(this.getFindResultForType(DSObjectType.TRANSFORMATION, trans));
		
		List<String> transInputs = new ArrayList<String>();
		//transInput的检索字段
		filterLists.clear();
		filterLists.add("id");
		filterLists.add("in_condition");
		List<Map<String, Object>> transInputList = this.getObjectForMap(TransformationInputs.class, filterLists, variable);
		if (transInputList!=null&&transInputList.size()>0){
			for (Map<String, Object> map : transInputList) {
				List<String> ids = this.filterListForVariable(variable.getVariable_name(), map, filterLists);
				if (StringUtils.isNotNull(ids)) transInputs.addAll(ids);
			}
		}
		list.addAll(this.getFindResultForType(DSObjectType.TRANSFORMATIONINPUTS, transInputs));
		
		List<String> joins = new ArrayList<String>();
		//join的检索字段
		filterLists.clear();
		filterLists.add("id");
		filterLists.add("join_condition");
		List<Map<String, Object>> joinList = this.getObjectForMap(SquidJoin.class, filterLists, variable);
		if (joinList!=null&&joinList.size()>0){
			for (Map<String, Object> map : joinList) {
				List<String> ids = this.filterListForVariable(variable.getVariable_name(), map, filterLists);
				if (StringUtils.isNotNull(ids)) joins.addAll(ids);
			}
		}
		list.addAll(this.getFindResultForType(DSObjectType.SQUIDJOIN, joins));
		
		List<String> params = new ArrayList<String>();
		//第三方参数的值类型
		filterLists.clear();
		filterLists.add("id");
		filterLists.add("val");
		List<Map<String, Object>> valList = this.getObjectForMap(ThirdPartyParams.class, filterLists, variable);
		if (valList!=null&&valList.size()>0){
			for (Map<String, Object> map : valList) {
				List<String> ids = this.filterListForVariable(variable.getVariable_name(), map, filterLists);
				if (StringUtils.isNotNull(ids)) params.addAll(ids);
			}
		}
		list.addAll(this.getFindResultForType(DSObjectType.THIRDPARTYPARAMS, params));
		System.out.println(JsonUtil.toGsonString(list));
		return list;
	}

	@Override
	public DSVariable copyVariableById(DSVariable oldVariable, int newSquidId,
			int newSquidFlowId, int newProjectId) throws SQLException {
		if (newSquidId>0){
			oldVariable.setSquid_id(newSquidId);
		}
		if (newSquidFlowId>0){
			oldVariable.setSquid_flow_id(newSquidFlowId);
		}
		if (newProjectId>0){
			oldVariable.setProject_id(newProjectId);
		}
		oldVariable.setId(0);
		oldVariable.setAdd_date(new Timestamp(new Date().getTime()).toString());
		resetVariableName(oldVariable);
		int newId = adapter.insert2(oldVariable);
		if (newId>0){
			oldVariable.setId(newId);
		}else{
			return null;
		}
		return oldVariable;
	}
	
	/**
	 * 去除重复的变量名
	 * @param variable
	 * @throws SQLException
	 */
	@Override
	public void resetVariableName(DSVariable variable) throws SQLException{
		String tempName = variable.getVariable_name();
		String variableName = variable.getVariable_name();
		int cnt = 1;
		while (true) {
			variable.setVariable_name(tempName);
			if (getExistsNameByObject(variable)){
				tempName = variableName + "_copy_" + cnt;
			}else{
				return;
			}
			cnt++;
		}
	}
	
	private List<String> filterListForVariable(String variableName, 
			Map<String, Object> map, List<String> strs){
		List<String> list = new ArrayList<String>();
		if (strs!=null&&strs.size()>0){
			for (String string : strs) {
				if (map.containsKey(string.toUpperCase())){
					String value = String.valueOf(map.get(string.toUpperCase()));
    				if (StringUtils.isNotNull((value))){
    					String val = toSingleQuotes(value);
    					boolean flag = isExistsForVariable(variableName, val);
    					if(flag){
    						list.add(map.get("ID")+"&"+string.toUpperCase());
    					}
    				}
				}
			}
		}
		return list;
	}
	
	private List<FindResult> getFindResultForType(DSObjectType type, List<String> list){
		List<FindResult> rsList = new ArrayList<FindResult>();
		if (list!=null&&list.size()>0){
			for (String key : list) {
				if (StringUtils.isNull(key)||!key.contains("&")){
					continue;
				}
				int id = Integer.parseInt(key.split("&")[0]);
				String name = key.split("&")[1];
				try {
					String sql = this.createVariableSqlStrByType(type, name, id);
					Map<String, Object> map = this.adapter.query2Object4(true, sql, null);
					FindResult rs  = this.getFilterForList(map);
					rsList.add(rs);
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		return rsList;
	}
	
	private String createVariableSqlStrByType(DSObjectType objType, String name, int id) throws Exception{
		Class c = DSObjectType.getClassForType(objType);
		String tableName="";
		MultitableMapping tm = c.newInstance().getClass().getAnnotation(MultitableMapping.class);
		if(tm!=null&&tm.name()!=null&&tm.name().length>0){
			int i = tm.name().length - 1;
			if (!ValidateUtils.isEmpty(tm.name()[i])){
				tableName = tm.name()[i];
			}
		}
		String sql = "";
		switch (objType) {
		case SQUID:
			sql += "select 0 id0,'"+name+"' name0,"+objType.value()+" type0,"
					+ "t1.id id1,t1.name name1,"+DSObjectType.SQUID.value()+" type1,"
					+ "df.id id2,df.name name2,"+DSObjectType.SQUID_FLOW.value()+" type2,"
					+ "dp.id id3,dp.name name3,"+DSObjectType.PROJECT.value()+" type3,"
					+ "dsr.id id4,dsr.name name4, "+DSObjectType.REPOSITORY.value()+" type4 ";
			sql += " from " + tableName + " t1 ";
			sql += " inner join DS_SQUID_FLOW df on df.id=t1.squid_flow_id ";
			sql += " inner join DS_PROJECT dp on dp.id=df.project_id ";
			sql += " inner join DS_SYS_REPOSITORY dsr on dsr.id=dp.repository_id ";
			sql += " where t1.id="+id;
			break;
		case SQUIDJOIN:
			sql += "select t1.id id0,'"+name+"' name0,"+objType.value()+" type0,"
					+ "ds.id id1,ds.name name1,"+DSObjectType.SQUID.value()+" type1,"
					+ "df.id id2,df.name name2,"+DSObjectType.SQUID_FLOW.value()+" type2,"
					+ "dp.id id3,dp.name name3,"+DSObjectType.PROJECT.value()+" type3,"
					+ "dsr.id id4,dsr.name name4, "+DSObjectType.REPOSITORY.value()+" type4 ";
			sql += " from " + tableName + " t1 ";
			sql += " inner join DS_SQUID ds on ds.id=t1.joined_squid_id ";
			sql += " inner join DS_SQUID_FLOW df on df.id=ds.squid_flow_id ";
			sql += " inner join DS_PROJECT dp on dp.id=df.project_id ";
			sql += " inner join DS_SYS_REPOSITORY dsr on dsr.id=dp.repository_id ";
			sql += " where t1.id="+id;
			break;
		case TRANSFORMATION:
			sql += "select 0 id0,'"+name+"' name0,"+objType.value()+" type0,"
					+ "dt.id id1,dt.name name1,"+DSObjectType.TRANSFORMATION.value()+" type1,"
					+ "ds.id id2,ds.name name2,"+DSObjectType.SQUID.value()+" type2,"
					+ "df.id id3,df.name name3,"+DSObjectType.SQUID_FLOW.value()+" type3,"
					+ "dp.id id4,dp.name name4,"+DSObjectType.PROJECT.value()+" type4,"
					+ "dsr.id id5,dsr.name name5, "+DSObjectType.REPOSITORY.value()+" type5 ";
			sql += " from  DS_TRANSFORMATION dt ";
			sql += " inner join DS_SQUID ds on ds.id=dt.squid_id ";
			sql += " inner join DS_SQUID_FLOW df on df.id=ds.squid_flow_id ";
			sql += " inner join DS_PROJECT dp on dp.id=df.project_id ";
			sql += " inner join DS_SYS_REPOSITORY dsr on dsr.id=dp.repository_id ";
			sql += " where dt.id="+id;
			break;
		case TRANSFORMATIONINPUTS:
			sql += "select t1.id id0,'"+name+"' name0,"+objType.value()+" type0,"
					+ "dt.id id1,dt.name name1,"+DSObjectType.TRANSFORMATION.value()+" type1,"
					+ "ds.id id2,ds.name name2,"+DSObjectType.SQUID.value()+" type2,"
					+ "df.id id3,df.name name3,"+DSObjectType.SQUID_FLOW.value()+" type3,"
					+ "dp.id id4,dp.name name4,"+DSObjectType.PROJECT.value()+" type4,"
					+ "dsr.id id5,dsr.name name5, "+DSObjectType.REPOSITORY.value()+" type5 ";
			sql += " from " + tableName + " t1 ";
			sql += " inner join DS_TRANSFORMATION dt on dt.id=t1.transformation_id ";
			sql += " inner join DS_SQUID ds on ds.id=dt.squid_id ";
			sql += " inner join DS_SQUID_FLOW df on df.id=ds.squid_flow_id ";
			sql += " inner join DS_PROJECT dp on dp.id=df.project_id ";
			sql += " inner join DS_SYS_REPOSITORY dsr on dsr.id=dp.repository_id ";
			sql += " where t1.id="+id;
			break;
		case THIRDPARTYPARAMS:
			sql += "select t1.id id0,'"+name+"' name0,"+objType.value()+" type0,"
					+ "ds.id id1,ds.name name1,"+DSObjectType.SQUID.value()+" type1,"
					+ "df.id id2,df.name name2,"+DSObjectType.SQUID_FLOW.value()+" type2,"
					+ "dp.id id3,dp.name name3,"+DSObjectType.PROJECT.value()+" type3,"
					+ "dsr.id id4,dsr.name name4, "+DSObjectType.REPOSITORY.value()+" type4 ";
			sql += " from " + tableName + " t1 ";
			sql += " inner join DS_SQUID ds on ds.id=t1.squid_id ";
			sql += " inner join DS_SQUID_FLOW df on df.id=ds.squid_flow_id ";
			sql += " inner join DS_PROJECT dp on dp.id=df.project_id ";
			sql += " inner join DS_SYS_REPOSITORY dsr on dsr.id=dp.repository_id ";
			sql += " where t1.id="+id;
		default:
			break;
		}
		return sql;
	}
	
	private static String toSingleQuotes(String str){
		Pattern pattern = Pattern.compile("\\'[^\\']*\\'");
		//Pattern pattern = Pattern.compile("\\'[\\s\\S]*\\'");
		Matcher m = pattern.matcher(" "+str.toLowerCase()+" ");
		StringBuffer sb = new StringBuffer();
		//使用find()方法查找第一个匹配的对象 
		boolean result = m.find();
		while (result) {
			m.appendReplacement(sb, "_");
			//System.out.println("匹配后sb的内容是："+m.group());
			result = m.find();
		}
		m.appendTail(sb);
		//System.out.println(sb.toString());
		return sb.toString();
	}
	
	private static boolean isExistsForVariable(String key, String str){
		String  tt = "@"+key.toLowerCase();
		if (tt.equals(str)){
			return true;
		}
		Pattern pattern = Pattern.compile("\\W@"+key.toLowerCase()+"\\W");
		Matcher m = pattern.matcher(str);
		//使用find()方法查找第一个匹配的对象 
		boolean result = m.find();
		while (result) {
			String s1 = m.group();
			if (!s1.contains("@@")){
				return true;
			}
		}
		return false;
	}
	
	// 返回拼接的对象集合
	private static FindResult getFilterForList(Map<String, Object> map){
		FindResult rs = new FindResult();
		List<PathItem> list = new ArrayList<PathItem>();
		if (map!=null&&map.keySet().size()>0){
			int cnt = map.keySet().size() / 3;
			int i = 0;
			while (i<cnt) {
				PathItem item = new PathItem();
				//map 中的字段取出来时区分大小写
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
			rs.setPathItems(list);
		}
		return rs;
	}
	
	public <T> List<Map<String, Object>> getObjectForMap(Class<T> c, 
			List<String> filters, DSVariable ds) throws Exception {
		String tableName="";
		MultitableMapping tm = c.newInstance().getClass().getAnnotation(MultitableMapping.class);
		if(tm!=null&&tm.name()!=null&&tm.name().length>0){
			int i = tm.name().length - 1;
			if (!ValidateUtils.isEmpty(tm.name()[i])){
				tableName = tm.name()[i];
			}
		}
		String sql  = "select  ";
		if (filters!=null&&filters.size()>0){
			String temp = "";
			for (String string : filters) {
				temp += ",t1." + string;
			}
			if (temp!=""){
				temp = temp.substring(1);
			}
			sql += temp + " ";
		}else{
			sql += " t1.* ";
		}
		sql += " from "+tableName+" t1 ";
		if (c==Squid.class){
			sql += " inner join DS_SQUID_FLOW df on df.id=t1.squid_flow_id ";
			sql += " inner join DS_PROJECT dp on dp.id=df.project_id ";
			sql += " inner join DS_SYS_REPOSITORY dsr on dsr.id=dp.repository_id ";
		}else if(c==Transformation.class){
			sql += " inner join DS_SQUID ds on ds.id=t1.squid_id ";
			sql += " inner join DS_SQUID_FLOW df on df.id=ds.squid_flow_id ";
			sql += " inner join DS_PROJECT dp on dp.id=df.project_id ";
			sql += " inner join DS_SYS_REPOSITORY dsr on dsr.id=dp.repository_id ";
		}else if (c==TransformationInputs.class){
			sql += " inner join DS_TRANSFORMATION dt on dt.id=t1.transformation_id ";
			sql += " inner join DS_SQUID ds on ds.id=dt.squid_id ";
			sql += " inner join DS_SQUID_FLOW df on df.id=ds.squid_flow_id ";
			sql += " inner join DS_PROJECT dp on dp.id=df.project_id ";
			sql += " inner join DS_SYS_REPOSITORY dsr on dsr.id=dp.repository_id ";
		}else if (c==SquidJoin.class){
			sql += " inner join DS_SQUID ds on ds.id=t1.joined_squid_id ";
			sql += " inner join DS_SQUID_FLOW df on df.id=ds.squid_flow_id ";
			sql += " inner join DS_PROJECT dp on dp.id=df.project_id ";
			sql += " inner join DS_SYS_REPOSITORY dsr on dsr.id=dp.repository_id ";
		}else if (c==ThirdPartyParams.class){
			sql += " inner join DS_SQUID ds on ds.id=t1.squid_id ";
			sql += " inner join DS_SQUID_FLOW df on df.id=ds.squid_flow_id ";
			sql += " inner join DS_PROJECT dp on dp.id=df.project_id ";
			sql += " inner join DS_SYS_REPOSITORY dsr on dsr.id=dp.repository_id ";
		}
		sql += " where 1=1 ";
		switch (ds.getVariable_scope()) {
		case 0:
			sql += " and dp.id="+ds.getProject_id();
			break;
		case 1:
			sql += " and df.id="+ds.getSquid_flow_id();
			break;
		case 2:
			if (c==Squid.class){
				sql += " and t1.id="+ds.getSquid_id();
			}else{
				sql += " and ds.id="+ds.getSquid_id();
			}
			break;
		}
		return adapter.query2List(true, sql, null);
	}

	/**
	 * 查询是否被引用
	 */
	public boolean findVariableExistsById(int variableId) throws Exception{
		Map<String, Object> paramsMap = new HashMap<String, Object>();
		paramsMap.put("id", variableId);
		DSVariable variable = adapter.query2Object(true, paramsMap, DSVariable.class);
		if (variable==null){
			return false;
		}
		//squid的检索字段
		List<String> filterLists = new ArrayList<String>();
		filterLists.add("id");
		filterLists.add("filter");
		List<Map<String, Object>> squidList = getObjectForMap(Squid.class, filterLists, variable);
		if (squidList!=null&&squidList.size()>0){
			for (Map<String, Object> map : squidList) {
				List<String> ids = this.filterListForVariable(variable.getVariable_name(), map, filterLists);
				if (StringUtils.isNotNull(ids)&&ids.size()>0) return true;
			}
		}
		//trans的检索字段
//		filterLists.clear();
//		filterLists.add("id");
//		filterLists.add("reg_expression");
//		filterLists.add("term_index");
//		filterLists.add("delimiter");
//		filterLists.add("replica_count");
//		filterLists.add("length");
//		filterLists.add("power");
//		filterLists.add("modulus");
//		filterLists.add("description");
//		filterLists.add("constant_value");
//		filterLists.add("tran_condition");
//		filterLists.add("start_position");
		filterLists.clear();
		filterLists.add("id");
		filterLists.add("description");
		filterLists.add("constant_value");
		filterLists.add("tran_condition");
		List<Map<String, Object>> transList = this.getObjectForMap(Transformation.class, filterLists, variable);
		if (transList!=null&&transList.size()>0){
			for (Map<String, Object> map : transList) {
				List<String> ids =  this.filterListForVariable(variable.getVariable_name(), map, filterLists);
				if (StringUtils.isNotNull(ids)&&ids.size()>0) return true;
			}
			
		}
		//transInput的检索字段
		filterLists.clear();
		filterLists.add("id");
		filterLists.add("in_condition");
		List<Map<String, Object>> transInputList = this.getObjectForMap(TransformationInputs.class, filterLists, variable);
		if (transInputList!=null&&transInputList.size()>0){
			for (Map<String, Object> map : transInputList) {
				List<String> ids = this.filterListForVariable(variable.getVariable_name(), map, filterLists);
				if (StringUtils.isNotNull(ids)&&ids.size()>0) return true;
			}
		}
		//join的检索字段
		filterLists.clear();
		filterLists.add("id");
		filterLists.add("join_condition");
		List<Map<String, Object>> joinList = this.getObjectForMap(SquidJoin.class, filterLists, variable);
		if (joinList!=null&&joinList.size()>0){
			for (Map<String, Object> map : joinList) {
				List<String> ids = this.filterListForVariable(variable.getVariable_name(), map, filterLists);
				if (StringUtils.isNotNull(ids)&&ids.size()>0) return true;
			}
		}
		
		//第三方参数的值类型
		filterLists.clear();
		filterLists.add("id");
		filterLists.add("val");
		List<Map<String, Object>> valList = this.getObjectForMap(ThirdPartyParams.class, filterLists, variable);
		if (valList!=null&&valList.size()>0){
			for (Map<String, Object> map : valList) {
				List<String> ids = this.filterListForVariable(variable.getVariable_name(), map, filterLists);
				if (StringUtils.isNotNull(ids)&&ids.size()>0) return true;
			}
		}
		return false;
	}
}
