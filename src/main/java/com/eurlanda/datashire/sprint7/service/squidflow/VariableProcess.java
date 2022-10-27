package com.eurlanda.datashire.sprint7.service.squidflow;

import com.eurlanda.datashire.adapter.DataAdapterFactory;
import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.dao.IVariableDao;
import com.eurlanda.datashire.dao.impl.VariableDaoImpl;
import com.eurlanda.datashire.entity.DSVariable;
import com.eurlanda.datashire.entity.FindResult;
import com.eurlanda.datashire.utility.JsonUtil;
import com.eurlanda.datashire.utility.MessageCode;
import com.eurlanda.datashire.utility.ReturnValue;
import com.eurlanda.datashire.utility.StringUtils;
import org.apache.log4j.Logger;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 变量业务处理层
 * @author yi.zhou
 * @date 2014/12/10
 */
public class VariableProcess {
	
	private static Logger logger = Logger.getLogger(VariableProcess.class);// 记录日志
	
	/**
	 * 创建变量
	 * @param info
	 * @param out
	 * @return
	 */
	public Map<String, Object> createVariable(String info, ReturnValue out){
		Map<String, Object> outputMap = new HashMap<String, Object>();
		IRelationalDataManager adapter = null;
		try {
			Map<String, Object> map = JsonUtil.toHashMap(info);
			DSVariable variable = JsonUtil.toGsonBean(map.get("Variable")+"", DSVariable.class);
			if (variable!=null){
				adapter = DataAdapterFactory.getDefaultDataManager();
				adapter.openSession();
				IVariableDao variableDao = new VariableDaoImpl(adapter);
				boolean flag = variableDao.getExistsNameByObject(variable);
				if (flag){
					out.setMessageCode(MessageCode.ERR_VARIABLE_EXISTS);
				}else{
					variable.setAdd_date(new Timestamp(new Date().getTime()).toString());
					int newId = variableDao.insert2(variable);
					outputMap.put("NewVariableId", newId);
					return outputMap;
				}
			}else{
				out.setMessageCode(MessageCode.NODATA);
			}
		} catch (Exception e) {
			if (adapter!=null){
				try {
					adapter.rollback();
				} catch (SQLException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			}
			out.setMessageCode(MessageCode.SQL_ERROR);
			logger.error("createVariabel 创建失败", e);
		} finally{
			if (adapter!=null) adapter.closeSession();
		}
		return outputMap;
	}
	
	/**
	 * 修改变量
	 * @param info
	 * @param out
	 * @return
	 */
	public Map<String, Object> updateVariable(String info, ReturnValue out){
		Map<String, Object> outputMap = new HashMap<String, Object>();
		IRelationalDataManager adapter = null;
		try {
			Map<String, Object> map = JsonUtil.toHashMap(info);
			DSVariable variable = JsonUtil.toGsonBean(map.get("Variable")+"", DSVariable.class);
			if (variable!=null){
				adapter = DataAdapterFactory.getDefaultDataManager();
				adapter.openSession();
				IVariableDao variableDao = new VariableDaoImpl(adapter);
				boolean flag = variableDao.getExistsNameByObject(variable, 1);
				if (flag){
					out.setMessageCode(MessageCode.ERR_VARIABLE_EXISTS);
				}else{
					variable.setAdd_date(new Timestamp(new Date().getTime()).toString());
					boolean rt = variableDao.update(variable);
					if (rt){
						return outputMap;
					}else{
						out.setMessageCode(MessageCode.UPDATE_ERROR);
					}
				}
			}else{
				out.setMessageCode(MessageCode.NODATA);
			}
		} catch (Exception e) {
			if (adapter!=null){
				try {
					adapter.rollback();
				} catch (SQLException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			}
			out.setMessageCode(MessageCode.SQL_ERROR);
			logger.error("updateVariable 出现错误", e);
		} finally{
			if (adapter!=null) adapter.closeSession();
		}
		return outputMap;
	}
	
	/**
	 * 删除变量
	 * @param info
	 * @param out
	 * @return
	 */
	public Map<String, Object> deleteVariable(String info, ReturnValue out){
		Map<String, Object> outputMap = new HashMap<String, Object>();
		IRelationalDataManager adapter = null;
		try {
			Map<String, Object> map = JsonUtil.toHashMap(info);
			DSVariable variable = JsonUtil.toGsonBean(map.get("Variable")+"", DSVariable.class);
			if (variable!=null){
				adapter = DataAdapterFactory.getDefaultDataManager();
				adapter.openSession();
				IVariableDao variableDao = new VariableDaoImpl(adapter);
				DSVariable ds_var = variableDao.getObjectById(variable.getId(), DSVariable.class);
				if (!StringUtils.isNotNull(ds_var)){
					out.setMessageCode(MessageCode.NODATA);
				}else{
					variableDao.delete(variable.getId(), DSVariable.class);
					return outputMap;
				}
			}else{
				out.setMessageCode(MessageCode.NODATA);
			}
		} catch (Exception e) {
			if (adapter!=null){
				try {
					adapter.rollback();
				} catch (SQLException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			}
			out.setMessageCode(MessageCode.SQL_ERROR);
			logger.error("delelteVariable 创建失败", e);
		} finally{
			if (adapter!=null) adapter.closeSession();
		}
		return outputMap;
	}
	
	/**
	 * 检索变量的引用
	 * @param info
	 * @param out
	 * @return
	 */
	public Map<String, Object> findVariableReferences(String info, ReturnValue out){
		Map<String, Object> outputMap = new HashMap<String, Object>();
		IRelationalDataManager adapter = null;
		try {
			Map<String, Object> map = JsonUtil.toHashMap(info);
			DSVariable variable = JsonUtil.toGsonBean(map.get("Variable")+"", DSVariable.class);
			if (variable!=null){
				adapter = DataAdapterFactory.getDefaultDataManager();
				adapter.openSession();
				IVariableDao variableDao = new VariableDaoImpl(adapter);
				DSVariable ds_var = variableDao.getObjectById(variable.getId(), DSVariable.class);
				if (StringUtils.isNull(ds_var)){
					out.setMessageCode(MessageCode.NODATA);
				}else{
					List<FindResult> list = variableDao.findVariableById(variable.getId());
					outputMap.put("FindResults", list);
					return outputMap;
				}
			}else{
				out.setMessageCode(MessageCode.NODATA);
			}
		} catch (Exception e) {
			if (adapter!=null){
				try {
					adapter.rollback();
				} catch (SQLException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			}
			out.setMessageCode(MessageCode.SQL_ERROR);
			logger.error("findVariable 失败", e);
		} finally{
			if (adapter!=null) adapter.closeSession();
		}
		return outputMap;
	}
	
	/**
	 * 复制variable
	 * @param info
	 * @param out
	 * @return
	 */
	public Map<String, Object> copyVariableByVariableId(String info, ReturnValue out){
		Map<String, Object> outputMap = new HashMap<String, Object>();
		IRelationalDataManager adapter3 = null;
		try {
			Map<String, Object> map = JsonUtil.toHashMap(info);
			Integer variableId = map.get("VariableId")==null?0:Integer.parseInt(map.get("VariableId")+"");
			boolean silence = map.get("Silence")==null?false:Boolean.parseBoolean(map.get("Silence")+"");
			Integer projectId = map.get("ProjectId")==null?0:Integer.parseInt(map.get("ProjectId")+"");
			Integer squidFlowId = map.get("SquidFlowId")==null?0:Integer.parseInt(map.get("SquidFlowId")+"");
			Integer squidId = map.get("SquidId")==null?0:Integer.parseInt(map.get("SquidId")+"");
			Integer scope = map.get("Scope")==null?0:Integer.parseInt(map.get("Scope")+"");
			if (variableId>0){
				adapter3 = DataAdapterFactory.getDefaultDataManager();
				adapter3.openSession();
				IVariableDao variableDao = new VariableDaoImpl(adapter3);
				DSVariable oldVar = variableDao.getObjectById(variableId, DSVariable.class);
				if (oldVar!=null){
					oldVar.setSquid_flow_id(squidFlowId);
					oldVar.setSquid_id(squidId);
					oldVar.setProject_id(projectId);
					oldVar.setVariable_scope(scope);
					boolean flag = variableDao.getExistsNameByObject(oldVar);
					if (flag&&!silence){
						out.setMessageCode(MessageCode.WARN_VARIABLE_EXISTS);
						String path = variableDao.getExistsNameForPath(oldVar);
						outputMap.put("VariablePath", path);
						return outputMap;
					}else{
						DSVariable newVariable = variableDao.copyVariableById(oldVar, oldVar.getSquid_id(), 
								oldVar.getSquid_flow_id(), oldVar.getProject_id());
						outputMap.put("Variable", newVariable);
						return outputMap;
					}
				}else{
					out.setMessageCode(MessageCode.NODATA);
				}
			}else{
				out.setMessageCode(MessageCode.NODATA);
			}
		} catch (Exception e) {
			if (adapter3!=null){
				try {
					adapter3.rollback();
				} catch (SQLException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			}
			out.setMessageCode(MessageCode.SQL_ERROR);
			logger.error("copyVariableByVariableId error", e);
		} finally{
			if(adapter3!=null) adapter3.closeSession();
		}
		return null;
	}
	
	/**
	 * 检查变量名是否被引用
	 * @param info
	 * @param out
	 * @return
	 */
	public Map<String, Object> findVariableExists(String info, ReturnValue out){
		Map<String, Object> outputMap = new HashMap<String, Object>();
		IRelationalDataManager adapter = null;
		try {
			Map<String, Object> paramsMap = JsonUtil.toHashMap(info);
			int variableId = Integer.parseInt(String.valueOf(paramsMap.get("VariableId")));
			adapter = DataAdapterFactory.getDefaultDataManager();
			adapter.openSession();
			IVariableDao variableDao = new VariableDaoImpl(adapter);
			DSVariable ds_var = variableDao.getObjectById(variableId, DSVariable.class);
			if (StringUtils.isNull(ds_var)){
				out.setMessageCode(MessageCode.NODATA);
			}else{
				boolean flag = variableDao.findVariableExistsById(variableId);
				outputMap.put("Flag", flag);
				return outputMap;
			}
		} catch (Exception e) {
			if (adapter!=null){
				try {
					adapter.rollback();
				} catch (SQLException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
			}
			out.setMessageCode(MessageCode.SQL_ERROR);
			logger.error("findVariableExists error", e);
		} finally{
			if(adapter!=null) adapter.closeSession();
		}
		return null;
	}
}
