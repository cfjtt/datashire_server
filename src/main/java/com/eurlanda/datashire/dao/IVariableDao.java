package com.eurlanda.datashire.dao;

import com.eurlanda.datashire.entity.DSVariable;
import com.eurlanda.datashire.entity.FindResult;
import com.ibm.db2.jcc.c.SqlException;

import java.sql.SQLException;
import java.util.List;

public interface IVariableDao extends IBaseDao {
	/**
	 * 查询在同作用域中是否存在同名的变量名
	 * @param variable
	 * @return
	 */
	public boolean getExistsNameByObject(DSVariable variable) throws SQLException;
	
	/**
	 * 查询在同作用域中是否存在同名的变量名
	 * @param variable
	 * @return
	 */
	public boolean getExistsNameByObject(DSVariable variable, int type) throws SQLException;
	
	/**
	 * 获取重复名称的路径
	 * @param variable
	 * @return
	 * @throws SQLException
	 */
	public String getExistsNameForPath(DSVariable variable) throws SQLException;
	
	/**
	 * 根据作用域查询变量集合
	 * @param scope
	 * @return
	 * @throws SQLException
	 */
	public List<DSVariable> getDSVariableByScope(int scope, int id) throws SQLException;
	
	/**
	 * 根据作用域查询变量集合
	 * @param scope
	 * @return
	 * @throws SQLException
	 */
	public List<DSVariable> getDSVariableByScope(int scope, int id, String ids) throws SQLException;
	
	/**
	 * 检索变量引用的列表
	 * @param variableId
	 * @return
	 * @throws SqlException
	 */
	public List<FindResult> findVariableById(int variableId) throws Exception;
	
	/**
	 * 复制变量对象
	 * @param oldVariable
	 * @param newSquidId
	 * @param newSquidFlowId
	 * @param newProjectId
	 * @return
	 * @throws SQLException
	 */
	public DSVariable copyVariableById(DSVariable oldVariable, 
			int newSquidId, int newSquidFlowId, int newProjectId) throws SQLException;
	
	/**
	 * 查询变量是否被引用
	 * @param variableId
	 * @return
	 * @throws Exception
	 */
	public boolean findVariableExistsById(int variableId) throws Exception;

	
	/**
	 * 生成不重名的变量
	 * @param variable
	 * @throws SQLException
	 */
	public void resetVariableName(DSVariable variable) throws SQLException;

}
