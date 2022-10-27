package com.eurlanda.datashire.entity;

import com.eurlanda.datashire.enumeration.SubDIProcessCode;

import java.sql.Connection;
import java.util.List;



/**
 * @author Fumin
 * @version 1.0
 * @created 16-May-2013 5:41:26 PM
 */
public class SubDIProcess {
	
	/**
	 * 数据库链接
	 */
	private Connection connection;

	/**
	 * Process定义ID
	 */
	private String processId;
	
	/**
	 * 执行方法
	 */
	private String method;
	
	/**
	 * 参数列表
	 */
	private List<Object> parameters;
	
	/**
	 * the target squid on which this subprocess is processing
	 */
	private Squid hostSquid;
	
	/**
	 * unique identifier of a running subDIProcess
	 */
	private int subDIProcessID;
	
	/**
	 * DIProcess全路径
	 */
	private String subDIProcessClassPath;
	
	/**
	 * 执行方法
	 */
	private String subDIProcessMethod;
	
	/**
	 * Sub DIProcess 类型识别代码
	 */
	private SubDIProcessCode subDIProcessCode;
	
	/**
	 * 构造器
	 */
	public SubDIProcess(){

	}

	public void finalize() throws Throwable {

	}

	/**
	 * Process定义ID
	 */
	public String getProcessId(){
		return processId;
	}

	/**
	 * Process定义ID
	 * 
	 * @param newVal
	 */
	public void setProcessId(String newVal){
		processId = newVal;
	}

	/**
	 * 执行方法
	 */
	public String getMethod(){
		return method;
	}

	/**
	 * 执行方法
	 * 
	 * @param newVal
	 */
	public void setMethod(String newVal){
		method = newVal;
	}

	public List<Object> getParameters(){
		return parameters;
	}

	/**
	 * 
	 * @param newVal
	 */
	public void setParameters(List<Object> newVal){
		parameters = newVal;
	}

	/**
	 * the target squid on which this subprocess is processing
	 */
	public Squid getHostSquid(){
		return hostSquid;
	}

	/**
	 * the target squid on which this subprocess is processing
	 * 
	 * @param newVal
	 */
	public void setHostSquid(Squid newVal){
		hostSquid = newVal;
	}

	/**
	 * unique identifier of a running subDIProcess
	 */
	public int getSubDIProcessID(){
		return subDIProcessID;
	}

	/**
	 * unique identifier of a running subDIProcess
	 * 
	 * @param newVal
	 */
	public void setSubDIProcessID(int newVal){
		subDIProcessID = newVal;
	}

	/**
	 * DIProcess全路径
	 */
	public String getSubDIProcessClassPath(){
		return subDIProcessClassPath;
	}

	/**
	 * DIProcess全路径
	 * 
	 * @param newVal
	 */
	public void setSubDIProcessClassPath(String newVal){
		subDIProcessClassPath = newVal;
	}

	public String getSubDIProcessMethod() {
		return subDIProcessMethod;
	}

	public void setSubDIProcessMethod(String subDIProcessMethod) {
		this.subDIProcessMethod = subDIProcessMethod;
	}

	public SubDIProcessCode getSubDIProcessCode() {
		return subDIProcessCode;
	}

	public void setSubDIProcessCode(SubDIProcessCode subDIProcessCode) {
		this.subDIProcessCode = subDIProcessCode;
	}

	public Connection getConnection() {
		return connection;
	}

	public void setConnection(Connection con) {
		this.connection = con;
	}
}