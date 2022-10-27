package com.eurlanda.report.global;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

public class Global {
	private static Properties prop= null;
	
	private static Map<String, Object> taskForTokenMap = new ConcurrentHashMap<>();

	/**
	 * 取系统配置参数
	 * @date 2014-5-17
	 * @author jiwei.zhang
	 * @param key
	 * @return
	 */
	public static String getProperty(String key){
		if(prop==null){
			loadProps();
		}
		return prop.getProperty(key);
	}
	/**
	 * 获取系统配置参数
	 * @date 2014-5-17
	 * @author jiwei.zhang
	 * @param key 参数名
	 * @param defaults 默认值
	 * @return
	 */
	public static String getProperty(String key,String defaults){
		if(prop==null){
			loadProps();
		}
		return prop.getProperty(key,defaults);
	}
	/**
	 * 加载配置文件。
	 * @date 2014-5-17
	 * @author jiwei.zhang
	 */
	private static void loadProps() {
		InputStream propIn = Global.class.getResourceAsStream("/config/MailServer.properties");
		prop=new Properties();
		try {
			prop.load(propIn);
		} catch (IOException e) {
			e.printStackTrace();
		}
		try {
			propIn.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 绑定taskId
	 * @param taskId
	 * @param obj
	 */
	public static void pushTask2Map(String taskId, Object obj){
		taskForTokenMap.put(taskId, obj);
	}
	
	/**
	 * 移除taskId
	 * @param taskId
	 */
	public static void removeTask2Map(String taskId){
		taskForTokenMap.remove(taskId);
	}
	
	/**
	 * 获取taskId绑定的值
	 * @param taskId
	 * @return
	 */
	public static Object getTask2Map(String taskId){
		return taskForTokenMap.get(taskId);
	}
}
