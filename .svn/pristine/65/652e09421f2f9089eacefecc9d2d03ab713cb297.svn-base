package com.eurlanda.datashire.sprint7.plug;

import com.eurlanda.datashire.adapter.DataAdapterFactory;
import com.eurlanda.datashire.adapter.IRelationalDataManager;
import org.apache.log4j.Logger;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 
 * <p>
 * Title : 参数插件类
 * </p>
 * <p>
 * Description:
 * </p>
 * <p>
 * Author :何科敏 Sep 6, 2013
 * </p>
 * <p>
 * update :何科敏 Sep 6, 2013
 * </p>
 * <p>
 * Department : JAVA后端研发部
 * </p>
 * Copyright : ©2012-2013 悦岚（上海）数据服务有限公司 </p>
 */
public class ParameterPlug{
	static Logger logger = Logger.getLogger(ParameterPlug.class);// 记录日志
	
	/**
	 * <p>
	 * 作用描述：获取系统服务参数（由于获取系统参数提供内部用，无需入参）
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 * 
	 * @return
	 */
	public Map<String, Object> getServerParameter() {
		IRelationalDataManager adapter =  DataAdapterFactory.getDefaultDataManager();
		Map<String, Object> parameterMap = new HashMap<String, Object>();
		List<Map<String, Object>> columnsValue;
		try {
			columnsValue = adapter.query2List("SELECT * FROM DS_SYS_SERVER_PARAMETER", null);
			if (columnsValue == null || columnsValue.isEmpty()) {
				logger.warn("No ServerParameter columnsValue!");
			} else {
				// 变量对象集合
				for (int i = 0; i < columnsValue.size(); i++) {
					if (columnsValue.get(i).containsKey("NAME")){
						parameterMap.put(String.valueOf(columnsValue.get(i).get("NAME")), columnsValue.get(i).get("VALUE"));
					}else if (columnsValue.get(i).containsKey("name")){
						parameterMap.put(String.valueOf(columnsValue.get(i).get("name")), columnsValue.get(i).get("value"));
					}
				}
				//parameterMap.put("LicenseKey", "");
				logger.debug("Load ServerParameter SUCCESS!");
			}
			//adapter.commitAdapter();
		} catch (SQLException e) {
			logger.error("", e);
		}
		return parameterMap;
	}
	
}