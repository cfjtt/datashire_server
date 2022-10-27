package com.eurlanda.datashire.sprint7.plug;

import com.eurlanda.datashire.adapter.DataAdapterFactory;
import com.eurlanda.datashire.adapter.IDBAdapter;
import com.eurlanda.datashire.utility.StringUtils;
import org.apache.log4j.Logger;

/**
 * 
 * <p>
 * Title :
 * </p>
 * <p>
 * Description:
 * </p>
 * <p>
 * Author :何科敏 Aug 26, 2013
 * </p>
 * <p>
 * update :何科敏 Aug 26, 2013
 * </p>
 * <p>
 * Department : JAVA后端研发部
 * </p>
 * Copyright : ©2012-2013 悦岚（上海）数据服务有限公司 </p>
 */
public abstract class SupportPlug {
	static Logger logger = Logger.getLogger(SupportPlug.class);// 记录日志
	protected DataAdapterFactory adapterFactory;
	protected IDBAdapter adapter;
	protected String token;

	public SupportPlug() {
		adapterFactory = DataAdapterFactory.newInstance();
	}

	public SupportPlug(String tokens) {
		if(StringUtils.isNotNull(tokens)){
			token = tokens;
			adapterFactory = DataAdapterFactory.newInstance();
			adapter = adapterFactory.getTokenAdapter(token);
//			try {
//				logger.debug(tokens+"\tadapter support ok! connection is valid = "+(adapter==null?false:adapter.isValid(0)));
//			} catch (SQLException e) {
//			}
		}else{
			logger.warn("token is null!");
		}
	}

}
