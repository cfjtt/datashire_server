package com.eurlanda.datashire.sprint7.service.workspace;

import org.apache.log4j.Logger;
import org.springframework.stereotype.Service;


/**
 *
 * 根据前端传送的取消状态，来终止推送的方法
 * @version 1.0
 * @author lei.bin
 * @created 2013-09-17
 */
@Service
public class ValidationCancleService implements IValidationCancleService{
	/**
	 * 记录ValidationCancleService日志
	 */
	static Logger logger = Logger.getLogger(ValidationCancleService.class);
	/**
	 * 定义取消状态
	 */
	private static boolean cancleValue;
	
	private String token;
	
	private String key;

	public ValidationCancleService() {

	}

	public ValidationCancleService(String token) {
		this.token = token;
	}

	public static boolean isCancleValue() {
		return cancleValue;
	}

	public static void setCancleValue(boolean cancleValue) {
		ValidationCancleService.cancleValue = cancleValue;
	}

	/**
	 * 判断取消方法是否被调用
	 * @return 
	 */
	public String cancle() {
		logger.debug("============================调用了取消方法=======================================");
		  cancleValue = true;
          return "";
	}

}
