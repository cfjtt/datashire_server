package com.eurlanda.datashire.socket;

import com.eurlanda.datashire.socket.protocol.Protocol;
import com.eurlanda.datashire.utility.CommonConsts;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * 
 * <p>
 * Title :
 * </p>
 * <p>
 * Description:
 * </p>
 * <p>
 * Author :何科敏 Sep 5, 2013
 * </p>
 * <p>
 * update :何科敏 Sep 5, 2013
 * </p>
 * <p>
 * Department : JAVA后端研发部
 * </p>
 * Copyright : ©2012-2013 悦岚（上海）数据服务有限公司 </p>
 */
public class AgreementProtocol {

	static Logger logger = Logger.getLogger(AgreementProtocol.class);

	public static Map<String, Object> AGREEMENT_HANDLER = new HashMap<String, Object>();

	public static Map<String, Protocol> AGREEMENT_PROTOCOL = new HashMap<String, Protocol>();

	public AgreementProtocol() {
	}

	/**
	 * <p>
	 * 作用描述：实例化外部调用类，并加载
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 */
	public static void instantHandler() {
		logger.info("Loading handler class....");
		// 获取外部处理类
		Map<String, String> handlerMap = Agreement.AGREEMENT_CLASS;
		try {
			// 命令号集合
			Set<String> setKey = handlerMap.keySet();
			if (setKey != null && !setKey.isEmpty()) {
				// 迭代命令号
				Iterator<String> iterator = setKey.iterator();
				while (iterator.hasNext()) {
					// 得到命令号
					String handlerKey = iterator.next();
					if (!AGREEMENT_HANDLER.containsKey(handlerKey)) {
                        if(handlerMap.get(handlerKey).contains(".")) {  // 根据类型来从spring中找
                            AGREEMENT_HANDLER.put(handlerKey, CommonConsts.application_Context
                                    .getBean(Class.forName(handlerMap.get(handlerKey))));
                        } else {
                            try {
                                AGREEMENT_HANDLER.put(handlerKey, CommonConsts.application_Context
                                        .getBean(handlerMap.get(handlerKey)));
                            } catch (Exception e) {
                                logger.warn("service handler load failed: cmd = " + handlerKey, e);
                            }
                        }
                        logger.debug("service handler: cmd = " + handlerKey + ", obj = "
                                + AGREEMENT_HANDLER.get(handlerKey));
					}
				}
				logger.info("Successful load the handler class....");
			} else {
				logger.warn("Not loaded handler class....");
			}

		} catch (Exception e) {
			logger.warn("Failed to load the handler class....",e);
		}
	}

	public static void instantProtocol() {
		logger.info("Loading protocol class....");
		// 获取协议对象
		Map<String, String> protocolMap = Agreement.AGREEMENT_PROTOCOL;
		try {
			// 协议号集合
			Set<String> setKey = protocolMap.keySet();
			if (setKey != null && !setKey.isEmpty()) {
				// 迭代协议号
				Iterator<String> iterator = setKey.iterator();
				while (iterator.hasNext()) {
					// 得到协议号
					String protocolKey = iterator.next();
					if (!AGREEMENT_PROTOCOL.containsKey(protocolKey)) {
						// 得到对应的协议
						String protocolClass = protocolMap.get(protocolKey);
						// 加载外包处理协议
						Class<?> cls = Class.forName(protocolClass);
						// 实例化外包处理协议
						Protocol newClass =(Protocol)cls.newInstance();
						AGREEMENT_PROTOCOL.put(protocolKey, newClass);
					}
				}
				logger.info("Successful load the protocol class....");
			} else {
				logger.error("Not loaded protocol class....");
			}
		} catch (ClassNotFoundException e) {
			logger.error("Failed to load the protocol class....",e);
		} catch (InstantiationException e) {
			logger.error("Failed to load the protocol class....",e);
		} catch (Exception e) {
			logger.error("Failed to load the protocol class....",e);
		}
	}
}
