package com.eurlanda.datashire.socket.protocol;

import java.util.List;

public class ServiceMonitor {
	
	String cmd;
	String childCmd;
	String methodName;
	
	// 理论上 业务调用次数=接口请求次数，但是可能有些业务被拦截或异常情况未调用
	// 理论上 业务返回次数=接口返回次数，可能消息积压未返回或部分接口后台主动推送
	// 部分接口前台未请求是后台主动推送的（如消息气泡），则没有业务调用/返回、接口请求记录(其数据库连接使用情况也是包含在相关业务中的)
	int invokeCnt; // 业务调用次数
	int succeedCnt;// 业务返回次数
	int reqCnt; // 接口请求次数
	int respCnt;// 接口返回次数
	
	int dbConnectionCnt; // 使用数据库连接数
	int dbConnectionUnclosedCnt; // 数据库连 接未关闭个数
	List<Integer> exectueTimeList; // 历史业务执行时间（毫秒）
	
	//List<Integer> usedTimeList; // 消息后台处理时间（包括解码、（反）序列化、校验、执行、编码等时间，单位毫秒）
	
}
