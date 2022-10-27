package com.eurlanda.datashire.sprint7.socket;


/**
 * 调用拦截器。
 * 
 * @date 2014-4-21
 * @author jiwei.zhang
 * 
 */
public interface IInvokeInterceptor {
	/**
	 * 执行之前调用 ，返回false将终止invoke
	 * 
	 * @date 2014-4-21
	 * @author jiwei.zhang
	 * @return
	 */
	boolean beforeInvoke(SocketRequestContext sc);
	/**
	 * 在执行之后调用 。
	 * @date 2014-4-21
	 * @author jiwei.zhang
	 * @param packet
	 */
	void afterInvoke(SocketRequestContext sc);

}
