package com.eurlanda.datashire.socket.protocol;

/**
 * 
 * 协议对象进行远程呼叫的映射对象
 * */

/**
 * 
 * <h3>文件名称：</h3>
 * ServerProtocolRemoteInvoker.java
 * <h3>内容摘要：</h3>
 * 协议对象进行远程呼叫的映射对象
 * <h3>其他说明：</h3>
 * <h4>数据格式：</h4>
 * <table border=1>
 * <tr>
 * <td>参数</td><td>用法</td>
 * </tr>
 * <tr>
 * <td>targetMethod</td><td>targetMethod:'HelloWorld'</td>
 * </tr>
 * <tr>
 * <td>targetXmlns</td><td>targetXmlns:'http:\\tempuri.org'</td>
 * </tr>
 * <tr>
 * <td>targetParamKeys</td><td>targetParamKeys: ['a','b','c']</td>
 * </tr>
 * <tr>
 * <td>targetParamKeyType</td><td>targetParamKeyType: {a:'String',b:'Int',c:'Boolean'}</td>
 * </tr>
 * <tr>
 * <td>JSON映射</td><td>{targetMethod:'HelloWorld',targetXmlns:'http:\\tempuri.org', targetParamKeys: ['a','b','c'], targetParamKeyType: {a:'String',b:'Int',c:'Boolean'}}</td>
 * </tr>
 * </table>
 * <h4>调用关系：</h4>
 * 在 {@link ProtocolThreadPoolTask#myRun } 方法中被调用
 * <h4>应用场景：</h4>
 * 主要对Spring配置文件中对协议->WS呼叫方法的JSON字符串的对象解析
 * 
 * */
public class ServerProtocolRemoteInvoker {

}
