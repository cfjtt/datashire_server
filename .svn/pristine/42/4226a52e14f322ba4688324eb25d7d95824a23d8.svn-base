package com.eurlanda.datashire.sprint7.service.squidflow;

import com.eurlanda.datashire.enumeration.DSObjectType;
import com.eurlanda.datashire.utility.JsonUtil;
import com.eurlanda.datashire.utility.ReturnValue;
import org.apache.log4j.Logger;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
public class UserNewService {
	private static Logger logger = Logger.getLogger(UserNewService.class);// 记录日志
	private String token;// 令牌根据令牌得到相应的连接信息
	private String key;// key值

	public String getToken() {
		return token;
	}

	public void setToken(String token) {
		this.token = token;
	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	// 异常处理机制
	ReturnValue out = null;
    @Deprecated // 老接口
	public String login(String info) throws Exception
	{
		out = new ReturnValue();
		UserNewProcess userNewProcess=new UserNewProcess();
//		Map<String, Object> map=userNewProcess.login(info, out);
//		return infoNewMessagePacket(map, DSObjectType.USER, out);
        return null;
	}
	/**
	 * 删除用户
	 * @param info
	 * @return
	 */
	public String deleteUser(String info)
	{
		out = new ReturnValue();
		UserNewProcess userNewProcess=new UserNewProcess();
		userNewProcess.deleteUser(info, out);
		return infoNewMessagePacket(null, DSObjectType.USER, out);
	}
	/**
	 * 新增用户
	 * @param info
	 * @return
	 */
	public String createUser(String info)
	{
		out = new ReturnValue();
		UserNewProcess userNewProcess=new UserNewProcess();
		Map<String, Object> map=userNewProcess.createUser(info, out);
		return infoNewMessagePacket(map, DSObjectType.USER, out);
	}
	/**
	 * 更新用户(不更新密码)
	 * @param info
	 * @return
	 */
	public String updateUser(String info)
	{
		out = new ReturnValue();
		UserNewProcess userNewProcess=new UserNewProcess();
		userNewProcess.updateUser(info, out);
		return infoNewMessagePacket(null, DSObjectType.USER, out);
	}
	/**
	 * 单独更新密码
	 */
	public String updateUserPassword(String info){
		out = new ReturnValue();
		UserNewProcess userNewProcess=new UserNewProcess();
		userNewProcess.updateUserPassword(info, out);
		return infoNewMessagePacket(null, DSObjectType.USER, out);
	}
	/**
	 * 更新用户
	 * @param info
	 * @return
	 */
	public String updateUserNew(String info)
	{
		out = new ReturnValue();
		UserNewProcess userNewProcess=new UserNewProcess();
		Map<String,Object> returnMap = userNewProcess.updateUserNew(info, out);
		return infoNewMessagePacket(returnMap, DSObjectType.USER, out);
	}
	/**
	 *获取所有用户
	 * @return
	 */
	public String getAllUser()
	{
		out = new ReturnValue();
		UserNewProcess userNewProcess=new UserNewProcess();
		Map<String, Object> map=userNewProcess.getAllUser(out);
		return infoNewMessagePacket(map, DSObjectType.USER, out);
	}
	
	/**
	 * 获取license的key值
	 * @return
	 */
	public String getLicenseKey(){
		out  = new ReturnValue();
		UserNewProcess userNewProcess=new UserNewProcess();
		Map<String, Object> map= userNewProcess.getLicenseKey(out);
		return infoNewMessagePacket(map, DSObjectType.LINCENSE, out);
	}
	
	public String setLicense(String info){
		out = new ReturnValue();
		UserNewProcess userNewProcess=new UserNewProcess();
		Map<String, Object> map=userNewProcess.setLicense(info, out);
		return infoNewMessagePacket(map, DSObjectType.LINCENSE, out);
	}
	
	/**
	 * 获取squid的总数量
	 * @return
	 */
	public String getSquidAllForCount(){
		out  = new ReturnValue();
		UserNewProcess userNewProcess=new UserNewProcess();
		Map<String, Object> map= userNewProcess.getSquidAllForCount(out);
		return infoNewMessagePacket(map, DSObjectType.SQUID, out);
	}
	
	/**
	 * 单对象转换成Json格式
	 * 作用描述：
	 * 修改说明：
	 * @param <T>
	 * @return
	 */
	private <T> String infoNewMessagePacket(T object,DSObjectType type,ReturnValue out) {
		return JsonUtil.toJsonString(object, type, out.getMessageCode());
	}
}
