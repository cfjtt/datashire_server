package com.eurlanda.datashire.sprint7.service.squidflow;

/**
 * 变量service
 */

import com.eurlanda.datashire.enumeration.DSObjectType;
import com.eurlanda.datashire.utility.JsonUtil;
import com.eurlanda.datashire.utility.ReturnValue;
import org.apache.log4j.Logger;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
public class VariableService {
	private static Logger logger = Logger.getLogger(VariableService.class);// 记录日志
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
	
	/**
	 * 创建变量对象
	 * @param info
	 * @return
	 */
	public String createVariable(String info){
		out  = new ReturnValue();
		VariableProcess process = new VariableProcess();
		Map<String, Object> map = process.createVariable(info, out);
		return JsonUtil.toJsonString(map, DSObjectType.VARIABLE, out.getMessageCode());
	}
	
	/**
	 * 修改变量对象
	 * @param info
	 * @return
	 */
	public String updateVariable(String info){
		out  = new ReturnValue();
		VariableProcess process = new VariableProcess();
		Map<String, Object> map = process.updateVariable(info, out);
		return JsonUtil.toJsonString(map, DSObjectType.VARIABLE, out.getMessageCode());
	}
	
	/**
	 * 删除变量
	 * @param info
	 * @return
	 */
	public String deleteVariable(String info){
		out  = new ReturnValue();
		VariableProcess process = new VariableProcess();
		Map<String, Object> map = process.deleteVariable(info, out);
		return JsonUtil.toJsonString(map, DSObjectType.VARIABLE, out.getMessageCode());
	}
	
	/**
	 * 检索变量的引用列表
	 * @param info
	 * @return
	 */
	public String findVariableReferences(String info){
		out  = new ReturnValue();
		VariableProcess process = new VariableProcess();
		Map<String, Object> map = process.findVariableReferences(info, out);
		return JsonUtil.toJsonString(map, DSObjectType.VARIABLE, out.getMessageCode());
	}
	
	/**
	 * 复制变量对象
	 * @param info
	 * @return
	 */
	public String copyVariable(String info){
		out = new ReturnValue();
		VariableProcess process = new VariableProcess();
		Map<String, Object> map = process.copyVariableByVariableId(info, out);
		return JsonUtil.toJsonString(map, DSObjectType.VARIABLE, out.getMessageCode());
	}
	
	/**
	 * 检索变量的是否被引用
	 * @param info
	 * @return
	 */
	public String findVariableExists(String info){
		out = new ReturnValue();
		VariableProcess process = new VariableProcess();
		Map<String, Object> map = process.findVariableExists(info, out);
		return JsonUtil.toJsonString(map, DSObjectType.VARIABLE, out.getMessageCode());
	}
}
