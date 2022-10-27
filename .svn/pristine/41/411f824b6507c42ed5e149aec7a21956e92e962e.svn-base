package com.eurlanda.datashire.sprint7.service.squidflow;

import com.eurlanda.datashire.enumeration.DSObjectType;
import com.eurlanda.datashire.server.utils.TokenUtil;
import com.eurlanda.datashire.utility.JsonUtil;
import com.eurlanda.datashire.utility.ReturnValue;
import org.apache.log4j.Logger;
import org.springframework.stereotype.Service;

import java.util.Map;

/**
 * SquidFlow加锁解锁接口
 * @author lei.bin
 *
 */
@Service
public class LockSquidFlowService {
	static Logger logger = Logger.getLogger(LockSquidFlowService.class);// 记录日志
	private String token;//令牌根据令牌得到相应的连接信息
	private String key;//key值
	
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
	//异常处理机制
    ReturnValue out = null;

    /**
     * 询问squidFlow是否加锁
     * @return
     */
    public String getSquidFlowStatus(String info)
    {
    	out = new ReturnValue();
    	LockSquidFlowProcess lockSquidFlowProcess=new LockSquidFlowProcess(TokenUtil.getToken());
    	Map<String, Object> map=lockSquidFlowProcess.getStatus(info, out);
    	return infoNewMessagePacket(map, DSObjectType.SQUID_FLOW, out);
    }
    /**
     * 解锁squidFlow(分为管理员主动解锁，和关闭窗口解锁)
     * @return
     */
    public String unLockSquidFlow(String info)
    {
    	out = new ReturnValue();
    	LockSquidFlowProcess lockSquidFlowProcess=new LockSquidFlowProcess(TokenUtil.getToken());
    	lockSquidFlowProcess.unLockSquidFlow(info, out);
    	return infoNewMessagePacket(null, DSObjectType.SQUID_FLOW, out);
    }
    
    public String sendAllClient()
    {
    	out = new ReturnValue(); 
    	LockSquidFlowProcess lockSquidFlowProcess=new LockSquidFlowProcess(TokenUtil.getToken());
    	/*Map<String, Object> outputMap=new HashMap<String, Object>();
    	lockSquidFlowProcess.sendAllClient(outputMap);*/
    	return null;
    }
    /**
	 * 单对象转换成Json格式
	 * 作用描述：
	 * 修改说明：
	 * @param <T>
	 * @return
	 *	 *@deprecated 请参考 JsonUtil.toString(...)
	 */
	private <T> String infoNewMessagePacket(T object,DSObjectType type,ReturnValue out) {
		return JsonUtil.toJsonString(object, type, out.getMessageCode());
	}
}
