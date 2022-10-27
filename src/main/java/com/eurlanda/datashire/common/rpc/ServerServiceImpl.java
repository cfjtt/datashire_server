package com.eurlanda.datashire.common.rpc;

import com.eurlanda.datashire.enumeration.DSObjectType;
import com.eurlanda.datashire.enumeration.datatype.DeBugStatusEnum;
import com.eurlanda.datashire.sprint7.packet.PushMessagePacket;
import com.eurlanda.datashire.utility.JsonUtil;
import com.eurlanda.report.global.Global;
import org.apache.avro.AvroRemoteException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Service;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 服务接口实现。
 * 
 * @date 2014-5-20
 * @author yi.zhou
 *   
 */
@Service
public class ServerServiceImpl implements IServerService, Serializable {

	private static final long serialVersionUID = 1L;
    private static Log log = LogFactory.getLog(ServerServiceImpl.class);

	@Override
	public void onTaskFinish(java.lang.CharSequence taskId, int status) throws AvroRemoteException {
        log.debug("-------------------server request----------------");
		// 发送消息气饱
		Map<String, Object> rs = new HashMap<String, Object>();
		Map<String, Object> params = new HashMap<String, Object>();
		params.put("is_final", true);
		params.put("squidFlowDebugStatus",status);
		//log.info("接受到的状态为:"+status+" taskId:"+taskId);
		Map<String, Object> obj = (Map<String, Object>)Global.getTask2Map(taskId.toString());
		if (obj!=null&&obj.get("token")!=null&&obj.get("key")!=null){
			String token  = obj.get("token")+"";
			String key = obj.get("key")+"";
			rs.put("DebugInfo", params);
			if(status<4) {
				//log.info("开始发送状态，状态为:"+status+" taskId:"+taskId);
				PushMessagePacket.pushMap(rs, DSObjectType.RUNSQUIDFLOW, "1011", "0000",
						key, token);
			} else {
				PushMessagePacket.pushMap(rs,DSObjectType.RUNSQUIDFLOW,"1011","0000",key,token,status);
			}
		}
		System.out.println("report finished status=!"+status);
	}

	@Override
	public boolean onDebugSquid(CharSequence taskId, int debugSquidId)
			throws AvroRemoteException {
		// 发送消息气饱
		boolean flag = false;
		try {
			Map<String, Object> rs = new HashMap<String, Object>();
			Map<String, Object> params = new HashMap<String, Object>();
			params.put("squid_id", debugSquidId);
			params.put("status", DeBugStatusEnum.PAUSE.value());
			Map<String, Object> obj = (Map<String, Object>)Global.getTask2Map(taskId.toString());
			if (obj!=null&&obj.get("token")!=null&&obj.get("key")!=null){
				String token  = obj.get("token")+"";
				String key = obj.get("key")+"";
				
				rs.put("DebugInfo", params);
				PushMessagePacket.pushMap(rs, DSObjectType.RUNSQUIDFLOW, "1011", "0000",
						key, token);
				flag = true;
			}
			log.debug("onDebugSquid finished!————"+params.toString());
		} catch (Exception e) {
			e.printStackTrace();
		}
		return flag;
	}

	@Override
	public boolean onDataViewSquid(CharSequence taskId, int dataViewSquidId,
			CharSequence data) throws AvroRemoteException {
		// 发送消息气饱
		boolean flag = false;
		try {
			Map<String, Object> rs = new HashMap<String, Object>();
			Map<String, Object> params = new HashMap<String, Object>();
			params.put("squid_id", dataViewSquidId);
			/*JSONArray jsonObj = JsonUtil.toJSONArray(data.toString());
			params.put("data", jsonObj);*/
			List dataStr = JsonUtil.toGsonList(data.toString(), ArrayList.class);
			/*if (dataStr!=null){
				for (Object object : dataStr) {
					List childDataStr = JsonUtil.toGsonList(object.toString(), ArrayList.class);
					if (childDataStr!=null){
						for (Object obj : childDataStr) {
							if (obj instanceof byte[]){
								String str = new String((byte[])obj, "UTF-8");
								obj = str;
							}
						}
					}
				}
			}*/
			params.put("data", dataStr);
			Map<String, Object> obj = (Map<String, Object>)Global.getTask2Map(taskId.toString());
			if (obj!=null&&obj.get("token")!=null&&obj.get("key")!=null){
				String token  = obj.get("token")+"";
				String key = obj.get("key")+"";
				
				rs.put("DebugInfo", params);
				PushMessagePacket.pushMap(rs, DSObjectType.RUNSQUIDFLOW, "1011", "0000",
						key, token);
				flag = true;
			}
            log.debug("onDataViewSquid finished!————"+params.toString());
		} catch (Exception e) {
			e.printStackTrace();
		}
		return flag;
	}

	@Override
	public CharSequence test(CharSequence test) throws AvroRemoteException {
		// TODO Auto-generated method stub
		System.out.println(test.toString());
		return test;
	}

	/**
	 * status: -1(失败)、0(进行中)、1(成功)
	 */
	@Override
	public boolean squidStatus(CharSequence taskId, int squidId, int status)
			throws AvroRemoteException {
		// 发送消息气饱
		boolean flag = false;
		try {
			Map<String, Object> rs = new HashMap<String, Object>();
			Map<String, Object> params = new HashMap<String, Object>();
			params.put("squid_id", squidId);
			switch (status) {
				case 0:
					params.put("status", DeBugStatusEnum.RUNNING.value());
					break;
				case 1:
					params.put("status", DeBugStatusEnum.COMPLETED.value());
					break;
				default:
					params.put("status", DeBugStatusEnum.ERROR.value());
					break;
			}
			Map<String, Object> obj = (Map<String, Object>)Global.getTask2Map(taskId.toString());
			if (obj!=null&&obj.get("token")!=null&&obj.get("key")!=null){
				String token  = obj.get("token")+"";
				String key = obj.get("key")+"";
				
				rs.put("DebugInfo", params);
				PushMessagePacket.pushMap(rs, DSObjectType.RUNSQUIDFLOW, "1011", "0000",
						key, token);
				flag = true;
			}
            log.debug("squidflow 运行中状态变更:" + params.toString());
		} catch (Exception e) {
			e.printStackTrace();
		}
		return flag;
	}
	
	public static void main(String[] args) {
		String temp = "['abc', 'fgg', '[122,120,99]', '[113,119,101]', 'aaa', 'abcdfg', '1', 'Mar 31', '2015 5:51:04 PM']";
		List dataStr = JsonUtil.toGsonList(temp, ArrayList.class);
		if (dataStr!=null){
			for (Object obj : dataStr) {
				if (obj instanceof byte[]){
					String str;
					try {
						str = new String((byte[])obj, "UTF-8");
						obj = str;
					} catch (UnsupportedEncodingException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
		}
		String ss = JsonUtil.toJSONString(dataStr);
		System.out.println(ss);
	}
}
