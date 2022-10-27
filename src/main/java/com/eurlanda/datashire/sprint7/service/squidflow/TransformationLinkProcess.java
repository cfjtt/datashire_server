package com.eurlanda.datashire.sprint7.service.squidflow;

import com.eurlanda.datashire.entity.TransformationLink;
import com.eurlanda.datashire.sprint7.packet.InfoPacket;
import com.eurlanda.datashire.utility.JsonUtil;
import com.eurlanda.datashire.utility.MessageCode;
import com.eurlanda.datashire.utility.ReturnValue;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * TransformationLinkProcess预处理类
 * TransformationLinkProcess预处理类：对数据进行验证验证数据的完整性和正确性；并调用下层业务处理方法
 * Title :
 * Description:
 * Author :赵春花 2013-8-23
 * update :赵春花2013-8-23
 * Department : JAVA后端研发部
 * Copyright : ©2012-2013 悦岚（上海）数据服务有限公司 </p>
 */
public class TransformationLinkProcess implements ITransformationLinkProcess{
	static Logger logger = Logger.getLogger(TransformationLinkProcess.class);// 记录日志
	private DataShirServiceplug plug;
	private TransformationLinkService transformationLinkService;
	public TransformationLinkProcess(String token) {
		plug = new DataShirServiceplug(token);
		transformationLinkService = new TransformationLinkService(token);
	}

	/**
	 * 创建TransformationLink集合对象处理类
	 * 作用描述：
	 * 创建TransformationLink对象集合
	 * 成功根据TransformationLink对象集合里的key查询相对应的Id封装成InfoPacket对象集合返回
	 * 修改说明：
	 *@param info json字符串
	 *@param out 异常处理
	 *@return
	 */
	public List<InfoPacket> createTransformationLinks(String info,ReturnValue out){
		logger.debug(String.format("createTransformationLinks-info=%s", info));
		List<InfoPacket> infoPackets = new ArrayList<InfoPacket>();
		try {
			//序列化JSONArray
			List<TransformationLink> transformationLinks =  JsonUtil.toGsonList(info, TransformationLink.class);
			//验证Project集合对象
			if(transformationLinks==null||transformationLinks.size()==0){
				//TransformationLink对象集合不能为空size不能等于0
				out.setMessageCode(MessageCode.ERR_TRANSFORMATIONLINK_NULL);
				logger.debug(String.format("createTransformationLinks-transformationLinks=%s", false));
				return infoPackets;
			}else{
				infoPackets = transformationLinkService.createTransformationLinks(transformationLinks, out);
				logger.debug(String.format("createTransformationLinks-return=%s", infoPackets));
				return infoPackets;
			}
		} catch (Exception e) {
			out.setMessageCode(MessageCode.ERR_ARRAYS);
		}
		return infoPackets;
	}

	/**
	 * 修改TransformationLink对象集合
	 * 作用描述：
	 * 根据TransformationLink对象集合里TransformationLink的ID进行修改
	 * 修改说明：
	 *@param info json字符串
	 *@param out 异常处理
	 *@return
	 */
	public List<InfoPacket> updateTransformationLinks(String info,ReturnValue out){
		logger.debug(String.format("updateTransformationLinks-info=%s", info));
		List<InfoPacket> infoPackets = new ArrayList<InfoPacket>();
		//Json序列化
		List<TransformationLink> transformationLinks= JsonUtil.toGsonList(info, TransformationLink.class);
		if(transformationLinks == null || transformationLinks.size() == 0){
			out.setMessageCode(MessageCode.ERR_TRANSFORMATIONLINK_NULL);
			return infoPackets;
		}
		infoPackets = transformationLinkService.updateTransformationLinks(transformationLinks, out);
		logger.debug(String.format("updateTransformationLinks-return=%s", out.getMessageCode()));
		return infoPackets;

	}

}
