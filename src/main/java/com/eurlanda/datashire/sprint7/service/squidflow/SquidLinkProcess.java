package com.eurlanda.datashire.sprint7.service.squidflow;

import com.eurlanda.datashire.entity.SquidLink;
import com.eurlanda.datashire.sprint7.packet.InfoPacket;
import com.eurlanda.datashire.utility.JsonUtil;
import com.eurlanda.datashire.utility.MessageCode;
import com.eurlanda.datashire.utility.ReturnValue;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * SquidLinkProcess预处理类
 * SquidLinkProcess预处理类：对数据进行验证验证数据的完整性和正确性；并调用下层业务处理方法
 * Title :
 * Description:
 * Author :赵春花 2013-8-23
 * update :赵春花2013-8-23
 * Department : JAVA后端研发部
 * Copyright : ©2012-2013 悦岚（上海）数据服务有限公司 </p>
 */
public class SquidLinkProcess implements ISquidLinkProcess{
	static Logger logger = Logger.getLogger(SquidLinkProcess.class);// 记录日志
	private SquidLinkService squidLinkService;
	public SquidLinkProcess(String token) {
		squidLinkService = new SquidLinkService(token);
	}

	/**
	 * 创建SquidLink集合对象处理类
	 * 作用描述：
	 *将json对象解析成SquidLink对象调用SquidLinkService处理业务逻辑
	 *验证：
	 *1、验证数据是否为空
	 *2、验证SquidLink的必要字段值是否正确
	 * 修改说明：
	 *@param info json字符串
	 *@param out 异常处理
	 *@return
	 */
	public List<InfoPacket> createSquidLinks(String info,ReturnValue out){
		logger.debug(String.format("createSquidLinks-info=%s", info));
		List<InfoPacket> infoPackets = new ArrayList<InfoPacket>();
		try {
			//序列化JSONArray
			List<SquidLink> squidLinks =  JsonUtil.toGsonList(info, SquidLink.class);
			//验证SquidLink集合对象
			if(squidLinks==null||squidLinks.size()==0){
				//SquidLink对象集合不能为空size不能等于0
				//记录异常
				out.setMessageCode(MessageCode.ERR_SQUIDlINK_LIST_NULL);
				logger.debug(String.format("createSquidLinks-result-=%s;Err_SquidLinkList.size()=null", false));
				return infoPackets;
			}else{
				//验证数据完整性
				for (SquidLink squidLink : squidLinks) {
					if(squidLink.getFrom_squid_id() == 0 || squidLink.getSquid_flow_id() == 0 || squidLink.getTo_squid_id() == 0 || squidLink.getKey()== null){
						out.setMessageCode(MessageCode.ERR_SQUIDLINK_DATA_INCOMPLETE);
						return infoPackets;
					}
				}
				infoPackets = squidLinkService.createSquidLinks(squidLinks, out);
				logger.debug(String.format("createSquidLinks-return:infoPacket.size()=%s", infoPackets));
			}
		} catch (Exception e) {
			out.setMessageCode(MessageCode.DESERIALIZATION_FAILED);
		}
		return infoPackets;
	}
	
	/**
	 * 修改SquidLink对象集合
	 * 作用描述：
	 * 根据Project对象集合里SquidLink的ID进行修改
	 * 修改说明：
	 *@param info json字符串
	 *@param out 异常处理
	 *@return
	 */
	public List<InfoPacket> updateSquidLinks(String info,ReturnValue out){
		//Json序列化
		List<SquidLink> squidLinks = JsonUtil.toGsonList(info, SquidLink.class);
		if(squidLinks == null || squidLinks.size()==0){
			out.setMessageCode(MessageCode.ERR_SQUIDlINK_LIST_NULL);
			return null;
		}else{
			return squidLinkService.updateSquidLinks(squidLinks, out);
		}
	}

	/**
	 * 创建SquidLink，同时添加SquidJoin
	 * 作用描述：
	 * 序列化SquidLink集合对象
	 * 验证：
	 * 验证SquidLink集合对象是否有值
	 * 修改说明：
	 *@param info Json串
	 *@param out 异常输出
	 *@return
	 */
	public List<InfoPacket> createSquidLinksJoin(String info,ReturnValue out){
		logger.debug(String.format("createSquidLinksJoin-info=%s", info));
		List<InfoPacket> infoPackets = new ArrayList<InfoPacket>();
		//Json序列化
		List<SquidLink> squidLinks = JsonUtil.toGsonList(info, SquidLink.class);
		if(squidLinks == null || squidLinks.size()==0){
			out.setMessageCode(MessageCode.ERR_SQUIDlINK_LIST_NULL);
			logger.debug(String.format("createSquidLinksJoin-return=%s;ERR", out.getMessageCode()));
			return infoPackets;
		}else{
			infoPackets = squidLinkService.createSquidLinksAndJoin(squidLinks, out);
			logger.debug(String.format("createSquidLinksJoin-return=%s", out.getMessageCode()));
		}
		return infoPackets;
	}
}
