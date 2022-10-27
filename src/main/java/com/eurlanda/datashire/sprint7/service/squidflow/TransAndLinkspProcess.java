package com.eurlanda.datashire.sprint7.service.squidflow;

import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.entity.MessageBubble;
import com.eurlanda.datashire.entity.SpecialTransformationAndLinks;
import com.eurlanda.datashire.entity.Transformation;
import com.eurlanda.datashire.entity.TransformationLink;
import com.eurlanda.datashire.enumeration.MessageBubbleCode;
import com.eurlanda.datashire.sprint7.packet.PushMessagePacket;
import com.eurlanda.datashire.utility.JsonUtil;
import com.eurlanda.datashire.utility.MessageCode;
import com.eurlanda.datashire.utility.ReturnValue;
import com.eurlanda.datashire.utility.StringUtils;
import org.apache.log4j.Logger;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TransAndLinkspProcess extends AbstractRepositoryService implements ITransAndLinkspProcess{
	//记录日志
	static Logger logger = Logger.getLogger(TransAndLinkspProcess.class);
	public TransAndLinkspProcess(String token) {
		super(token);
	}
	public TransAndLinkspProcess(IRelationalDataManager adapter){
		super(adapter);
	}
	public TransAndLinkspProcess(String token, IRelationalDataManager adapter){
		super(token, adapter);
	}

	/**
	 * 拖动transformation打断transformationlink业务处理类
	 * 
	 * @return
	 */
	public List<SpecialTransformationAndLinks> drapTransformationAndLink(
			String info, ReturnValue out) {
		int TransformationId=0;//transformation的id
		//返回信息
		List<SpecialTransformationAndLinks> output=new ArrayList<SpecialTransformationAndLinks>();
		try {
			// 序列化前端传送对象
			List<SpecialTransformationAndLinks> specialTransformationAndLinks = JsonUtil
					.toGsonList(info, SpecialTransformationAndLinks.class);
			logger.debug("[获取specialTransformationAndLinks的长度==]"
					+ specialTransformationAndLinks.size());
			adapter.openSession();
			for (SpecialTransformationAndLinks transformationAndLinks : specialTransformationAndLinks) {
				// 先记录传过来links的from_transformation_id 和to_transformation_id
				int linkId = transformationAndLinks.getTransformationLinks().get(0).getId();
				Map<String, String> paramMap = new HashMap<String, String>();
				paramMap.put("id", String.valueOf(linkId));
				List<TransformationLink> transformationLinks = adapter.query2List(true, paramMap, TransformationLink.class);
				if (transformationLinks==null||transformationLinks.size()==0){
					out.setMessageCode(MessageCode.NODATA);
					return null;
				}
				int from_transformation_id = transformationLinks.get(0).getFrom_transformation_id();
				int to_transformation_id = transformationLinks.get(0).getTo_transformation_id();
				int source_output_index = 0;
				Map<String, Integer> indexMap = new HashMap<String, Integer>();
				// 然后删除link
				if (transformationLinks!=null&&transformationLinks.size()>0){
					DataShirServiceplug plug = new DataShirServiceplug(token);
					plug.resetTransformationInput(adapter, from_transformation_id, to_transformation_id, indexMap);
					if (indexMap.containsKey("index")){
						source_output_index = indexMap.get("index");
					}
				}
				int executResult = adapter.delete(paramMap,TransformationLink.class);
				// 如果传过来的transformation有id,则不需要进行新增
				if (0 != transformationAndLinks.getTransformation().getId()) {
					TransformationId = transformationAndLinks.getTransformation().getId();
					//消除孤立transformation消息泡
					logger.debug("推送消除孤立transformation消息泡");
					PushMessagePacket.push(
							new MessageBubble(transformationAndLinks.getTransformation().getKey(), transformationAndLinks.getTransformation().getKey(), MessageBubbleCode.WARN_TRANSFORMATION_NO_LINK.value(), true), 
							token);
				} else {
					// 新增transformation(返回id)
					TransformationId = adapter.insert2(transformationAndLinks.getTransformation());
				}
				TransformationLink transformationLinkFrom = new TransformationLink();
				TransformationLink transformationLinkTo = new TransformationLink();
				// 给对象赋值
				this.setTransfromation(transformationLinkFrom, from_transformation_id, TransformationId);
				this.setTransfromation(transformationLinkTo, TransformationId, to_transformation_id);
				// 创建新的links
				int fromLinkId = adapter.insert2(transformationLinkFrom);
				transformationLinkFrom.setId(fromLinkId);
				int toLinkId = adapter.insert2(transformationLinkTo);
				transformationLinkTo.setId(toLinkId);
				// 封装结果集返回给前端
				SpecialTransformationAndLinks links=new SpecialTransformationAndLinks();
				Transformation transformationOutput=new Transformation();//transformation对象
				transformationOutput.setId(TransformationId);
				transformationOutput.setKey(transformationAndLinks.getTransformation().getKey());
				links.setTransformation(transformationOutput);
				List<TransformationLink> transformationLinksOutput=new ArrayList<TransformationLink>();//transformationlink对象
				transformationLinksOutput.add(transformationLinkFrom);
				transformationLinksOutput.add(transformationLinkTo);
				links.setTransformationLinks(transformationLinksOutput);
				output.add(links);
			}
		} catch (SQLException e) {
			try {
				adapter.rollback();
			} catch (SQLException e1) {
				logger.error("[drap sql is error]" + e1);
				e1.printStackTrace();
			}
			logger.error("[drapTransformationAndLink is error]" + e);
		} finally {
			adapter.closeSession();
		}
		return output;
	}

	/**
	 * 对transformationLink对象的赋值
	 * 
	 * @param transformationLink
	 * @param from_id
	 * @param to_id
	 */
	public void setTransfromation(TransformationLink transformationLink,
			int from_id, int to_id) {
		transformationLink.setFrom_transformation_id(from_id);
		transformationLink.setTo_transformation_id(to_id);
		transformationLink.setKey(StringUtils.generateGUID());
		transformationLink.setIn_order(1);
	}
}
