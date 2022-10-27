package com.eurlanda.datashire.sprint7.service.squidflow;

import com.eurlanda.datashire.entity.TransformationLink;
import com.eurlanda.datashire.entity.operation.DataEntity;
import com.eurlanda.datashire.entity.operation.WhereCondition;
import com.eurlanda.datashire.enumeration.DSObjectType;
import com.eurlanda.datashire.enumeration.DataStatusEnum;
import com.eurlanda.datashire.enumeration.MatchTypeEnum;
import com.eurlanda.datashire.enumeration.SystemTableEnum;
import com.eurlanda.datashire.sprint7.packet.InfoPacket;
import com.eurlanda.datashire.sprint7.plug.GetParam;
import com.eurlanda.datashire.sprint7.plug.SupportPlug;
import com.eurlanda.datashire.sprint7.plug.TransformationLinkPlug;
import com.eurlanda.datashire.utility.MessageCode;
import com.eurlanda.datashire.utility.ReturnValue;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * TransformationLink业务处理类
 * 
 * Title : 
 * Description: 
 * Author :赵春花 2013-10-28
 * update :赵春花 2013-10-28
 * Department :  JAVA后端研发部
 * Copyright : ©2012-2013 悦岚（上海）数据服务有限公司
 */
public class TransformationLinkService extends SupportPlug implements ITransformationLinkService{
	static Logger logger = Logger.getLogger(TransformationLinkService.class);// 记录日志
	private String token;
	public TransformationLinkService(String token) {
		super(token);
		this.token = token;
	}
	TransformationLinkPlug transformationLinkPlug = new TransformationLinkPlug(
			adapter);
	/**
	 * 创建TransformationLink对象集合
	 * 作用描述：
	 * 根据传入的TransformationLink对象集合创建TransformationLink对象
	 * 创建成功——根据TransformationLink集合对象里的Key查询相对应的ID
	 * 验证：
	 * 1、 TransformationLink对象集合不能为空且必须有数据
	 * 2、 TransformationLink对象集合里的每个Key必须有值
	 * 修改说明：
	 *@param transformationLinks transformationLink集合对象
	 *@param out 异常返回
	 *@return
	 */
	public List<InfoPacket> createTransformationLinks(
			List<TransformationLink> transformationLinks, ReturnValue out) {
		logger.debug(String.format(
				"createTransformationLinks-TransfromationLinkList.size()=%s",
				transformationLinks.size()));
		// 定义接收返回结果集
		List<InfoPacket> infoPackets = new ArrayList<InfoPacket>();
		boolean create = false;
		try {
			create = transformationLinkPlug.createTransformationLinks(
					transformationLinks, out);
			// 新增成功
			if (create) {
				// 根据Key查询出新增的ID
				for (TransformationLink transformationLink : transformationLinks) {
					// 获得Key
					String key = transformationLink.getKey();
					// 根据Key获得ID
					int id = transformationLinkPlug.getTransformationLinkId(
							key, out);
					InfoPacket infoPacket = new InfoPacket();
					infoPacket.setId(id);
					infoPacket.setKey(key);
					infoPacket.setType(DSObjectType.TRANSFORMATIONLINK);
					infoPackets.add(infoPacket);
				}
			}
			logger.debug(String.format("createTransformationLinks-return=%s",
					infoPackets));
		} catch (Exception e) {
			logger.error("createTransformationLinks-return=%s", e);
			out.setMessageCode(MessageCode.ERR_ARRAYS);
		} finally {
			// 释放连接
			adapter.commitAdapter();
		}

		return infoPackets;

	}
	

	/**
	 * 
	 * 作用描述：
	 * 修改TransformationLink对象集合
	 * 调用方法前确保TransformationLink对象集合不为空，并且TransformationLink对象集合里的每个TransformationLink对象的Id不为空
	 * 修改说明：
	 *@param transformationLinks transformationLink集合对象
	 *@param out 异常返回
	 *@return
	 */
	public List<InfoPacket> updateTransformationLinks(List<TransformationLink> transformationLinks,ReturnValue out){
		logger.debug(String.format("updateTransformationLinks-transformationLinkList.size()=%s", transformationLinks.size()));
		boolean create = false;
		List<InfoPacket> infoPackets = new ArrayList<InfoPacket>();
		try {
			//封装批量新增数据集
			List<List<DataEntity>>  paramList = new ArrayList<List<DataEntity>>();
			//封装条件集合
			List<List<WhereCondition>>  whereCondList = new ArrayList<List<WhereCondition>>();
			//调用转换类
			GetParam getParam = new GetParam();
			for (TransformationLink transformationLink: transformationLinks) {
				//参数
				List<DataEntity> dataEntitys =  new ArrayList<DataEntity>();
				getParam.getTransformationLink(transformationLink, dataEntitys);
				paramList.add(dataEntitys);
				//条件
				List<WhereCondition> whereConditions = new ArrayList<WhereCondition>();
				WhereCondition whereCondition = new WhereCondition();
				whereCondition.setAttributeName("ID");
				whereCondition.setDataType(DataStatusEnum.INT);
				whereCondition.setMatchType(MatchTypeEnum.EQ);
				whereCondition.setValue(transformationLink.getId());
				
				whereConditions.add(whereCondition);
				
				whereCondList.add(whereConditions);
				
			}
			//调用批量新增
			create = adapter.updatBatch(SystemTableEnum.DS_TRANSFORMATION_LINK.toString(), paramList, whereCondList, out);
			logger.debug(String.format("updateTransformationLinks-return=%s",create ));
			//查询返回结果
			if(create){
				TransformationLinkPlug transformationLinkPlug = new TransformationLinkPlug(adapter);
				//定义接收返回结果集
				//根据Key查询出新增的ID
				for (TransformationLink transformationLink : transformationLinks) {
					//获得Key
					String key = transformationLink.getKey();
					int id = transformationLinkPlug.getTransformationLinkId(key, out);
					InfoPacket infoPacket = new InfoPacket();
					infoPacket.setCode(1);
					infoPacket.setId(id);
					infoPacket.setType(DSObjectType.TRANSFORMATIONLINK);
					infoPacket.setKey(key);
					infoPackets.add(infoPacket);
				}
			}
		} catch (Exception e) {
			logger.error("updateTransformationLinks-return=%s",e);
			out.setMessageCode(MessageCode.ERR_ARRAYS);
		}finally{
			//释放
			adapter.commitAdapter();
		}
		return infoPackets;
	}
}
