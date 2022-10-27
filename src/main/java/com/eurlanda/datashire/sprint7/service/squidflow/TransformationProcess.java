package com.eurlanda.datashire.sprint7.service.squidflow;

import com.eurlanda.datashire.entity.Transformation;
import com.eurlanda.datashire.enumeration.TransformationTypeEnum;
import com.eurlanda.datashire.sprint7.packet.InfoPacket;
import com.eurlanda.datashire.utility.JsonUtil;
import com.eurlanda.datashire.utility.MessageCode;
import com.eurlanda.datashire.utility.ReturnValue;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * TransformationProcess预处理类
 * TransformationProcess预处理类：对数据进行验证验证数据的完整性和正确性；并调用下层业务处理方法
 * Title :
 * Description:
 * Author :赵春花 2013-8-23
 * update :赵春花2013-8-23
 * Department : JAVA后端研发部
 * Copyright : ©2012-2013 悦岚（上海）数据服务有限公司 </p>
 */
public class TransformationProcess implements ITransformationProcess{
	static Logger logger = Logger.getLogger(TransformationProcess.class);// 记录日志
	private DataShirServiceplug plug;
	private TransformationService transformationService;
	private String token;
	public TransformationProcess(String token) {
		this.token=token;
		plug = new DataShirServiceplug(token);
		transformationService = new TransformationService(token);
	}

	/**
	 * 创建Transformation集合对象处理类
	 * 作用描述：
	 * 创建SquidFlow对象集合
	 * 成功根据Transformation对象集合里的key查询相对应的Id封装成InfoPacket对象集合返回
	 * 修改说明：
	 *@param info json字符串
	 *@param out 异常处理
	 *@return
	 */
	public List<InfoPacket> createTransformations(String info,ReturnValue out){
		logger.debug(String.format("createTransformations-info=%s", info));
		List<InfoPacket> infoPackets = new ArrayList<InfoPacket>();
		try {
			//序列化JSONArray
			List<Transformation> transformations =  JsonUtil.toGsonList(info, Transformation.class);
			//验证Transformation集合对象
			if(transformations==null||transformations.size()==0){
				//Transformation对象集合不能为空size不能等于0
				//异常返回
				out.setMessageCode(MessageCode.ERR_TRANSFORMATION_NULL);
				logger.debug(String.format("createTransformations-result-=%s", false));
				return infoPackets;
			}else{
				//验证
				for (Transformation transformation : transformations) {
					if(transformation.getSquid_id() == 0){
						out.setMessageCode(MessageCode.ERR_TRANSFORMATION_DATA_INCOMPLETE);
						return infoPackets;
					}
				}
				infoPackets = transformationService.createTransformations(transformations, out);
				logger.debug(String.format("createTransformations-return=%s", infoPackets));
				return infoPackets;
			}
		} catch (Exception e) {
			out.setMessageCode(MessageCode.ERR_ARRAYS);
		}
		return infoPackets;
	}
	
	/**
	 * 修改Transformation对象集合
	 * 作用描述：
	 * 根据Transformation对象集合里Transformation的ID修改数据
	 * 验证：
	 * 1、验证数据序列化后是否有值
	 * 2、检验Transformation对象必要字段值的正确性
	 * 修改说明：
	 *@param info json字符串
	 *@param out 异常处理
	 *@return
	 */
	public List<InfoPacket> updateTransformations(String info,ReturnValue out){
		logger.debug(String.format("updateTransformations-info=%s", info));
		List<InfoPacket> infoPackets = new ArrayList<InfoPacket>();
		//Json序列化
		List<Transformation> transformations = JsonUtil.toGsonList(info, Transformation.class);
		if(transformations == null || transformations.size() == 0){
			out.setMessageCode(MessageCode.ERR_TRANSFORMATION_NULL);
			return infoPackets;
		}
		//验证
		for (Transformation transformation : transformations) {
			if(transformation.getSquid_id()<=0 || transformation.getId() <= 0 || transformation.getKey() == null){
				out.setMessageCode(MessageCode.ERR_TRANSFORMATION_DATA_INCOMPLETE);
				return infoPackets;
			}
			if(transformation.getTranstype() == TransformationTypeEnum.VIRTUAL.value() && transformation.getColumn_id()<=0){
				out.setMessageCode(MessageCode.ERR_TRANSFORMATION_DATA_INCOMPLETE);
				return infoPackets;
			}
		}
		infoPackets= transformationService.updateTransformations(transformations, out);
		logger.debug(String.format("updateTransformations-return=%s", out.getMessageCode()));
		return infoPackets;
		
	}
	
}
