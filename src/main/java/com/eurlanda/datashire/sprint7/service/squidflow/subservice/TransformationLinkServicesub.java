package com.eurlanda.datashire.sprint7.service.squidflow.subservice;

import cn.com.jsoft.jframe.utils.ValidateUtils;
import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.entity.Transformation;
import com.eurlanda.datashire.entity.TransformationInputs;
import com.eurlanda.datashire.entity.TransformationLink;
import com.eurlanda.datashire.enumeration.TransformationTypeEnum;
import com.eurlanda.datashire.enumeration.datatype.SystemDatatype;
import com.eurlanda.datashire.sprint7.service.squidflow.AbstractRepositoryService;
import com.eurlanda.datashire.utility.DatabaseException;
import com.eurlanda.datashire.utility.MessageCode;
import com.eurlanda.datashire.utility.ReturnValue;
import org.apache.log4j.Logger;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
/**
 * 删除TransformationLink处理类
 * @author lei.bin
 *
 */
public class TransformationLinkServicesub extends AbstractRepositoryService implements ITransformationLinkService{
	Logger logger = Logger.getLogger(TransformationLinkServicesub.class);// 记录日志
	public TransformationLinkServicesub(String token) {
		super(token);
	}
	public TransformationLinkServicesub(IRelationalDataManager adapter){
		super(adapter);
	}
	public TransformationLinkServicesub(String token, IRelationalDataManager adapter){
		super(token, adapter);
	}
  
	/**
	 * 删除TransformationLink，同时更改引用列is_referenced='N'
	 * @param TransformationLink_id
	 * @return
	 */
	public boolean deleteTransLink(int TransformationLink_id,ReturnValue out){
		Map<String, String> paramMap = new HashMap<String, String>();
		try{
			// 1. 更改引用列is_referenced='N'
			paramMap.put("id", String.valueOf(TransformationLink_id));
			List<TransformationLink> transformationLinks=adapter.query2List(true, paramMap, TransformationLink.class);
			
			if(transformationLinks!=null && transformationLinks.size()>0){
				int from_transformation_id = transformationLinks.get(0).getFrom_transformation_id();
				updateReferenceColumn(from_transformation_id, false);
				
				int to_transformation_id = transformationLinks.get(0).getTo_transformation_id();
				resetTransformationInput(from_transformation_id, to_transformation_id);
			}
			// 2. 删除TransformationLink
			paramMap.clear();
			paramMap.put("id", Integer.toString(TransformationLink_id, 10));
			int cnt = adapter.delete(paramMap, TransformationLink.class);
			logger.debug(cnt+"[id="+TransformationLink_id+"] TransformationLink(s) removed!");
		} catch (Exception e) {
			logger.error("[删除deleteTransLink=========================================exception]", e);
			try {
				adapter.rollback();
			} catch (SQLException e1) {
				logger.error("rollback err!", e1);
			}
			out.setMessageCode(MessageCode.SQL_ERROR);
			return false;
		} 
		return true;
	}
	/**
	 * 更新引用列
	 * @param from_transformation_id
	 * @param is_referenced
	 * @return
	 * @throws SQLException
	 */
	private boolean updateReferenceColumn(int from_transformation_id, boolean is_referenced) throws DatabaseException{
		String sql = "UPDATE DS_REFERENCE_COLUMN SET is_referenced='"+(is_referenced?'Y':'N')+"'";
		sql += " WHERE column_id IN (SELECT column_id FROM DS_TRANSFORMATION WHERE ID="+from_transformation_id+" AND column_id IS NOT NULL)";
		return adapter.execute(sql)>0?true:false;
	}
	
	//根据link中的transid 进行重置和删除功能。
	private void resetTransformationInput(int from_transformation_id, int to_transformation_id) throws SQLException{
		Map<String, String> paramMap = new HashMap<String, String>();
		paramMap.put("transformation_id", String.valueOf(to_transformation_id));
		paramMap.put("source_transform_id", String.valueOf(from_transformation_id));
		List<TransformationInputs> transInputs = adapter.query2List(true, paramMap, TransformationInputs.class);
		if (null != transInputs && transInputs.size() > 0) {
			boolean isUpdate = false;
			StringBuffer sql = new StringBuffer();
			sql.append("select max(dtid.input_order) as CNT from ds_tran_input_definition dtid ");
			sql.append("inner join ds_transformation_type dtt on dtid.code=dtt.code ");
			sql.append("inner join ds_transformation dt on dtt.id=dt.transformation_type_id ");
			sql.append(" where dtid.input_order not in (-1) and dt.id="+ to_transformation_id);
			Map<String, Object> map = adapter.query2Object(true, 
					sql.toString() , null);
			//验证，是否还有空余的链接数
			int cnt = 0;
			if(map!=null&&map.containsKey("CNT")&&
					ValidateUtils.isNumeric(map.get("CNT")+"")){
				cnt =  Integer.parseInt(map.get("CNT")+"") + 1;
				if (cnt<9999){
					isUpdate = true;
				}
			}
			for (TransformationInputs transformationInput : transInputs) {
				if (isUpdate){
					transformationInput.setIn_condition("");
					transformationInput.setSource_tran_output_index(0);
					transformationInput.setSource_transform_id(0);
					adapter.update2(transformationInput);
				}else{
					paramMap.clear();
					paramMap.put("id", String.valueOf(transformationInput.getId()));
					adapter.delete(paramMap, TransformationInputs.class);
				}
				
				paramMap.clear();
				paramMap.put("id", String.valueOf(to_transformation_id));
				Transformation toTrans = adapter.query2Object2(true, paramMap, Transformation.class);
				if (toTrans!=null){
					int typeid = toTrans.getTranstype();
					if (typeid==TransformationTypeEnum.CHOICE.value()){
						String sqlstr =  "select * from ds_transformation_link " +
								"where from_transformation_id="+transformationInput.getTransformationId();
						List<TransformationLink> links =  adapter.query2List(true, sqlstr, null, TransformationLink.class);
						if (links==null||links.size()==0){
							paramMap.clear();
							paramMap.put("id", String.valueOf(transformationInput.getTransformationId()));
							Transformation trans = adapter.query2Object2(true, paramMap, Transformation.class);
							trans.setOutput_data_type(SystemDatatype.OBJECT.value());
							adapter.update2(trans);
						}
					}
				}
			}
		}
	}
}
