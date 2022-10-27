package com.eurlanda.datashire.sprint7.service.squidflow.subservice;

import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.entity.Transformation;
import com.eurlanda.datashire.entity.TransformationInputs;
import com.eurlanda.datashire.entity.TransformationLink;
import com.eurlanda.datashire.sprint7.service.squidflow.AbstractRepositoryService;
import com.eurlanda.datashire.utility.MessageCode;
import com.eurlanda.datashire.utility.ReturnValue;
import org.apache.log4j.Logger;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
/**
 * 删除Transformation业务处理类
 * @author lei.bin
 *
 */
public class TransformationServicesub extends AbstractRepositoryService implements ITransformationService {
	Logger logger = Logger.getLogger(TransformationServicesub.class);// 记录日志
	public TransformationServicesub(String token) {
		super(token);
	}
	public TransformationServicesub(IRelationalDataManager adapter){
		super(adapter);
	}
	public TransformationServicesub(String token, IRelationalDataManager adapter){
		super(token, adapter);
	}

	/**
	 * 根据transformation_id删除transformation以及关联表信息
	 * 
	 * @param trans_id
	 * @param out
	 * @return
	 */
	public boolean deleteTransformation(int trans_id, ReturnValue out) {
		boolean delete = true;
		Map<String, String> paramMap = new HashMap<String, String>();
		try {
			paramMap.clear();
			paramMap.put("id", String.valueOf(trans_id));
			Transformation trans = adapter.query2Object2(true, paramMap, Transformation.class);
			if (trans==null){
				return false;
			}
			// 根据transformation_id 查询trans_input集合  放在Link之前删除（如果只是删除link，可以进行重置）
			String queryInputSql = "select * from DS_TRAN_INPUTS where transformation_id=" + trans_id;
			List<TransformationInputs> transInputs = adapter.query2List(true, queryInputSql, null, TransformationInputs.class);
			if (null != transInputs && transInputs.size() > 0) {
				/*for (TransformationInputs transformationInput : transInputs) {
					paramMap.clear();
					paramMap.put("id", String.valueOf(transformationInput.getId()));
					delete = adapter.delete(paramMap, TransformationInputs.class) > 0 ? true : false;
				}*/
				paramMap.clear();
				paramMap.put("transformation_id", String.valueOf(trans_id));
				delete = adapter.delete(paramMap, TransformationInputs.class) > 0 ? true : false;
			}
			// 根据transformation_id 查询transformationlink集合
			String queryTranSql = "select * from DS_TRANSFORMATION_LINK where from_transformation_id="
					+ trans_id + " or to_transformation_id=" + trans_id + "";
			List<TransformationLink> transformationLinks = adapter.query2List(true, queryTranSql, null, TransformationLink.class);
			if (null != transformationLinks && transformationLinks.size() > 0) {
				TransformationLinkServicesub transformationLinkService = new TransformationLinkServicesub(token, adapter);
				for (TransformationLink transformationLink : transformationLinks) {
					// 删除link
					delete = transformationLinkService.deleteTransLink(
							transformationLink.getId(), out);
				}
			}
			// 删除transformation
			if (delete) {
				paramMap.clear();
				paramMap.put("id", String.valueOf(trans_id));
				return adapter.delete(paramMap, Transformation.class) > 0 ? true : false;
			}
		} catch (Exception e) {
			logger.error("[删除Transformation=========================================exception]",e);
			try {
				adapter.rollback();
			} catch (SQLException e1) {
				logger.error("rollback err!", e1);
			}
			out.setMessageCode(MessageCode.SQL_ERROR);
			return false;
		}
		return delete;
	}
	
	   /**
     * 根据transformation_id删除transformation以及关联表信息
     * 
     * @param trans_id
     * @param out
     * @return
     */
    public Map<String, Object> delTransformation(int trans_id, Boolean delTransfLinkFlag, ReturnValue out) {
        boolean delFlag = false;
        List<Integer> toTransformationIdList = new ArrayList<Integer>();
        Map<String, String> paramMap = new HashMap<String, String>();
        try {
            
            // 根据transformation_id 查询trans_input集合  放在Link之前删除（如果只是删除link，可以进行重置）
            String queryInputSql = "select * from DS_TRAN_INPUTS where transformation_id="
                    + trans_id + " or source_transform_id=" + trans_id + "";
            List<TransformationInputs> transInputs = adapter.query2List(true, queryInputSql, null, TransformationInputs.class);
            if (null != transInputs && transInputs.size() > 0) {
                for (TransformationInputs transformationInput : transInputs) {
                    paramMap.clear();
                    paramMap.put("id", String.valueOf(transformationInput.getId()));
                    delFlag = adapter.delete(paramMap, TransformationInputs.class) > 0 ? true : false;
                }
            }
            // 是否需要删除TransformationLink
            if(delTransfLinkFlag){
            // 根据transformation_id 查询transformationlink集合
            String queryTranSql = "select * from DS_TRANSFORMATION_LINK where from_transformation_id="
                    + trans_id + " or to_transformation_id=" + trans_id + "";
            List<TransformationLink> transformationLinks = adapter.query2List(true, queryTranSql, null, TransformationLink.class);
            if (null != transformationLinks && transformationLinks.size() > 0) {
                TransformationLinkServicesub transformationLinkService = new TransformationLinkServicesub(token, adapter);
                for (TransformationLink transformationLink : transformationLinks) {
                    toTransformationIdList.add(transformationLink.getTo_transformation_id());
                    // 删除link
                    delFlag = transformationLinkService.deleteTransLink(
                            transformationLink.getId(), out);
                }
            }
            
            }
            // 删除transformation
            if (delFlag) {
                paramMap.clear();
                paramMap.put("id", String.valueOf(trans_id));
                if(adapter.delete(paramMap, Transformation.class) > 0 ){
                    delFlag = true;
                }
            }
        } catch (Exception e) {
            logger.error("[删除Transformation=========================================exception]",e);
            try {
                adapter.rollback();
            } catch (SQLException e1) {
                logger.error("rollback err!", e1);
            }
            out.setMessageCode(MessageCode.SQL_ERROR);
            delFlag = false;
        }
        Map<String, Object> resultMap = new HashMap<String, Object>();
        resultMap.put("delFlag", delFlag);
        resultMap.put("toTransformationIdList", toTransformationIdList);
        return resultMap;
    }
}
