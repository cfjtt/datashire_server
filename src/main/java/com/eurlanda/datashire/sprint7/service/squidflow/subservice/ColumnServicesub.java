package com.eurlanda.datashire.sprint7.service.squidflow.subservice;

import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.entity.Column;
import com.eurlanda.datashire.entity.MessageBubble;
import com.eurlanda.datashire.entity.ReferenceColumn;
import com.eurlanda.datashire.entity.Squid;
import com.eurlanda.datashire.entity.SquidLink;
import com.eurlanda.datashire.entity.Transformation;
import com.eurlanda.datashire.enumeration.MessageBubbleCode;
import com.eurlanda.datashire.enumeration.SquidTypeEnum;
import com.eurlanda.datashire.sprint7.service.squidflow.AbstractRepositoryService;
import com.eurlanda.datashire.utility.MessageCode;
import com.eurlanda.datashire.utility.ReturnValue;
import com.eurlanda.datashire.utility.StringUtils;
import org.apache.log4j.Logger;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
/**
 * 删除Column处理类
 * @author lei.bin
 *
 */
public class ColumnServicesub extends AbstractRepositoryService implements IColumnService{
	Logger logger = Logger.getLogger(ColumnServicesub.class);// 记录日志
	public ColumnServicesub(String token) {
		super(token);
	}
	public ColumnServicesub(IRelationalDataManager adapter){
		super(adapter);
	}
	public ColumnServicesub(String token, IRelationalDataManager adapter){
		super(token, adapter);
	}

	/**
	 * 根据id删除column
	 * 
	 * @param id
	 * @param out
	 * @return
	 */
	public boolean deleteColumn(int id, ReturnValue out) {
		boolean delete = true;
		Map<String, String> paramMap = new HashMap<String, String>();
		try {
            //根据column_id查询出transformation集合
			paramMap.put("column_id", String.valueOf(id));
			List<Transformation> transformations = adapter.query2List(true,paramMap,Transformation.class);
			if(null!=transformations&&transformations.size()>0)
			{
				TransformationServicesub transformationService=new TransformationServicesub(token, adapter);
				//删除transformation
				for(Transformation transformation:transformations)
				{
					delete=transformationService.deleteTransformation(transformation.getId(), out);
				}
			}
			if (delete) {
				// 删除column
				paramMap.clear();
				paramMap.put("id", String.valueOf(id));
				return adapter.delete(paramMap, Column.class)>0?true:false;
			}
		} catch (Exception e) {
			logger.error("[删除deleteColumn=========================================exception]", e);
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
     * 根据id删除column
     * 
     * @param id
     * @param out
     * @return
     */
    public boolean delColumn(int id, ReturnValue out) {
        boolean delete = false;
        Map<String, String> paramMap = new HashMap<String, String>();
        try {
            adapter.openSession();
            //根据column_id查询出transformation集合
            paramMap.put("column_id", String.valueOf(id));
            List<Transformation> transformations = adapter.query2List(true,paramMap,Transformation.class);
            TransformationServicesub transformationService = new TransformationServicesub(token, adapter);
            if(null!=transformations&&transformations.size()>0)
            {
                //删除transformation
                for(Transformation transformation:transformations)
                {
                    delete=transformationService.deleteTransformation(transformation.getId(), out);
                }
            }
            paramMap.clear();
            paramMap.put("id", String.valueOf(id));
            Column column = adapter.query2Object2(true,paramMap, Column.class);
            int squidId = column.getSquid_id();
            paramMap.clear();
            paramMap.put("from_squid_id", Integer.toString(squidId, 10));
            List<SquidLink> squidLinkList = adapter.query2List(true, paramMap, SquidLink.class);
            SquidLink squidLink = null;
            int toSquidId = 0;
            Squid squid = null;
//            ReferenceColumnService referenceColumnService = new ReferenceColumnService(token);
            Transformation transformation = null;
            int squidType = 0;
            int toTransformationId = 0;
            int columnId = 0;
            //adapter.openSession();
            // 检索当前Squid是否有下游Squid,如果有下游Squid，那么需要更新下游所有的column, transformation
            if(StringUtils.isNotNull(squidLinkList) && !squidLinkList.isEmpty()){
                for(int j=0; j<squidLinkList.size(); j++){
                    squidLink = squidLinkList.get(j);
                    toSquidId = squidLink.getTo_squid_id();
                    paramMap.clear();
                    paramMap.put("id", Integer.toString(toSquidId, 10));
                    squid = adapter.query2Object(paramMap, Squid.class);
                    squidType = squid.getSquid_type();
                    if(StringUtils.isNull(squid)){
                        continue;
                    }
                    // 如果是ReportSquid的话，需要生成错误的消息泡
                    if(SquidTypeEnum.REPORT.value() == squidType){
/*                        List<MessageBubble> messageBubbleList = new ArrayList<MessageBubble>();
                        messageBubbleList.add(new MessageBubble(toSquidId, toSquidId, squid.getName(), MessageBubbleCode.WARN_REPORT_SQUID_COLUMNS_BY_DELETED.value(), false, MessageBubbleCode.WARN, "报表squid的源数据被删除，可能导致报表运行不正常"));
                        PushMessagePacket.push(messageBubbleList, token);*/
                        //推送消息泡
                        new MessageBubbleService(token).pushMessageBubble(new MessageBubble(false, toSquidId, toSquidId, squid.getName(), MessageBubbleCode.WARN_REPORT_SQUID_COLUMNS_BY_DELETED.value(), MessageBubbleCode.WARN, "报表squid的源数据被删除，可能导致报表运行不正常"));
                        continue;
                    }
                    else {
                        // 删除ReferenceColumn
                        paramMap.clear();
                        paramMap.put("column_id", String.valueOf(id));
                        paramMap.put("reference_squid_id", Integer.toString(toSquidId, 10));
                        delete = adapter.delete(paramMap, ReferenceColumn.class)>0?true:false;
                        //referenceColumnService.deleteReferenceColumn(id, out);
                        //adapter.commit();
                        
                        paramMap.clear();
                        paramMap.put("column_id", String.valueOf(id));
                        paramMap.put("squid_id", Integer.toString(toSquidId, 10));
                        transformation = adapter.query2Object2(true, paramMap, Transformation.class);
                        if(StringUtils.isNull(transformation)){
                            continue;
                        }
                        // 同时删除ReferenceColumn对应的虚拟的Transformation、TransformationLink、TransformationInputs
                        Map<String, Object> resultMap = transformationService.delTransformation(transformation.getId(), true, out);
                        
                        delete = (Boolean) resultMap.get("delFlag");
                        List<Integer> toTransformationIdList = (List<Integer>) resultMap.get("toTransformationIdList");
                        // 如果下游是Exception SquidModelBase，那么还需要删除对应的Squid Column（转换器面板上左边Column）
                        if(SquidTypeEnum.EXCEPTION.value() == squidType){
                            if(StringUtils.isNotNull(toTransformationIdList) && toTransformationIdList.isEmpty()){
                                for(int i=0; i<toTransformationIdList.size(); i++){
                                    toTransformationId = toTransformationIdList.get(i);
                                    paramMap.clear();
                                    paramMap.put("id", Integer.toString(toTransformationId, 10));
                                    transformation = adapter.query2Object2(true, paramMap, Transformation.class);
                                    columnId = transformation.getColumn_id();
                                    paramMap.clear();
                                    paramMap.put("id", Integer.toString(columnId, 10));
                                    // 删除对应Squid Column
                                    adapter.delete(paramMap, Column.class);
                                    // 删除对应Squid Column的虚拟Transformation、TransformationInputs
                                    transformationService.delTransformation(transformation.getId(), false, out);
                                }
                            }
                        }
                    }
                }
            }
            if (delete) {
                // 删除column
                paramMap.clear();
                paramMap.put("id", String.valueOf(id));
                return adapter.delete(paramMap, Column.class)>0?true:false;
            }
        } catch (Exception e) {
            logger.error("[删除deleteColumn=========================================exception]", e);
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
     * 根据id删除column
     * 
     * @param id
     * @param out
     * @return
     */
    public boolean delColumn(IRelationalDataManager adapter3, int columnId, int squidId, ReturnValue out) {
        boolean flag = true;
        String selectSql = "select *";
        String deleteSql = "delete ";
        String sql = " from ds_column where 1=1";
        if (columnId>0){
            sql = sql + " and id="+columnId;
        } else if (squidId>0){
            sql = sql + " and squid_id="+squidId;
        }
        List<Column> columnLists = adapter3.query2List(true, selectSql+sql, null, Column.class);
        try {
        if (columnLists!=null&&columnLists.size()>0){
            flag = adapter3.execute(deleteSql+sql)>=0?true:false;
        }
        if(squidId > 0){
            return flag;
        }
        //boolean delete = false;
        Map<String, String> paramMap = new HashMap<String, String>();
            adapter.openSession();
/*            //根据column_id查询出transformation集合
            paramMap.put("column_id", String.valueOf(columnId));
            List<Transformation> transformations = adapter.query2List(true,paramMap,Transformation.class);
            TransformationServicesub transformationService = new TransformationServicesub(token, adapter);
            if(null!=transformations&&transformations.size()>0)
            {
                //删除transformation
                for(Transformation transformation:transformations)
                {
                    delete=transformationService.deleteTransformation(transformation.getId(), out);
                }
            }*/
            paramMap.clear();
            paramMap.put("id", String.valueOf(columnId));
            Column column = adapter.query2Object2(true,paramMap, Column.class);
            squidId = column.getSquid_id();
            paramMap.clear();
            paramMap.put("from_squid_id", Integer.toString(squidId, 10));
            List<SquidLink> squidLinkList = adapter.query2List(true, paramMap, SquidLink.class);
            SquidLink squidLink = null;
            int toSquidId = 0;
            Squid squid = null;
//            ReferenceColumnService referenceColumnService = new ReferenceColumnService(token);
            Transformation transformation = null;
            int squidType = 0;
            int toTransformationId = 0;
            //int columnId = 0;
            //adapter.openSession();
            // 检索当前Squid是否有下游Squid,如果有下游Squid，那么需要更新下游所有的column, transformation
            if(StringUtils.isNotNull(squidLinkList) && !squidLinkList.isEmpty()){
                TransformationServicesub transformationService = new TransformationServicesub(token, adapter);
                for(int j=0; j<squidLinkList.size(); j++){
                    squidLink = squidLinkList.get(j);
                    toSquidId = squidLink.getTo_squid_id();
                    paramMap.clear();
                    paramMap.put("id", Integer.toString(toSquidId, 10));
                    squid = adapter.query2Object(paramMap, Squid.class);
                    squidType = squid.getSquid_type();
                    if(StringUtils.isNull(squid)){
                        continue;
                    }
                    // 如果是ReportSquid的话，需要生成错误的消息泡
                    if(SquidTypeEnum.REPORT.value() == squidType){
/*                        List<MessageBubble> messageBubbleList = new ArrayList<MessageBubble>();
                        messageBubbleList.add(new MessageBubble(toSquidId, toSquidId, squid.getName(), MessageBubbleCode.WARN_REPORT_SQUID_COLUMNS_BY_DELETED.value(), false, MessageBubbleCode.WARN, "报表squid的源数据被删除，可能导致报表运行不正常"));
                        PushMessagePacket.push(messageBubbleList, token);*/
                        //推送消息泡
                        new MessageBubbleService(token).pushMessageBubble(new MessageBubble(false, toSquidId, toSquidId, squid.getName(), MessageBubbleCode.WARN_REPORT_SQUID_COLUMNS_BY_DELETED.value(), MessageBubbleCode.WARN, "报表squid的源数据被删除，可能导致报表运行不正常"));
                        continue;
                    }
                    else {
                        // 删除ReferenceColumn
                        paramMap.clear();
                        paramMap.put("column_id", String.valueOf(columnId));
                        paramMap.put("reference_squid_id", Integer.toString(toSquidId, 10));
                        flag = adapter.delete(paramMap, ReferenceColumn.class)>0?true:false;
                        //referenceColumnService.deleteReferenceColumn(id, out);
                        //adapter.commit();
                        
                        paramMap.clear();
                        paramMap.put("column_id", String.valueOf(columnId));
                        paramMap.put("squid_id", Integer.toString(toSquidId, 10));
                        transformation = adapter.query2Object2(true, paramMap, Transformation.class);
                        if(StringUtils.isNull(transformation)){
                            continue;
                        }
                        
                        // 同时删除ReferenceColumn对应的虚拟的Transformation、TransformationLink、TransformationInputs
                        Map<String, Object> resultMap = transformationService.delTransformation(transformation.getId(), true, out);
                        
                        flag = (Boolean) resultMap.get("delFlag");
                        List<Integer> toTransformationIdList = (List<Integer>) resultMap.get("toTransformationIdList");
                        // 如果下游是Exception SquidModelBase，那么还需要删除对应的Squid Column（转换器面板上左边Column）
                        if(SquidTypeEnum.EXCEPTION.value() == squidType){
                            if(StringUtils.isNotNull(toTransformationIdList) && toTransformationIdList.isEmpty()){
                                for(int i=0; i<toTransformationIdList.size(); i++){
                                    toTransformationId = toTransformationIdList.get(i);
                                    paramMap.clear();
                                    paramMap.put("id", Integer.toString(toTransformationId, 10));
                                    transformation = adapter.query2Object2(true, paramMap, Transformation.class);
                                    columnId = transformation.getColumn_id();
                                    paramMap.clear();
                                    paramMap.put("id", Integer.toString(columnId, 10));
                                    // 删除对应Squid Column
                                    adapter.delete(paramMap, Column.class);
                                    // 删除对应Squid Column的虚拟Transformation、TransformationInputs
                                    transformationService.delTransformation(transformation.getId(), false, out);
                                }
                            }
                        }
                    }
                }
            }
/*            if (flag) {
                // 删除column
                paramMap.clear();
                paramMap.put("id", String.valueOf(columnId));
                return adapter.delete(paramMap, Column.class)>0?true:false;
            }*/
        } catch (Exception e) {
            logger.error("[删除deleteColumn=========================================exception]", e);
            try {
                adapter.rollback();
            } catch (SQLException e1) {
                logger.error("rollback err!", e1);
            }
            out.setMessageCode(MessageCode.SQL_ERROR);
            return false;
        }
        return flag;
    }
}
