package com.eurlanda.datashire.sprint7.service.squidflow.subservice;

import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.entity.ReferenceColumn;
import com.eurlanda.datashire.entity.ReferenceColumnGroup;
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
 * 删除ReferenceGroup处理类
 * @author lei.bin
 *
 */
public class ReferenceGroupService extends AbstractRepositoryService implements IReferenceGroupService {
	Logger logger = Logger.getLogger(ReferenceGroupService.class);// 记录日志
	public ReferenceGroupService(String token) {
		super(token);
	}
	public ReferenceGroupService(IRelationalDataManager adapter){
		super(adapter);
	}
	public ReferenceGroupService(String token, IRelationalDataManager adapter){
		super(token, adapter);
	}
	
	/**
	 * 根据id删除ReferenceGroup
	 * @param id
	 * @return
	 * @throws SQLException
	 */
	public boolean deleteReferenceGroup(int id, ReturnValue out)
			throws SQLException {
		int excuteResult = 0;
		Map<String, String> paramMap = new HashMap<String, String>();
		try {
			//先根据group_id查询referenceColumn集合
			String queryColumn="select * from DS_REFERENCE_COLUMN where group_id="+id;
		    List<ReferenceColumn> referenceColumns=	adapter.query2List(true, queryColumn, null,ReferenceColumn.class );
		   
		    if(null !=referenceColumns&&referenceColumns.size()>0)
		    {
		    	//根据group_id去删除ReferenceColumn
		    	paramMap.clear();
				paramMap.put("group_id", Integer.toString(id));
				excuteResult = adapter.delete(paramMap, ReferenceColumn.class);
				
		    	 //for(ReferenceColumn referenceColumn:referenceColumns)
				  //  {
		    		// 根据host_squid_id和reference_squid_id删除squidlink
					/*	paramMap.clear();
						paramMap.put("from_squid_id", Integer.toString(referenceColumn.getHost_squid_id()));
						paramMap.put("to_squid_id", Integer.toString(referenceColumn.getReference_squid_id()));
						excuteResult = adapter.delete(paramMap, SquidLink.class);
						// 根据host_squid_id和reference_squid_id删除squidjoin
						paramMap.clear();
						paramMap.put("target_squid_id", Integer.toString(referenceColumn.getHost_squid_id()));
						paramMap.put("joined_squid_id", Integer.toString(referenceColumn.getReference_squid_id()));
						excuteResult = adapter.delete(paramMap,SquidJoin.class);*/
					/*	// 根据column_id和transformation_type_id查询出transformation的id,调用删除transformation的方法
						paramMap.clear();
						paramMap.put("column_id", Integer.toString(referenceColumn.getColumn_id()));
						paramMap.put("transformation_type_id",Integer.toString(TransformationTypeEnum.VIRTUAL.value()));
						List<Transformation> transformations = adapter.query2List(true,paramMap,Transformation.class);
						for(Transformation transformation : transformations)
						{
							//删除transformation
							TransformationService transformationService=new TransformationService(token,adapter);
							delteResult=transformationService.deleteTransformation(transformation.getId(), out);
						}*/
				   // }
		    }
		  //删除ReferenceGroup
		    if(excuteResult >= 0)
		    {
		    	paramMap.clear();
		    	paramMap.put("id", String.valueOf(id));
		    	excuteResult= adapter.delete(paramMap,ReferenceColumnGroup.class );
		    }
		} catch (Exception e) {
			logger.error("[删除deleteReferenceGroup=========================================exception]", e);
			try {
				adapter.rollback();
			} catch (SQLException e1) {
				logger.error("rollback err!", e1);
			}
			out.setMessageCode(MessageCode.ERR_DELETE_REFERENCECOLUMNGROUP);
			return false;
		}
		return excuteResult >= 0 ? true : false;
	}
	
    /**
     * 创建ReferenceColumn
     * 
     * @param extractSquidId
     * @return
     * @author bo.dang          // update bug：728  by yi.zhou 
     * @date 2014年5月10日
     */
    public ReferenceColumnGroup mergeReferenceColumnGroup(IRelationalDataManager adapter,int fromSquid, int toSquidId) {
        ReferenceColumnGroup columnGroup = null;
        try {
            //columnGroup.setKey(StringUtils.generateGUID());
        	String sql = "select * from ds_reference_column_group where id in " +
            		"(select group_id from ds_reference_column where reference_squid_id="+toSquidId+" and host_squid_id="+fromSquid+")";
            columnGroup = adapter.query2Object(true, sql, null, ReferenceColumnGroup.class);
            if(StringUtils.isNull(columnGroup)){
            	sql  = "select count(*) cnt from ds_reference_column_group where reference_squid_id="+toSquidId;
            	Map<String, Object> map = adapter.query2Object(true, sql, null);
            	int cnt = 0;
            	if (map!=null&&map.containsKey("CNT")){
            		cnt = Integer.parseInt(map.get("CNT")+"");
            	}
                columnGroup = new ReferenceColumnGroup();
                columnGroup.setKey(StringUtils.generateGUID());
                columnGroup.setReference_squid_id(toSquidId);
                columnGroup.setRelative_order(cnt+1);
                columnGroup.setId(adapter.insert2(columnGroup));
            }
            //没有意义 update by yi.zhou
            /*else {
                adapter.update2(columnGroup);
            }*/
        } catch (SQLException e) {
            logger.error("Insert ReferenceColumnGroup异常", e);
            try {
                adapter.rollback();
            } catch (SQLException e1) { // 数据库回滚失败（程序不能处理该异常）！
                logger.fatal("rollback err!", e1);
            }
        } finally {
            //adapter.closeSession();
        }
        return columnGroup;
    }
}
