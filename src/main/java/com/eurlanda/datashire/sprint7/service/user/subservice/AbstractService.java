package com.eurlanda.datashire.sprint7.service.user.subservice;

import com.eurlanda.datashire.adapter.DataAdapterFactory;
import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.entity.*;
import com.eurlanda.datashire.enumeration.DMLType;
import com.eurlanda.datashire.enumeration.DSObjectType;
import com.eurlanda.datashire.server.model.Base.BaseObject;
import com.eurlanda.datashire.server.model.User;
import com.eurlanda.datashire.server.utils.TokenUtil;
import com.eurlanda.datashire.sprint7.packet.InfoPacket;
import com.eurlanda.datashire.utility.AnnotationHelper;
import com.eurlanda.datashire.utility.MessageCode;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 原子业务抽象类
 * 
 * 	1.通用查询
 *  2.通用新增、更新
 *  3.删除操作比较复杂，不同对象逻辑不同，由子类自己实现
 * 
 * @author dang.lu 2013.09.12
 *
 */
public abstract class AbstractService {
	// final variables should be fully capitalized
	protected static final Logger logger = LogManager.getLogger(AbstractService.class);
	// 数据持久化操作适配器（传统关系型数据-内存数据库）
	protected static final IRelationalDataManager adapter = DataAdapterFactory.getDefaultDataManager();
	/** key:class name, value:sql that select all columns from a table */
	public static final Map<String, String> SQL_Cache = new HashMap<String, String>();
	
	/**
	 * 删除对象
	 * @param id
	 * @return
	 */
	public abstract InfoPacket remove(int id);
	
	/** 通用查询全部 */
	public List getAll(Class<? extends DSBaseModel> c) {
		String clsName = c.getName();
		if(SQL_Cache.get(clsName)==null){
			String [] tableName = AnnotationHelper.getTableName(c);
			SQL_Cache.put(clsName, "SELECT "+AnnotationHelper.cols2Str(c)+" FROM "+tableName[tableName.length-1]);
		}
		return adapter.query2List(SQL_Cache.get(clsName), null, c);
	}
	
	/**
	 * 通用（权限相关）添加/修改
	 *   支持混合对象处理(即入参列表可以不是同一种类型对象)
	 * @param beans
	 * @return List<InfoPacket> (包括对象ID、key、code、type等信息)
	 */
	public List<InfoPacket> save(List<? extends DSBaseModel> beans, int t) {
		List<InfoPacket> list = new ArrayList<InfoPacket>();
		if(beans==null || beans.isEmpty()) return list; // 如果校验入参为空，或操作类型不支持，则不处理直接返回
		adapter.openSession();
		DSBaseModel bean;
		InfoPacket info;
		boolean succeed=true;
		try{
		  for(int i=beans.size()-1; i>=0; i--){
			  bean=beans.get(i);
			  info = new InfoPacket(); // AvoidInstantiatingObjectsInLoops,Avoid this whenever you can, it's really expensive
			  info.setKey(bean.getKey());
			  info.setType(getType(bean));
			  logger.info(MessageFormat.format("{0}: {1}", DMLType.parse(t),bean));
			  if(t == DMLType.INSERT.value()){
				info.setId(adapter.insert2(bean));
				info.setCode((info.getId()>0?MessageCode.SUCCESS:MessageCode.SQL_ERROR).value());
			  }else if(t == DMLType.UPDATE.value()){
				info.setId(bean.getId());
				info.setCode(adapter.update2(bean)?MessageCode.SUCCESS.value():MessageCode.SQL_ERROR.value());
			  }
			  list.add(info);
		  }
		} catch (SQLException e) {
			succeed=false;
			logger.error("[save-sql-exception]", e);
			try {  // 事务回滚，很好很强大
				adapter.rollback();
			} catch (SQLException e1) {
				logger.error("rollback err!", e1);
			}
		}finally{
			//try {Thread.sleep(80000);} catch (InterruptedException e) {} // test lock table
			adapter.closeSession();
			if(!succeed){ // 如果新增/更新失败，则封装原信息返回，并将code设为：SQL_ERROR
				list.clear();
				for(int i=beans.size()-1; i>=0; i--){
					bean=beans.get(i);
					info = new InfoPacket();
					info.setKey(bean.getKey());
					info.setType(getType(bean));
					info.setId(bean.getId());
					info.setCode(MessageCode.SQL_ERROR.value());
					list.add(info);
				}
			}
		}
		return list;
	}

    /**
     * 通用（权限相关）添加/修改 用户体系改了,临时使用TODO
     *   支持混合对象处理(即入参列表可以不是同一种类型对象)
     * @param beans
     * @return List<InfoPacket> (包括对象ID、key、code、type等信息)
     */
    public List<InfoPacket> saveBaseObjectForUser(List<? extends BaseObject> beans, int t) {
        List<InfoPacket> list = new ArrayList<InfoPacket>();
        if(beans==null || beans.isEmpty()) return list; // 如果校验入参为空，或操作类型不支持，则不处理直接返回
        adapter.openSession();
        BaseObject bean;
        InfoPacket info;
        boolean succeed=true;
        try{
            for(int i=beans.size()-1; i>=0; i--){
                bean=beans.get(i);
                info = new InfoPacket(); // AvoidInstantiatingObjectsInLoops,Avoid this whenever you can, it's really expensive
                info.setKey(TokenUtil.getKey());
                info.setType(getType(bean));
                logger.info(MessageFormat.format("{0}: {1}", DMLType.parse(t),bean));
                if(t == DMLType.INSERT.value()){
                    info.setId(adapter.insert2(bean));
                    info.setCode((info.getId()>0?MessageCode.SUCCESS:MessageCode.SQL_ERROR).value());
                }else if(t == DMLType.UPDATE.value()){
                    info.setId(bean.getId());
                    info.setCode(adapter.update2(bean)?MessageCode.SUCCESS.value():MessageCode.SQL_ERROR.value());
                }
                list.add(info);
            }
        } catch (SQLException e) {
            succeed=false;
            logger.error("[save-sql-exception]", e);
            try {  // 事务回滚，很好很强大
                adapter.rollback();
            } catch (SQLException e1) {
                logger.error("rollback err!", e1);
            }
        }finally{
            //try {Thread.sleep(80000);} catch (InterruptedException e) {} // test lock table
            adapter.closeSession();
            if(!succeed){ // 如果新增/更新失败，则封装原信息返回，并将code设为：SQL_ERROR
                list.clear();
                for(int i=beans.size()-1; i>=0; i--){
                    bean=beans.get(i);
                    info = new InfoPacket();
                    info.setKey(TokenUtil.getKey());
                    info.setType(getType(bean));
                    info.setId(bean.getId());
                    info.setCode(MessageCode.SQL_ERROR.value());
                    list.add(info);
                }
            }
        }
        return list;
    }
	
	/* 根据入参对象返回对象类型 */
//	public static DSObjectType getType(final DSBaseModel bean){ // MethodArgumentCouldBeFinal
	public static DSObjectType getType(final Object bean){ // MethodArgumentCouldBeFinal
		if(bean instanceof Team){
			return DSObjectType.TEAM;
		}else if(bean instanceof User){
			return DSObjectType.USER;
		}
		else if(bean instanceof Role){
			return DSObjectType.ROLE;
		}
		else if(bean instanceof Group){
			return DSObjectType.GROUP;
		}
		// added by bo.dang start
		else if(bean instanceof TableExtractSquid){
			return DSObjectType.EXTRACT;
		}
        else if(bean instanceof DocExtractSquid){
            return DSObjectType.DOC_EXTRACT;
        }
        else if(bean instanceof XmlExtractSquid){
            return DSObjectType.XML_EXTRACT;
        }
        else if(bean instanceof WebLogExtractSquid){
            return DSObjectType.WEBLOGEXTRACT;
        }
        else if(bean instanceof WeiBoExtractSquid){
            return DSObjectType.WEIBOEXTRACT;
        }
        else if(bean instanceof WeiBoExtractSquid){
            return DSObjectType.WEIBOEXTRACT;
        }
        else if(bean instanceof WebExtractSquid){
            return DSObjectType.WEBEXTRACT;
        }
        else if(bean instanceof SquidLink){
            return DSObjectType.SQUIDLINK;
        }
		// added by bo.dang end
		else if(bean instanceof StageSquid){
			return DSObjectType.STAGE;
		}
		else if(bean instanceof Transformation){
			return DSObjectType.TRANSFORMATION;
		}
		// ReportSquid added by bo.dang
		else if(bean instanceof ReportSquid){
			return DSObjectType.REPORT;
		}
		return DSObjectType.UNKNOWN;
	}
	
	   /* 根据入参对象返回对象类型 
	    * 
	    */
    public static DSObjectType getDSObjectType(final Object bean){ // MethodArgumentCouldBeFinal
        if(bean instanceof Team){
            return DSObjectType.TEAM;
        }else if(bean instanceof User){
            return DSObjectType.USER;
        }
        else if(bean instanceof Role){
            return DSObjectType.ROLE;
        }
        else if(bean instanceof Group){
            return DSObjectType.GROUP;
        }
        // added by bo.dang start
        else if(bean instanceof TableExtractSquid){
            return DSObjectType.EXTRACT;
        }
        else if(bean instanceof DocExtractSquid){
            return DSObjectType.DOC_EXTRACT;
        }
        else if(bean instanceof XmlExtractSquid){
            return DSObjectType.XML_EXTRACT;
        }
        else if(bean instanceof WebLogExtractSquid){
            return DSObjectType.WEBLOGEXTRACT;
        }
        else if(bean instanceof WeiBoExtractSquid){
            return DSObjectType.WEIBOEXTRACT;
        }
        else if(bean instanceof WeiBoExtractSquid){
            return DSObjectType.WEIBOEXTRACT;
        }
        else if(bean instanceof WebExtractSquid){
            return DSObjectType.WEBEXTRACT;
        }
        else if(bean instanceof SquidLink){
            return DSObjectType.SQUIDLINK;
        }
        // added by bo.dang end
        else if(bean instanceof StageSquid){
            return DSObjectType.STAGE;
        }
        else if(bean instanceof Transformation){
            return DSObjectType.TRANSFORMATION;
        }
        // ReportSquid added by bo.dang
        else if(bean instanceof ReportSquid){
            return DSObjectType.REPORT;
        }
        // GISMapSquid added by yi.zhou
        else if (bean instanceof GISMapSquid){
        	return DSObjectType.GISMAP;
        }
        else if(bean instanceof DataMiningSquid){
            return DSObjectType.DATAMINING;
        }
        else if(bean instanceof SquidIndexes){
            return DSObjectType.SQUIDINDEXS;
        }else if(bean instanceof WebserviceExtractSquid){
        	return DSObjectType.WEBSERVICEEXTRACT;
        }else if(bean instanceof HttpExtractSquid){
        	return DSObjectType.HTTPEXTRACT;
        }
        return DSObjectType.UNKNOWN;
    }
    
}
