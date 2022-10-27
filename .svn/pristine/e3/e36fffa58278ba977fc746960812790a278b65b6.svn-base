package com.eurlanda.datashire.sprint7.service.squidflow;

import com.eurlanda.datashire.adapter.DataAdapterFactory;
import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.dao.ITemplateTypeDao;
import com.eurlanda.datashire.dao.impl.TemplateTypeDaoImpl;
import com.eurlanda.datashire.entity.TemplateDataInfo;
import com.eurlanda.datashire.entity.TemplateTypeInfo;
import com.eurlanda.datashire.utility.JsonUtil;
import com.eurlanda.datashire.utility.MessageCode;
import com.eurlanda.datashire.utility.ReturnValue;
import org.apache.log4j.Logger;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * templateData（模版数据管理 ） 业务处理类
 * @author yi.zhou
 *
 */
public class TemplateDataProcess {
	/**
	 * 记录SquidJoinProcess日志
	 */
	static Logger logger = Logger.getLogger(TemplateDataProcess.class);
	
	/**
	 * 根据type集合，每个类型各取 count条记录
	 * TemplateTypes:[TemplateType的集合]
	 * TemplateDataCount：生成数据的数量 
	 * 返回模版数据
	 * @param info
	 * @param out
	 * @return
	 */
	public Map<String, Object> getTemplateDataByTypes (String info, ReturnValue out){
		Map<String, Object> outputMap = new HashMap<String, Object>();
		IRelationalDataManager adapter3 = null;
		try {
			Map<String, Object> infoMap = JsonUtil.toHashMap(info);
			Map<String, Object> parms = JsonUtil.toHashMap(infoMap.get("TemplateParms"));
			Map<String, Object> map = new HashMap<String, Object>();
			if (parms!=null&&parms.size()>0){
				List<TemplateTypeInfo> templateTypes = JsonUtil.toGsonList(parms.get("TemplateTypes")+"", TemplateTypeInfo.class) ;
				int templateCount = (Integer)parms.get("TemplateDataCount");
				adapter3 = DataAdapterFactory.getDefaultDataManager();
				adapter3.openSession();
				ITemplateTypeDao templateDao = new TemplateTypeDaoImpl(adapter3);
				if (templateTypes!=null&&templateTypes.size()>0){
					for (TemplateTypeInfo typeInfo : templateTypes) {
						List<TemplateDataInfo> tempList = templateDao.getTemplateDataListByType(typeInfo.getId(), templateCount);
						List<String> dataList = new ArrayList<String>();
						if (tempList!=null&&tempList.size()>0){
							for (TemplateDataInfo templateDataInfo : tempList) {
								dataList.add(templateDataInfo.getData_value());
							}
						}
						String key = typeInfo.getTemplateColumnName()!=null?
								typeInfo.getTemplateColumnName():typeInfo.getId()+"";
						map.put(key, dataList);
					}
					outputMap.put("TemplateData", map);
				}else{
					out.setMessageCode(MessageCode.NODATA);
				}
			}
		} catch (Exception e) {
			logger.error("[删除getTemplateDataByTypes=========================================exception]", e);
			try {
				adapter3.rollback();
			} catch (SQLException e1) {
				logger.error("rollback err!", e1);
			}
			logger.error(MessageFormat.format("删除getTemplateDataByTypes异常", 0), e);
			out.setMessageCode(MessageCode.SQL_ERROR);
		}
		return outputMap;
	}
	
	/**
	 * 所有样板数据的类型，返回List<TemplateTypeInfo>
	 * @param out
	 * @return
	 */
	public Map<String, Object> getAllTemplateDataTypes(ReturnValue out){
		Map<String, Object> outputMap = new HashMap<String, Object>();
		IRelationalDataManager adapter3 = null;
		try {
			adapter3 = DataAdapterFactory.getDefaultDataManager();
			adapter3.openSession();
			ITemplateTypeDao tempDao = new TemplateTypeDaoImpl(adapter3);
			List<TemplateTypeInfo> list = tempDao.getTemplateTypeList();
			outputMap.put("TemplateTypes", list);
		} catch (Exception e) {
			logger.error("[删除getAllTemplateDataTypes=========================================exception]", e);
			try {
				adapter3.rollback();
			} catch (SQLException e1) {
				logger.error("rollback err!", e1);
			}
			logger.error(MessageFormat.format("删除getAllTemplateDataTypes异常", 0), e);
			out.setMessageCode(MessageCode.SQL_ERROR);
		}finally{
			adapter3.closeSession();
		}
		return outputMap;
	}
	
	public static void main(String[] args) {
		getDataForTimestamp();
	}
	
	private static void getDataForDouble(){
		for (int j = 0; j < 100; j++) {
			Random dom = new Random();
			double d = dom.nextDouble();
			int i = dom.nextInt(100);
			d = d + i;
			BigDecimal bg = new BigDecimal(d);
	        double f1 = bg.setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue();
	        String sql = "insert into ds_sys_template_data(type_id,data_value) values(6,"+f1+");";
			System.out.println(sql);
		}
	}
	
	private static void getDataForTimestamp(){
		DateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		String tsStr = "";
		for (int i = 0; i < 100; i++) {
			long c = new Date().getTime()+(i*1000*3600*24);
			Timestamp ss = new Timestamp(c);
			tsStr = sdf.format(ss);
			String sql = "insert into ds_sys_template_data(type_id,data_value) values(7,'"+tsStr+"');";
			System.out.println(sql);
		}
	}
}

