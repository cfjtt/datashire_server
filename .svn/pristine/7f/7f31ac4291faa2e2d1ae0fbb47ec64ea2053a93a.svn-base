package com.eurlanda.datashire.sprint7.service.squidflow.subservice;

import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.entity.Squid;
import com.eurlanda.datashire.enumeration.DSObjectType;
import com.eurlanda.datashire.sprint7.packet.PushMessagePacket;
import com.eurlanda.datashire.sprint7.service.squidflow.AbstractRepositoryService;
import com.eurlanda.datashire.utility.MessageCode;
import com.eurlanda.datashire.utility.ReturnValue;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
/**
 * 通用校验表是否被抽取
 * @author lei.bin
 *
 */
public class CheckExtractService extends AbstractRepositoryService implements ICheckExtractService {

	protected CheckExtractService(IRelationalDataManager adapter) {
		super(adapter);
	}

	public CheckExtractService(String token) {
		super(token);
	}

	public CheckExtractService(String token, IRelationalDataManager adapter) {
		super(token, adapter);
	}

	/**
	 * 更新DS_SOURCE_TABLE中is_extracted
	 * 
	 * @param tableName
	 * @param squid_id
	 */
	public void updateExtract(String tableName, int squid_id, String type,
			ReturnValue out,String key,String token) {
		List<String> list=new ArrayList<String>();
		int executeResult = 0;
		try {
			if ("create".equals(type)) {
				logger.debug("消息气泡 -是否抽取(新增)");
				// 先去判断前端传送过来的表名是否为空,空则不处理。
				//if (StringUtils.isNotBlank(tableName)) {
					// 根据前端传送过来的信息，更新ds_sourece_table的是否抽取字段
					executeResult = adapter
							.execute("update DS_SOURCE_TABLE set is_extracted ='Y' where table_name='"
									+ tableName
									+ "' and source_squid_id="
									+ squid_id);
					if (executeResult > 0) {
						// 调用推送方法
						logger.debug("消息气泡 -是否抽取,颜色变换 ");
						list.add(tableName);
//						PushMessagePacket.push(list, DSObjectType.DBSOURCE, "0001", "0105", key, token);2015-01-04
					} else {
						//out.setMessageCode(MessageCode.SQL_ERROR);
					}
				//}
			}else if ("delete".equals(type)){
					logger.debug("消息气泡 -是否抽取(删除)--表名存在");
					// 查询出删除extrasquid相同表名的个数
					List<Squid> squidList = adapter
							.query2List(true,
									"select * from DS_SQUID where id in ( select to_squid_id from DS_SQUID_LINK where from_squid_id = ( select from_squid_id from DS_SQUID_LINK where to_squid_id = "
											+ squid_id
											+ ")) and table_name='"
											+ tableName + "'", null,
									Squid.class);
					if (squidList.size() == 1) {
						// 调用推送方法
						logger.debug("消息气泡 -是否抽取(颜色消失)");
						list.add(tableName);
						//查询出上游dbsquid的key
						String querySql="select * from DS_SQUID where id =(select from_squid_id from DS_SQUID_LINK where to_squid_id ="+squid_id+")";//对于extractsquid只可能有一个from_squid_id
						List<Squid> dbSquids=adapter.query2List(true, querySql, null, Squid.class);
						//更新缓存表的抽取状态
						executeResult = adapter
								.execute("update DS_SOURCE_TABLE set is_extracted ='N' where table_name='"
										+ tableName
										+ "' and source_squid_id="
										+ squid_id);
						logger.debug("更新缓存表状态成功");
						key=dbSquids.get(0).getKey();
						PushMessagePacket.push(list, DSObjectType.DBSOURCE, "0001", "0108", key, token);
					}
			}
		} catch (Exception e) {
			try {
				adapter.rollback();
				out.setMessageCode(MessageCode.SQL_ERROR);
			} catch (SQLException e1) {
				logger.error("rollback err!", e1);
			}
			logger.error("[updateExtract is error]", e);
		} 
	}
}
