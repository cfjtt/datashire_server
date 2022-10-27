package com.eurlanda.datashire.sprint7.service.squidflow;

import com.eurlanda.datashire.adapter.DataAdapterFactory;
import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.entity.Column;
import com.eurlanda.datashire.sprint7.packet.InfoPacket;
import com.eurlanda.datashire.utility.JsonUtil;
import com.eurlanda.datashire.utility.MessageCode;
import com.eurlanda.datashire.utility.ReturnValue;
import org.apache.log4j.Logger;
import org.springframework.stereotype.Service;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * ColumnProcess预处理类
 * ColumnProcess预处理类：对数据进行验证验证数据的完整性和正确性；并调用下层业务处理方法
 * Title :
 * Description:
 * Author :赵春花 2013-8-26
 * update :赵春花2013-8-26
 * Department : JAVA后端研发部
 * Copyright : ©2012-2013 悦岚（上海）数据服务有限公司 </p>
 */
@Service
public class ColumnProcess implements IColumnProcess{
	static Logger logger = Logger.getLogger(ColumnProcess.class);// 记录日志
	//@Autowired
	ColumnService columnService;
	public ColumnProcess(String token) {
		columnService = new ColumnService(token);
	}
	public ColumnProcess() {
	}
	/**
	 * 创建Column集合对象处理类
	 * 作用描述：
	 * 创建Column对象集合
	 * 成功根据Column对象集合里的key查询相对应的Id封装成InfoPacket对象集合返回
	 * 验证：
	 * 1、序列化结果集数据是否为空
	 * 2、Column对象集合里的Column对象的必要字段的值是否正确
	 * 修改说明：
	 *@param info json字符串
	 *@param out 异常处理
	 *@return
	 */
	public List<InfoPacket> createColumns(String info,ReturnValue out){
		logger.debug(String.format("createColumns-info=%s", info));
		List<InfoPacket> infoPackets  = new ArrayList<InfoPacket>();
		//序列化JSONArray
		List<Column> columns =  JsonUtil.toGsonList(info, Column.class);
		//验证Project集合对象
		if(columns==null||columns.size()==0){
			//Column对象集合不能为空size不能等于0
			out.setMessageCode(MessageCode.ERR_COLUMN_NULL);
			logger.debug(String.format("createColumns-columns-=%s", false));
			return infoPackets;
		}else{
			//验证数据
			for (Column column : columns) {
				if(column.getSquid_id()== 0/* || column.getDb_source_table_id() == 0*/){
					//数据不完整
					out.setMessageCode(MessageCode.ERR_COLUMN_DATA_INCOMPLETE);
					return infoPackets;
				}
			}
			infoPackets = columnService.createColumns(columns, out);
			logger.debug(String.format("createColumns-return=%s", infoPackets));
			return infoPackets;
		}
	}
	
	/**
	 * 修改Column对象集合
	 * 作用描述：
	 * 根据Column对象集合里Column的ID进行修改
	 * 修改说明：
	 *@param info json字符串
	 *@param out 异常处理
	 *@return
	 */
	public void updateColumns(String token,String info,ReturnValue out){
		IRelationalDataManager adapter = DataAdapterFactory.getDefaultDataManager();
		try {
			List<Column> list = JsonUtil.toGsonList(info, Column.class);
			if(null!=list&&list.size()>0)
			{
				
			}else
			{
				out.setMessageCode(MessageCode.UPDATE_ERROR);
			}
		} catch (Exception e) {
			// TODO: handle exception
			try {
				adapter.rollback();
			} catch (SQLException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			out.setMessageCode(MessageCode.UPDATE_ERROR);
			logger.error("updateColumn异常", e);
		}finally
		{
			adapter.closeSession();
		}
		
	}
}
