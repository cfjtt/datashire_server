package com.eurlanda.datashire.sprint7.service.squidflow;

import com.eurlanda.datashire.adapter.DataAdapterFactory;
import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.dao.IColumnDao;
import com.eurlanda.datashire.dao.IReferenceColumnDao;
import com.eurlanda.datashire.dao.ISquidDao;
import com.eurlanda.datashire.dao.ITransformationDao;
import com.eurlanda.datashire.dao.ITransformationInputsDao;
import com.eurlanda.datashire.dao.ITransformationLinkDao;
import com.eurlanda.datashire.dao.impl.ColumnDaoImpl;
import com.eurlanda.datashire.dao.impl.ReferenceColumnDaoImpl;
import com.eurlanda.datashire.dao.impl.SquidDaoImpl;
import com.eurlanda.datashire.dao.impl.TransformationDaoImpl;
import com.eurlanda.datashire.dao.impl.TransformationInputsDaoImpl;
import com.eurlanda.datashire.dao.impl.TransformationLinkDaoImpl;
import com.eurlanda.datashire.entity.*;
import com.eurlanda.datashire.entity.operation.DataEntity;
import com.eurlanda.datashire.entity.operation.WhereCondition;
import com.eurlanda.datashire.enumeration.*;
import com.eurlanda.datashire.server.utils.TokenUtil;
import com.eurlanda.datashire.sprint7.packet.InfoPacket;
import com.eurlanda.datashire.sprint7.plug.ColumnPlug;
import com.eurlanda.datashire.sprint7.plug.GetParam;
import com.eurlanda.datashire.sprint7.plug.SupportPlug;
import com.eurlanda.datashire.sprint7.plug.TransformationPlug;
import com.eurlanda.datashire.sprint7.service.squidflow.subservice.ReferenceColumnService;
import com.eurlanda.datashire.utility.ColumnUtility;
import com.eurlanda.datashire.utility.JsonUtil;
import com.eurlanda.datashire.utility.MessageCode;
import com.eurlanda.datashire.utility.ReturnValue;
import com.eurlanda.datashire.utility.StringUtils;
import org.apache.log4j.Logger;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 
 * Column业务处理类
 * Title : 
 * Description: 
 * Author :赵春花 2013-10-28
 * update :赵春花 2013-10-28
 * Copyright : ©2012-2013 悦岚（上海）数据服务有限公司
 */
public  class ColumnService  extends SupportPlug implements IColumnService{
	static Logger logger = Logger.getLogger(ColumnService.class);// 记录日志
	private String token;
	public ColumnService(String token) {
		super(token);
		this.token = token;
	}
	/**
	 * 创建Column对象集合
	 * 作用描述：
	 * 1、根据传入的Column对象集合创建Column对象
	 * 2、创建成功——根据Column集合对象里的Key查询相对应的ID
	 * 3、关闭数据连接信息
	 * 调用方法前验证：
	 * Column对象集合不能为空且必须有数据
	 * Column对象集合里的每个Key必须有值
	 * 修改说明：
	 *@param out
	 *@return
	 */
	public List<InfoPacket> createColumns(List<Column> columns, ReturnValue out) {
		logger.debug(String.format("createColumns-ColumnList.size()=%s",
				columns.size()));
		boolean create = false;
		List<InfoPacket> infoPackets = new ArrayList<InfoPacket>();
		// 1、根据传入的Column对象集合创建Column对象 调用批量新增
		ColumnPlug columnPlug = new ColumnPlug(adapter);
		try {
			create = columnPlug.createColumns(columns, out);
			// 新增成功
			//2、创建成功——根据Column集合对象里的Key查询相对应的ID
			TransformationPlug transformationPlug = new TransformationPlug(adapter);
			if (create) {
				int j = 0;
				for (Column column : columns) {
					// 创建虚拟转换
					List<Column> columnsize = columnPlug.getColumns(
							column.getSquid_id(), out);
					if (columnsize != null) {
						j = columnsize.size() + j;
					}
					// 根据KEY获得ID
					int fromColumnId = columnPlug.getColumnId(column.getKey(), out);
					// 组装Column
					column.setId(fromColumnId);
					column.setType(DSObjectType.COLUMN.value());
					// 组装Transformation
					Transformation eTransformation = new Transformation();
					eTransformation.setColumn_id(fromColumnId);
					// 生产Guid(to)
					eTransformation.setKey(StringUtils.generateGUID());
					eTransformation.setLocation_x(0);
					eTransformation.setLocation_y(j * 25 + 25 / 2);
					eTransformation.setSquid_id(column.getSquid_id());
					eTransformation.setTranstype(TransformationTypeEnum.VIRTUAL
							.value());
					create = transformationPlug.createTransformation(
							eTransformation, out);
					if (create == false) {
						out.setMessageCode(MessageCode.INSERT_ERROR);
						return null;
					}
					j++;
					if (create) {
						// 获得Key
						String key = column.getKey();
						// 根据Key获得ID
						int id = columnPlug.getColumnId(key, out);
						InfoPacket infoPacket = new InfoPacket();
						infoPacket.setId(id);
						infoPacket.setKey(key);
						infoPacket.setType(DSObjectType.COLUMN);
						infoPackets.add(infoPacket);
					}
				}
			}
		} catch (Exception e) {
			logger.error("createColumns-return=%s", e);
		} finally {
			// 释放连接
			//3、关闭数据连接信息
			adapter.commitAdapter();
		}

		return infoPackets;
	}
	
	/**
	 * 
	 * 作用描述：
	 * 1、根据Column对象集合的Column对象的ＩＤ对相应的ｃｏｌｕｍｎ对象进行修改
	 * 2、修改完根据Column对象的key查询结果集返回
	 * 3、关闭数据库连接
	 * 调用方法前验证：
	 * 确保Column对象集合不为空，并且Column对象集合里的每个Column对象的Id不为空
	 * 修改说明：
	 *@param out
	 *@return
	 */
	public List<InfoPacket> updateColumns(List<Column> columns,ReturnValue out){
		logger.debug(String.format("updateColumns-columnList.size()=%s", columns.size()));
		List<InfoPacket> infoPackets = new ArrayList<InfoPacket>();
		boolean create = false;
		try {
			//封装批量新增数据集
			List<List<DataEntity>>  paramList = new ArrayList<List<DataEntity>>();
			//封装条件集合
			List<List<WhereCondition>>  whereCondList = new ArrayList<List<WhereCondition>>();
			ColumnPlug columnPlug = new ColumnPlug(adapter);
			//1、根据Column对象集合的Column对象的ＩＤ对相应的Column对象进行修改
			//调用转换类
			GetParam getParam = new GetParam();
			for (Column column: columns) {
				//根据
				Column sourceColumn = columnPlug.getColumn(column.getId(), out);
				//参数
				List<DataEntity> dataEntitys =  new ArrayList<DataEntity>();
				getParam.getColumn(column, dataEntitys);
				paramList.add(dataEntitys);
				//条件
				List<WhereCondition> whereConditions = new ArrayList<WhereCondition>();
				WhereCondition whereCondition = new WhereCondition();
				whereCondition.setAttributeName("ID");
				whereCondition.setDataType(DataStatusEnum.INT);
				whereCondition.setMatchType(MatchTypeEnum.EQ);
				whereCondition.setValue(column.getId());
				whereConditions.add(whereCondition);
				whereCondList.add(whereConditions);
				
			}
			//调用批量新增
			create = adapter.updatBatch(SystemTableEnum.DS_COLUMN.toString(), paramList, whereCondList, out);
			logger.debug(String.format("updateColumns-return=%s",create ));
			//2、修改完根据Column对象的key查询结果集返回
			if(create){
				//根据Key查询出新增的ID
				for (Column column : columns) {
					//获得Key
					String key = column.getKey();
					int id = columnPlug.getColumnId(key, out);
					InfoPacket infoPacket = new InfoPacket();
					infoPacket.setCode(1);
					infoPacket.setId(id);
					infoPacket.setType(DSObjectType.COLUMN);
					infoPacket.setKey(key);
					infoPackets.add(infoPacket);
				}
			}
		} catch (Exception e) {
			logger.error("updateTransformationLinks-return=%s",e);
			out.setMessageCode(MessageCode.ERR_ARRAYS);
		}finally{
			//3、关闭数据库连接
			//释放
			adapter.commitAdapter();
		}
		
		return infoPackets;
	}

	/**
	 * 将referenceColumn 与column 进行匹配，类型，名称，
	 * 长度等属性一致，并且column没有被连线，则创建link
	 * @param info
	 * @param out
	 * @return
	 */
	@Override
	public List<TransformationLink> autoMatchColumnByNameAndType(String info, ReturnValue out) {

		// 根据squidId,获取column和referenceColumn
		// 根据squidId,获取没连线的column
		// 匹配column和referenceColumn，并判断column是否连线，如果未连线则创建link
		Map<String, Object> infoMap = JsonUtil.toHashMap(info);
		int squidId = Integer.parseInt(infoMap.get("SquidId").toString());

		IRelationalDataManager adapter3 = null;
		try {
			adapter3 = DataAdapterFactory.getDefaultDataManager();
			adapter3.openSession();
			Map<String, String> paramMap = new HashMap<String, String>();
			paramMap.put("ID", Integer.toString(squidId, 10));
			List<Squid> squids = adapter3.query2List(paramMap, Squid.class);
			IColumnDao columnDao = new ColumnDaoImpl(adapter3);
			// 获取未连线的column
			List<Column> columns = columnDao.getUnlinkedColumnBySquidId(squidId);

			IReferenceColumnDao referenceColumnDao = new ReferenceColumnDaoImpl(adapter3);
			// 获取所有的referenceColumn
			List<ReferenceColumn> referenceColumns = referenceColumnDao.getRefColumnListByRefSquidId(squidId);

			ITransformationDao transformationDao = new TransformationDaoImpl(adapter3);
			ITransformationLinkDao transformationLinkDao = new TransformationLinkDaoImpl(adapter3);
			ITransformationInputsDao transformationInputsDao = new TransformationInputsDaoImpl(adapter3);
			// 开始匹配
			List<TransformationLink> links = new ArrayList<>();
			for(Column c : columns) {
				if(squids!=null){
					//squid类型为stage时不做判断
					if (ColumnUtility.checkIsSystemColumn(c)&&squids.get(0).getSquid_type()!= SquidTypeEnum.STAGE.value())
						continue;
				}
				for(ReferenceColumn rc : referenceColumns) {
					if(rc.getName().equals(c.getName())
							&& rc.getData_type()==c.getData_type()) {
						TransformationLink transformationLink = new TransformationLink();
						Transformation fromTrans = transformationDao.getVTransformationByReferenceColumnId(rc.getColumn_id(), squidId);
						transformationLink.setFrom_transformation_id(fromTrans.getId());
						Transformation toTrans = transformationDao.getVTransformationByColumnId(c.getId(), squidId);
						transformationLink.setTo_transformation_id(toTrans.getId());
						transformationLink.setSource_input_index(0);
						transformationLink.setKey(StringUtils.generateGUID());
						// 保存link
						int id =transformationLinkDao.insert2(transformationLink);
						// 跟新虚拟transInputs:[source_transformation_id,]
						TransformationInputs ti = transformationInputsDao.getTransInputListByTransId(toTrans.getId()).get(0);
						ti.setSource_transform_id(fromTrans.getId());
						transformationInputsDao.update(ti);

						transformationLink.setId(id);
						links.add(transformationLink);
						break;
					}
				}
			}
			return links;
		} catch (Exception e) {
			logger.error("autoMatchColumnByNameAndType 异常", e);
			out.setMessageCode(MessageCode.SQL_ERROR);
			if(adapter3 != null) {
				try {
					adapter3.rollback();
				} catch (SQLException e1) {
					e1.printStackTrace();
				}
			}
			return null;
		} finally {
			if(adapter3 != null) {
				adapter3.closeSession();
			}
		}
	}

	/**
	 * 复制column
	 * 1.复制column
	 * 2.添加virtTrans
	 * 3.添加tranInputs
	 * 4.添加refColumn
	 * @param info SquidId:int List<int> ColumnIds
	 * @param out
	 * @return  "NewColumns:List<Column> NewVTrans:List<Transformation>"
	 */
	public Map<String, Object> copySquidColumns(String info, ReturnValue out) {
		Map<String, Object> infoMap = JsonUtil.toHashMap(info);
		// 新增的column和transformation
		List<Column> newColumns = new ArrayList<>();
		List<Transformation> newTrans = new ArrayList<>();
		// 粘贴的squidId
		int squidId = Integer.parseInt(infoMap.get("SquidId").toString());
		// 复制的 column id集合
		List<Integer> columnIds = (List<Integer>)infoMap.get("ColumnIds");
		logger.debug("复制column:" + columnIds);
		IRelationalDataManager adapter3 = null;
		try {
			adapter3 = DataAdapterFactory.getDefaultDataManager();
			adapter3.openSession();

			IColumnDao columnDao = new ColumnDaoImpl(adapter3);
			ITransformationDao transformationDao = new TransformationDaoImpl(adapter3);
			ITransformationInputsDao transformationInputsDao = new TransformationInputsDaoImpl(adapter3);
			ISquidDao squidDao = new SquidDaoImpl(adapter3);
			ReferenceColumnService referenceColumnService = new ReferenceColumnService(TokenUtil.getToken());

			List<Column> sourceColumns = columnDao.getColumnListByIds(columnIds);

			// 该squid原来column
			List<Column> targetColumn = columnDao.getColumnListBySquidId(squidId);

			// 遍历column,将他们的名字放置到set中
			Set<String> nameSet = new HashSet<>();
			for(Column c : targetColumn) {
				nameSet.add(c.getName());
			}
			nameSet.add("start_date");
			nameSet.add("end_date");
			nameSet.add("active_flag");
			nameSet.add("version");
			int relative_order = targetColumn.size() + 1;
			// 保存column并创建referenceColumn
			for(Column column : sourceColumns) {
				// column原始名称
				String cname = column.getName();
				int no = 1;
				while(nameSet.contains(column.getName())) {
					column.setName(cname +"_"+no);
					no ++;
				}
				nameSet.add(column.getName());

				column.setKey(StringUtils.generateGUID());
				column.setSquid_id(squidId);
				// 设置第几位
				column.setRelative_order(relative_order++);
				Map<String, Object> map = createColumn(column, columnDao, squidDao, transformationInputsDao, transformationDao);
				newColumns.add((Column)map.get("column"));
				newTrans.add((Transformation) map.get("virtTrans"));

				RepositoryServiceHelper helper = new RepositoryServiceHelper(TokenUtil.getToken());
				helper.synchronousInsertColumn(adapter3, squidId, column, DMLType.INSERT.value(), false);
				// 创建referenceColumn
//				referenceColumnService.createReferenceColumn(column, adapter3);
			}

		} catch (Exception e) {
			logger.error("复制粘贴column异常", e);
			out.setMessageCode(MessageCode.SQL_ERROR);
			if(adapter3 != null) {
				try {
					adapter3.rollback();
				} catch (SQLException e1) {
					logger.error("回滚失败", e1);
				}
			}
			return null;
		} finally {
			if(adapter3 != null) {
				adapter3.closeSession();
			}
		}
		Map<String, Object> map = new HashMap<>();
		map.put("NewColumns", newColumns);
		map.put("NewVTrans", newTrans);
		return map;
	}

	/**
	 * 创建一个column
	 * @param column 要创建的column
	 * @param columnDao
	 * @param transformationDao
	 * @throws Exception
	 */
	private Map<String, Object> createColumn(Column column,
											 IColumnDao columnDao,
											 ISquidDao squidDao,
											 ITransformationInputsDao transformationInputsDao,
											 ITransformationDao transformationDao) throws Exception {
		return createColumn(column, null, columnDao, squidDao, transformationInputsDao, null, transformationDao);
	}

	/**
	 * 创建一个column
	 * @param column 要创建的column
	 * @param transformation 连接该column的transformation,为空则说明只是单独创建column,不需要连线
	 * @param columnDao
	 * @param transformationDao
	 * @throws Exception
	 */
	private Map<String, Object> createColumn(Column column, Transformation transformation,
							  IColumnDao columnDao,
							  ISquidDao squidDao,
							  ITransformationInputsDao transformationInputsDao,
							  ITransformationLinkDao transformationLinkDao,
							  ITransformationDao transformationDao) throws Exception {
		int id = columnDao.insert2(column);
		column.setId(id);

		// 判断是否有下游squid,如果存在，需要对下游squid添加referenceColumn
//		squidDao.getNextSquidIdsById(column.getSquid_id());

		// 创建virtual transformation
		Transformation virtTransformation = new Transformation();

		virtTransformation.setColumn_id(column.getId());
		virtTransformation.setKey(StringUtils.generateGUID());
		virtTransformation.setSquid_id(column.getSquid_id());
//		virtTransformation.setLocation_x(0);
//		virtTransformation.setLocation_y(columnList.size()*25 + 25/2);
		virtTransformation.setTranstype(TransformationTypeEnum.VIRTUAL.value());
		virtTransformation.setOutput_data_type(column.getData_type());

		int transId = transformationDao.insert2(virtTransformation);
		// 查询，以获取数据库生成的ID
//			virtTransformation = transformationDao.getTransformationByKey(virtTransformation.getKey());;
		virtTransformation.setId(transId);

		TransformationInputs transformationInputs = new TransformationInputs();
		transformationInputs.setInput_Data_Type(column.getData_type());
		transformationInputs.setTransformationId(virtTransformation.getId());
		if(transformation != null) {
			transformationInputs.setSource_transform_id(transformation.getId());
			transformationInputs.setSource_tran_output_index(0);
			transformationInputs.setSourceTransformationName(transformation.getName());
		}
		transformationInputs.setKey(StringUtils.generateGUID());

//		ITransformationInputsDao transformationInputsDao = new TransformationInputsDaoImpl(adapter3);
		int inputsId = transformationInputsDao.insert2(transformationInputs);
		transformationInputs.setId(inputsId);
		List<TransformationInputs> inputsList = new ArrayList<>();
		inputsList.add(transformationInputs);
		virtTransformation.setInputs(inputsList);

		Map<String, Object> map = new HashMap<>();
		// 创建Link
		if(transformation != null) {
			TransformationLink transformationLink = new TransformationLink();
			transformationLink.setFrom_transformation_id(transformation.getId());
			transformationLink.setTo_transformation_id(virtTransformation.getId());
			transformationLink.setKey(StringUtils.generateGUID());
			transformationLink.setIn_order(1);

//			ITransformationLinkDao transformationLinkDao = new TransformationLinkDaoImpl(adapter3);
			int linkId = transformationLinkDao.insert2(transformationLink);
			transformationLink.setId(linkId);
			map.put("link", transformationLink);
		}

		map.put("column", column);
		map.put("virtTrans", virtTransformation);
		return map;
	}

}
