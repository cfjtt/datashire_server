package com.eurlanda.datashire.sprint7.service.squidflow.subservice;

import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.dao.IReferenceColumnDao;
import com.eurlanda.datashire.dao.ISquidDao;
import com.eurlanda.datashire.dao.impl.ReferenceColumnDaoImpl;
import com.eurlanda.datashire.dao.impl.SquidDaoImpl;
import com.eurlanda.datashire.entity.Column;
import com.eurlanda.datashire.entity.ReferenceColumn;
import com.eurlanda.datashire.entity.ReferenceColumnGroup;
import com.eurlanda.datashire.entity.Squid;
import com.eurlanda.datashire.entity.Transformation;
import com.eurlanda.datashire.enumeration.TransformationTypeEnum;
import com.eurlanda.datashire.sprint7.service.squidflow.AbstractRepositoryService;
import com.eurlanda.datashire.sprint7.service.squidflow.TransformationService;
import com.eurlanda.datashire.utility.MessageCode;
import com.eurlanda.datashire.utility.ReturnValue;
import com.eurlanda.datashire.utility.StringUtils;
import org.apache.log4j.Logger;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
/**
 * 删除ReferenceCloumn处理类
 * @author eurlanda01
 *
 */
public class ReferenceColumnService extends AbstractRepositoryService implements IReferenceColumnService {
	Logger logger = Logger.getLogger(ReferenceColumnService.class);// 记录日志
	public ReferenceColumnService(String token) {
		super(token);
	}
	public ReferenceColumnService(IRelationalDataManager adapter){
		super(adapter);
	}
	public ReferenceColumnService(String token, IRelationalDataManager adapter){
		super(token, adapter);
	}
	/**
	 * 根据id删除ReferenceCloumn
	 * @param column_id
	 * @return
	 * @throws SQLException
	 */
	public boolean deleteReferenceColumn(int column_id,ReturnValue out) {
		Map<String, String> paramMap = new HashMap<String, String>();
		try {
			paramMap.put("column_id", String.valueOf(column_id));
			return adapter.delete(paramMap, ReferenceColumn.class)>0?true:false;
		} catch (Exception e) {
			logger.error("[删除deleteReferenceColumn=========================================exception]", e);
			try {
				adapter.rollback();
			} catch (SQLException e1) {
				logger.error("rollback err!", e1);
			}
			out.setMessageCode(MessageCode.SQL_ERROR);
			return false;
		}
	}

	/**
	 * 为column 创建所有的referenceColumn
	 * @param column column
	 * @return
	 * @throws SQLException
	 */
	public void createReferenceColumn(Column column, IRelationalDataManager adapter) throws Exception {
		// 获取column 连接的所有下游squidId
		ISquidDao squidDao = new SquidDaoImpl(adapter);
		IReferenceColumnDao referenceColumnDao = new ReferenceColumnDaoImpl(adapter);
		TransformationService transformationService = new TransformationService(token);

		List<Integer> squidIds = squidDao.getNextSquidIdsById(column.getSquid_id());

		Squid squid = squidDao.getObjectById(column.getSquid_id(), Squid.class);

		ReferenceColumn rc = null;
		if(squidIds == null && squidIds.size()==0) {
			logger.debug("该column 不存在下游squid");
			return;
		} else {
			// 通过column 生成referenceColumn
			rc = genReferenceColumnByColumn(column);
		}

		// 更新所有下游squid的referenceColumn
		for(int squidId : squidIds) {
			// 获取该squid 的所有referenceColumn
			// 新增一条referenceColumn
			// 新增一个virtTrans
			// 新增一个transInputs


			ReferenceColumnGroup rcg = referenceColumnDao.getRefColumnGroupBySquidId(squidId, column.getSquid_id());

			if(rcg == null) {
			 	rcg = new ReferenceColumnGroup();
				rcg.setKey(StringUtils.generateGUID());
				rcg.setReference_squid_id(squid.getId());
				rcg.setRelative_order(1);
				rcg.setGroup_name(squid.getName());
				rcg.setId(adapter.insert2(rcg));
				rc.setGroup_order(1);
			} else {
				List<ReferenceColumn> referenceColumns = referenceColumnDao.getRefColumnListBySquid(column.getSquid_id(), squidId);
				rc.setGroup_order(rcg.getRelative_order());
				rc.setRelative_order(referenceColumns.size() + 1);
			}

			rc.setGroup_id(rcg.getId());
			rc.setGroup_name(rcg.getGroup_name());
			rc.setReference_squid_id(squidId);
			rc.setId(referenceColumnDao.insert2(rc));

			// 创建虚拟trans
			Transformation virtTran = transformationService.initTransformation(adapter, squidId, rc.getColumn_id(), TransformationTypeEnum.VIRTUAL.value(), rc.getData_type(), 1);
		}

		return;
	}

	/**
	 * 通过column 生成referenceColumn
	 * 【注意】需要设置的地方：
	 * rc.setGroup_id(rcg.getId());
	 * rc.setGroup_name(rcg.getGroup_name());
	 * rc.setReference_squid_id(squidId);
	 * rc.setGroup_order(1);
	 * @param c
	 * @return
	 */
	private ReferenceColumn genReferenceColumnByColumn(Column c) {
		ReferenceColumn rc = new ReferenceColumn();
		rc.setColumn_id(c.getId());
		rc.setHost_column_deleted(false);
		rc.setHost_squid_id(c.getSquid_id());
		rc.setIs_referenced(true);
//			rc.setReference_squid_id();
		rc.setAggregation_type(c.getAggregation_type());
		rc.setCdc(c.getCdc());
		rc.setCollation(c.getCollation());
		rc.setColumnAttribute(c.getColumnAttribute());
		rc.setColumntype(c.getColumntype());
		rc.setData_type(c.getData_type());
		rc.setDescription(c.getDescription());
		rc.setFK(c.isFK());
		rc.setIsUnique(c.isIsUnique());
		rc.setIs_Business_Key(c.getIs_Business_Key());
		rc.setIs_groupby(c.isIs_groupby());
		rc.setIsAutoIncrement(c.isIsAutoIncrement());
		rc.setIsPK(c.isIsPK());
		rc.setLength(c.getLength());
		rc.setNullable(c.isNullable());
		rc.setPrecision(c.getPrecision());
//		rc.setRelative_order();
		rc.setScale(c.getScale());
		rc.setSort_Level(c.getSort_Level());
		rc.setSort_type(c.getSort_type());
		rc.setValue(c.getValue());
		rc.setKey(StringUtils.generateGUID());
		rc.setName(c.getName());
		rc.setStatus(c.getStatus());
		rc.setType(c.getType());

		return rc;
	}
}
