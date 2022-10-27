package com.eurlanda.datashire.dao;

import com.eurlanda.datashire.entity.ReferenceColumn;
import com.eurlanda.datashire.entity.ReferenceColumnGroup;
import com.eurlanda.datashire.utility.DatabaseException;

import java.sql.SQLException;
import java.util.List;

/**
 * ReferenceColumn 数据处理接口
 * @author yi.zhou
 *
 */
public interface IReferenceColumnDao extends IBaseDao {
	/**
	 * 获取ReferenceColumn实体类
	 * @param refSquidId
	 * @return
	 */
	public ReferenceColumn getReferenceColumnById(int refSquidId, int columnId);
	
	/**
	 * 获取某squid下的ReferenceColumn列表
	 * @param hostSquidId 上游squidId
	 * @param refSquidId 下游squidId
	 * @return
	 */
	public List<ReferenceColumn> getRefColumnListBySquid(int hostSquidId, int refSquidId);
	
	/**
	 * 获取某Group下的ReferenceColumn列表
	 * @param groupId
	 * @return
	 */
	public List<ReferenceColumn> getRefColumnListByGroupId(int groupId);
	
	/**
	 * 获取squid中SourceColumn的引用列
	 * @param ref_squid_id
	 * @return
	 */
	public List<ReferenceColumn> getRefColumnListByRefSquidId(int ref_squid_id);
	
	/**
	 * 删除某referenceColumn
	 * @param columnId
	 * @return
	 */
	public boolean delReferenceColumnByColumnId(int columnId, int toSquidId) throws SQLException;
	
	/**
	 * 删除某上游和下游之间的referenceColumn
	 * @param hostSquidId
	 * @param refSquidId
	 * @return
	 * @throws SQLException
	 */
	public boolean deleReferenceColumnBySquidId(int hostSquidId, int refSquidId) throws SQLException;
	
	/**
	 * 获取某squid下的ReferecolumnGroup列表
	 * @param refSquidId
	 * @return
	 */
	public List<ReferenceColumnGroup> getRefColumnGroupListBySquidId(int refSquidId);
	
	/**
	 * 根据refColumn中的上下游squidId获取group对象
	 * @param refSquidId
	 * @param hostSquidId
	 * @return
	 * @throws SQLException
	 */
	public ReferenceColumnGroup getRefColumnGroupBySquidId(int refSquidId, int hostSquidId) throws SQLException;
	
	/**
	 * 给transInputs中的 sourceTransformation 绑定名字
	 * @param transSquidId
	 * @param transColumnId
	 * @return
	 * @throws SQLException
	 */
	public String getRefColumnNameForTrans(int transSquidId, int transColumnId) throws SQLException;
	
	/**
	 * 获取squidId中的group集合数
	 * @param squidId
	 * @return
	 * @throws SQLException
	 */
	public Integer getRefGroupCountForSquidId(int squidId) throws SQLException;
	
	/**
	 * 修改refColumn的是否被引用字段
	 * @param from_transformation_id
	 * @param is_referenced
	 * @return
	 * @throws DatabaseException
	 */
	public boolean updateRefColumnForLink(int from_transformation_id,
            boolean is_referenced) throws DatabaseException;
	
	/**
	 * 修改refColumn
	 * @param refColumn
	 * @return
	 * @throws SQLException
	 */
	public boolean updateRefColumnByObj(ReferenceColumn refColumn) throws SQLException;
	
	/**
	 * 修改refColumn的排序
	 * @param refColumn
	 * @return
	 * @throws DatabaseException
	 */
	public boolean updateRefColumnForOrder(ReferenceColumn refColumn) throws Exception ;
	
	/**
	 * 通过上游squidId获取初始化的refColumn集合
	 * @param squid
	 * @return
	 */
	public List<ReferenceColumn> getInitRefColumnListByRefSquid(int squid);

	/**
	 * 通过当前squidId获取初始化的refColumn集合
	 * @param squid
	 * @return
	 */
	public List<ReferenceColumn> getInitRefColumnListByCurrentSquid(int squid);
}
