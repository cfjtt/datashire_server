package com.eurlanda.datashire.dao;

import com.eurlanda.datashire.entity.Column;
import com.eurlanda.datashire.entity.SourceColumn;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public interface IColumnDao  extends IBaseDao{

	public Column getColumnByKey(String key);
	
	/**
	 * 获取某squid下面的Column集合
	 * @param squidId
	 * @return
	 */
	public List<Column> getColumnListBySquidId(int squidId);
	
	/**
	 * 删除squid下的column
	 * @param squidId
	 * @return
	 * @throws Exception
	 */
	public Integer delColumnListBySquidId(int squidId) throws Exception;

	/**
	 * 把得到的squid中SourceColumn分组,得到想要的集合
	 * @param source_squid_id
	 * @return
	 */
	public Map<Integer, Map<String, Column>> managerSourceColumnForMap(int source_squid_id);

	/**
	 * 根据sourceTable获得Column
	 * 2014-12-8
	 * @author Akachi
	 * @E-Mail zsts@hotmail.com
	 * @param sourceTableId
	 * @return
	 */
	public List<SourceColumn> findSourceColumnBySourceTableId(int sourceTableId);
	
	/**
	 * 根据主键id取得column
	 * @param id
	 * @return
	 * @author bo.dang
	 */
	public Column selectColumnByPK(Boolean inSession, Integer id);
	
	/**
	 * 获取squid当前的transformation和column的依赖
	 * @param squidId
	 * @return
	 */
	public List<Map<String, Object>> getColumnTransBySquidId(int squidId) throws SQLException;

	/**
	 * 根据当前squid的上游squid的column(是否可以通过Map数组的方式存储column对象)
	 * @param squid
	 * @return
     */
	public List<Column> getColumnByTransAndSquidId(int squid);
	/**
	 * 获取当前squid中cdc设置生成的column信息
	 * @param squidId
	 * @return
	 */
	public List<Column> getColumnListForCdc(int squidId);
	
	/**
	 * 获取squid的column的记录数
	 * @param squidId
	 * @return
	 */
	public Integer getColumnCountBySquidId(int squidId) throws SQLException;
	
	/**
	 * 根据名字和squidId查询
	 * @param squidId
	 * @param name
	 * @return
	 */
	public Column getColumnByParams(int squidId, String name);

    /**
     * 判断datamingsquid里面的key是否引用
	 * @param squidId
	 * @return
	 * @throws SQLException
	 */
	public Column getColumnKeyForDataMingSquid(int squidId) throws SQLException;

	public List<Column> getUnlinkedColumnBySquidId(int squidId);

	/**
	 * 根据id获取Column集合
	 * @param ids
	 * @return
	 */
	public List<Column> getColumnListByIds(List<Integer> ids);
}
