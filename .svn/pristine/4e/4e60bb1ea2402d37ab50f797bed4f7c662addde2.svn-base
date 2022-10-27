package com.eurlanda.datashire.dao;

import com.eurlanda.datashire.entity.DataSquid;
import com.eurlanda.datashire.entity.Squid;
import com.eurlanda.datashire.entity.Url;
import com.eurlanda.datashire.utility.DatabaseException;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 * 针对squid的一些简易数据操作
 * @author yi.zhou
 * @date 2014/09/09
 *
 */
public interface ISquidDao extends IBaseDao{
	
	/**
	 * 获取某种类的squid的map信息
	 * @param squidId
	 * @param c
	 * @return
	 * @throws Exception
	 */
	public <T> Map<String, Object> getMapForCond(int squidId, Class<T> c) throws Exception;
	/**
	 * 获取某种类型的squid的实体类包括parameter
	 * @return
	 */
	public <T> T getSquidForCond(int squidId, Class<T> c) throws Exception;

	/**
	 * 通过上游SquidId获取下游所有Squid的对象
	 * @param squidID
	 * @param c
	 * @param <T>
	 * @return
     * @throws Exception
     */
	public <T> List<T> getNextSquidsForSquidID(int squidID, Class<T> c) throws  Exception;
	
	/**
	 * 获取某squidflow下面的一种类型数据信息
	 * @param squidType
	 * @param squidFlowId
	 * @param squidIds
	 * @return
	 * @throws Exception
	 */
	public <T> List<T> getSquidListForParams(int squidType, int squidFlowId, String squidIds, Class<T> c) throws Exception;

	/**
	 * 根据squidName查询某squid
	 * @param squidFlowId
	 * @param name
	 * @return
	 */
	public Squid getSquidByName(int squidFlowId, String name);
	
	/**
	 * 获取所有孤立的squid且不是特殊的类型(特殊包括：Extract、Exception、Report)
	 * @param squidFlowId
	 * @return
	 * @throws SQLException
	 */
	public List<Integer> getSquidIdsForNoLinkBySquidFlow(int squidFlowId) throws SQLException;
	
	/**
     * 修改squid属性
     * @param squid
     * @return
     * @throws SQLException
     */
	public int updateDataSquid(DataSquid squid) throws SQLException;
	
	/**
	 * 获取当前columnId对应的squid名称
	 * @param columnId
	 * @return
	 * @throws SQLException
	 */
	public String getSquidNameByColumnId(int columnId) throws SQLException;
	
	/**
	 * 批量设置DataSquid落地的属性为true
	 * @param ids
	 * @return
	 * @throws DatabaseException
	 */
	public int setPersistenceByIds(String ids) throws DatabaseException;

	/**
	 * 批量设置DataSquid落地目标
	 * @param ids
	 * @return
	 * @throws Exception
     */
	public int setDestSquidByIds(int destSquidId,String ids) throws Exception;
	/**
	 * 获取上游squid集合
	 * @param childId
	 * @return
	 */
	public List<Map<String, Object>> getFromSquidListByChildId(int childId) throws Exception;
	
	/**
	 * 获取web和weibo的抓取信息集合
	 * @param squidId
	 * @return
	 */
	public List<Url> getDSUrlsBySquidId(int squidId);
	
	/**
	 * 更新落地squid的属性
	 * @param squidId
	 * @param squidMap
	 * @return
	 * @throws Exception
	 */
	public int modifyDestinationBySquidId(int squidId, Map<Integer, Integer> squidMap) throws Exception;
	
	/**
	 * map地图的引用替换
	 * @param squidId
	 * @param oldSquidId
	 * @param columnMap
	 * @return
	 * @throws Exception
	 */
	public int modifyTemplateBySquidId(int squidId, int oldSquidId, Map<String, Integer> columnMap) throws Exception;
	
	/**
	 * map地图的引用
	 * @param squidId
	 * @param columnMap
	 * @return
	 * @throws Exception
	 */
	public int modifyTemplateImportBySquidId(int squidId, Map<Integer, Integer> columnMap) throws Exception;
	
	/**
	 * dataMiningSquid的引用（分类映射表）
	 * @param squidId
	 * @param squidMap
	 * @return
	 * @throws Exception
	 */
	public int modifyDataMiningBySquidId(int squidId, Map<Integer, Integer> squidMap) throws Exception;
	
	/**
	 * SourceTableId的引用（分类映射表）
	 * @param squidId
	 * @return
	 * @throws Exception
	 */
	public int modifySourceTableIdBySquidId(int squidId, Map<Integer, Integer> columnMap,Class<?> cz) throws Exception;

	/**
	 * 根据ID获取下游所有的squid
	 * @param id
	 * @return
	 */
	public List<Integer> getNextSquidIdsById(int id) throws SQLException;

	/**
	 * 根据squid id获取上游的squid
	 */
	public List<Squid> getUpSquidIdsById(int squidId) throws SQLException;
}
