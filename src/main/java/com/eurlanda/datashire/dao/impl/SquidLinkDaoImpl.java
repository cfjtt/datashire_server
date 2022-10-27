package com.eurlanda.datashire.dao.impl;

import cn.com.jsoft.jframe.utils.ValidateUtils;
import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.dao.ISquidLinkDao;
import com.eurlanda.datashire.entity.SquidLink;
import com.eurlanda.datashire.enumeration.SquidTypeEnum;
import com.eurlanda.datashire.utility.JsonUtil;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SquidLinkDaoImpl extends BaseDaoImpl implements ISquidLinkDao {
	
	public SquidLinkDaoImpl(){
	}
	
	public SquidLinkDaoImpl(IRelationalDataManager adapter){
		this.adapter = adapter;
	}

	@Override
	public SquidLink getSquidLinkByParams(int fromSquidId, int toSquidId) {
		Map<String, Object> paramMap = new HashMap<String, Object>();
		paramMap.put("from_squid_id", fromSquidId);
		paramMap.put("to_squid_id", toSquidId);
		SquidLink squidLink = adapter.query2Object(true, paramMap, SquidLink.class);
		return squidLink;
	}

	@Override
	public boolean delSquidLinkByParams(int fromSquidId, int toSquidId)
			throws SQLException {
		Map<String, String> paramMap = new HashMap<String, String>();
		paramMap.put("from_squid_id", fromSquidId+"");
		paramMap.put("to_squid_id", toSquidId+"");
		return adapter.delete(paramMap, SquidLink.class)>=0?true:false;
	}

	@Override
	public List<SquidLink> getSquidLinkListByFromSquid(int fromSquidId) {
		Map<String, Object> params = new HashMap<String, Object>();
    	params.put("from_squid_id", fromSquidId);
    	List<SquidLink> link = adapter.query2List2(true, params, SquidLink.class);
    	return link;
	}

	@Override
	public List<SquidLink> getSquidLinkListByToSquid(int toSquidId) {
		Map<String, Object> params = new HashMap<String, Object>();
		params.put("to_squid_id", toSquidId);
		List<SquidLink> link = adapter.query2List2(true, params, SquidLink.class);
		return link;
	}

	@Override
	public List<SquidLink> getSquidLinkListBySquidFlow(int squidFlowId) {
		Map<String, Object> params = new HashMap<String, Object>();
    	params.put("squid_flow_id", squidFlowId);
    	List<SquidLink> link = adapter.query2List2(true, params, SquidLink.class);
    	return link;
	}
	
	@Override
	public List<Integer> orderSquidLinkIds(List<Integer> squidLinkIds) throws SQLException{
			List<Integer> list = new ArrayList<Integer>();
			if (squidLinkIds!=null&&squidLinkIds.size()>0){
				String linkIds = JsonUtil.toJSONString(squidLinkIds);
				if (!ValidateUtils.isEmpty(linkIds)){
					linkIds = linkIds.substring(1);
					linkIds = linkIds.substring(0, linkIds.length()-1);
					String sql = "select id,from_squid_id,to_squid_id from ds_squid_link where id in ("+linkIds+")";

					List<Map<String, Object>> listMap = adapter.query2List(true, sql, null);

					if (listMap!=null&&listMap.size()>0){
						for (Map<String, Object> map : listMap) {
							int id = Integer.parseInt(map.get("ID")+"");
							int toSquidid = Integer.parseInt(map.get("TO_SQUID_ID")+"");
							list.add(id);
							getLinkIdsForList(toSquidid, list, squidLinkIds);
						}
						return list;
					}
				}
			}
    	return null;
    }
    
	@Override
    public List<Integer> orderSquidLinkIdsForSquidFlowId(int squidFlowId) throws SQLException{
    	List<Integer> list = new ArrayList<Integer>();
    	if (squidFlowId>0){
			String sql = "select id,from_squid_id,to_squid_id from ds_squid_link where squid_flow_id="+squidFlowId;

			List<Map<String, Object>> listMap = adapter.query2List(true, sql, null);
			if (listMap!=null&&listMap.size()>0){
				for (Map<String, Object> map : listMap) {
					int id = Integer.parseInt(map.get("ID")+"");
					int toSquidid = Integer.parseInt(map.get("TO_SQUID_ID")+"");
					list.add(id);
					//getLinkIdsForList(toSquidid, list);
				}
				return list;
			}
    	}
    	return null;
    }
    
    /**
     * 根据输入参数在数据中进行查询，并且排序(复制squidFlow时用，不过滤）
    // * @param adapter3
     * @param toSquidId
     * @param links
     * @throws SQLException
     */
    private void getLinkIdsForList(int toSquidId, List<Integer> links) throws SQLException{
    	getLinkIdsForList(toSquidId, links, null);
    }
    
    /**
     * 根据输入参数在数据中进行查询，并且排序(复制squid时用，过滤，只对输入Link集合排序）
     //* @param adapter3
     * @param toSquidId
     * @param links
     * @throws SQLException
     */
    private void getLinkIdsForList(int toSquidId, List<Integer> links, List<Integer> oldLinks) throws SQLException{
		String sql = "select * from ds_squid_link where from_squid_id = "+toSquidId;
			List<SquidLink> squidLinks = adapter.query2List(true, sql, null, SquidLink.class);
			if (squidLinks!=null&&squidLinks.size()>0){
				for (SquidLink squidLink : squidLinks) {
					int tempId = squidLink.getTo_squid_id();
					if ((oldLinks==null||oldLinks.contains(squidLink.getId()))
							&&!links.contains(squidLink.getId())){
						links.add(squidLink.getId());
					}
					getLinkIdsForList(tempId, links, oldLinks);
				}
			}
    }

	@Override
	public List<SquidLink> getSquidLinkBySquidIds(int squidFlowId, String ids) {
		List<SquidLink> squidLinks = adapter.query2List(true, 
				"select * from ds_squid_link " +
				"where squid_flow_id=" + squidFlowId + "" +
				" and (from_squid_id in ("+ids+") or to_squid_id in ("+ids+"))",
				null, SquidLink.class);
		return squidLinks;
	}

	@Override
	public List<SquidLink> getSquidLinkListByCdc(int squidId)
			throws SQLException {
		String sql = "select * from ds_squid_link t1 "
				+ " inner join ds_squid t2 on t1.to_squid_id=t2.id "
				+ " where from_squid_id="+squidId+""
				+ " and t2.squid_type_id not in ("+SquidTypeEnum.REPORT.value()+","+SquidTypeEnum.GISMAP.value()+","+SquidTypeEnum.DESTWS.value()+")";
		List<SquidLink> squidLinkList = adapter.query2List(true, sql, null, SquidLink.class);
		return squidLinkList;
	}

	@Override
	public List<SquidLink> getFormSquidLinkBySquidId(int squidId) throws SQLException {
		String sql="select * from ds_squid_link t1 inner join ds_squid t2 on t1.to_squid_id=t2.id where t1.to_squid_id="+squidId;
		List<SquidLink> squidLinkList = adapter.query2List(true,sql,null,SquidLink.class);
		return squidLinkList;
	}

}
