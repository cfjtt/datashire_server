package com.eurlanda.datashire.sprint7.service.squidflow;

import com.eurlanda.datashire.entity.Column;
import com.eurlanda.datashire.entity.TransformationLink;
import com.eurlanda.datashire.sprint7.packet.InfoPacket;
import com.eurlanda.datashire.utility.ReturnValue;

import java.util.List;

/**
 * 
 * Column业务处理类
 * Title : 
 * Description: 
 * Author :赵春花 2013-10-28
 * update :赵春花 2013-10-28
 * Copyright : ©2012-2013 悦岚（上海）数据服务有限公司
 */
public interface IColumnService{
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
	public List<InfoPacket> createColumns(List<Column> columns, ReturnValue out);
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
	public List<InfoPacket> updateColumns(List<Column> columns,ReturnValue out);

	/**
	 * 根据左边referenceColumn 与column匹配，连上线
	 * @param info
	 * @param out
	 * @return
	 */
	public List<TransformationLink> autoMatchColumnByNameAndType(String info, ReturnValue out);
}