package com.eurlanda.datashire.sprint7.service.squidflow;

import com.eurlanda.datashire.entity.DBDestinationSquid;
import com.eurlanda.datashire.entity.DBSourceSquid;
import com.eurlanda.datashire.entity.DbSquid;
import com.eurlanda.datashire.entity.Squid;
import com.eurlanda.datashire.entity.StageSquid;
import com.eurlanda.datashire.entity.TableExtractSquid;
import com.eurlanda.datashire.entity.operation.ExtractSquidAndSquidLink;
import com.eurlanda.datashire.entity.operation.TransformationAndCloumn;
import com.eurlanda.datashire.sprint7.packet.InfoPacket;
import com.eurlanda.datashire.utility.EnumException;
import com.eurlanda.datashire.utility.ReturnValue;

import java.util.List;

/** Squid业务处理类
 * Title : 
 * Description: 
 * Author :赵春花 2013-10-29
 * update :赵春花 2013-10-29
 * Department :  JAVA后端研发部
 * Copyright : ©2012-2013 悦岚（上海）数据服务有限公司
 */
public interface ISquidService{
/**
	 * 根据传入的ExtractSquid,SquidLink创建ExtractSquid信息
	 * 推送
	 * ExtractSquidAndSquidLink对象
	 * 作用描述：
	 * 1、获得extractSquid信息
	 * 2、获得squidLink信息
	 * 3、创建ExtractSquid获得id
	 * 4、获得ExtractSquid的TableName
	 * 5、根据SquidLink的来源ID查询源的DBSourceSquid查询出连接信息切换连接信息查询出该表的列信息
	 * 6、查询SourceTable表是否有储存该Squid表名（是：获得相对应的ID；否：信息再获得ID）
	 * 7、根据查询的SourceColumn信息新增目标Column（squidID为目标id，sourceTableId为查询或新增的ID）
	 * 8、查询SourceSquid是否有Column信息（否：创建Column信息）
	 * 9、新增Transformation信息
	 * 10、新增TransformationLink信息
	 * 11、查询ExtractSquid相关信息组装推送
	 * 验证：
	 * extractSquidAndSquidLinks不能为空
	 * 修改说明：
	 *@param extractSquidAndSquidLink  extractSquidAndSquidLink集合对象
	 *@param out 异常处理
	 *@return
	 * @throws EnumException 
	 */
	
	public  void createMoreExtractSquid(
			List<ExtractSquidAndSquidLink> extractSquidAndSquidLinks,
			String token, ReturnValue out);
	/**
	 * 创建DestinationSquid对象集合
	 * 作用描述：
	 * stageSquid对象集合不能为空且必须有数据
	 * stageSquid对象集合里的每个Key必须有值
	 * 根据传入的stageSquid对象集合创建stageSquid对象
	 * 创建成功——根据stageSquid集合对象里的Key查询相对应的ID
	 * 修改说明：
	 *@param dbDestinationSquids dbDestinationSquid集合对象
	 *@param out 异常处理
	 *@return
	 */
	public List<InfoPacket> createDestinationSquids(List<DBDestinationSquid> dbDestinationSquids,ReturnValue out);
	/**
	 * DestinationSquid
	 * 作用描述：
	 * 修改DestinationSquid对象集合
	 * 调用方法前确保DestinationSquid对象集合不为空，并且DestinationSquid对象集合里的每个DestinationSquid对象的Id不为空
	 * 修改说明：
	 *@param dbDestinationSquids dbDestinationSquid集合对象
	 *@param out 异常处理
	 *@return
	 */
	public List<InfoPacket> updateDestinationSquids(List<DbSquid> dbDestinationSquids,ReturnValue out);
	/**
	 * 根据传入的column对象集，Transformation对象集创建相对应的数据
	 * 作用描述：
	 * 验证：
	 * 调用方法前确保TransformationAndCloumn对象集合不为空
	 * 调用根据获得的对象集调用批量新增的方法，创建错误返回
	 * 成功：根据key查询ID返回InfoPacket集合对象
	 * 修改说明：
	 *@param transformationAndCloumns transformationAndCloumn集合对象
	 *@param out 异常处理
	 *@return
	 */
	public List<InfoPacket> createMoreStageSquid(
			List<TransformationAndCloumn> transformationAndCloumns,
			ReturnValue out);
	/**
	 * 修改ExtractSquid
	 * 作用描述：
	 * 修改extractSquid对象集合
	 * 调用方法前确保extractSquid对象集合不为空，并且extractSquid对象集合里的每个extractSquid对象的Id不为空
	 * 修改说明：
	 *@param extractSquids extractSquid集合对象
	 *@param out 异常处理
	 *@return
	 */
	public List<InfoPacket> updateExtractSquids(List<TableExtractSquid> extractSquids,ReturnValue out);
	/**
	 * 
	 * 作用描述：
	 * 修改stageSquid对象集合
	 * 调用方法前确保stageSquid对象集合不为空，并且stageSquid对象集合里的每个stageSquid对象的Id不为空
	 * 修改说明：
	 *@param stageSquids stageSquid集合对象
	 *@param out 异常处理
	 *@return
	 */
	public List<InfoPacket> updateStageSquids(List<StageSquid> stageSquids,ReturnValue out);
	/**
	 * 修改Squid
	 * 作用描述：
	 * 修改说明：
	 *@param squids squid对象集合
	 *@param out 异常处理
	 *@return
	 */
	public boolean updateSquids(List<Squid> squids,ReturnValue out);
	/**
	 * 修改DbsourceSquid
	 * 作用描述：
	 * 修改dbSourceSquids对象集合
	 * 调用方法前确保dbSourceSquids对象集合不为空，并且dbSourceSquids对象集合里的每个dbSourceSquids对象的Id不为空
	 * 根据SquidId查询是否保存了connection信息，储存的情况下进行修改连接信息，没有的情况下进行新增
	 * 修改说明：
	 *@param dbSourceSquids dbSourceSquid集合对象
	 *@param out 异常处理
	 *@return
	 */
	public List<DbSquid> updatedbSourceSquids(List<DbSquid> dbSourceSquids,ReturnValue out);
	/**
	 * 创建dbSourceSquid对象集合
	 * 作用描述：
	 * dbSourceSquid对象集合不能为空且必须有数据
	 * dbSourceSquid对象集合里的每个Key必须有值
	 * 根据传入的dbSourceSquid对象集合创建dbSourceSquid对象
	 * 创建成功——根据dbSourceSquid集合对象里的Key查询相对应的ID
	 * 修改说明：
	 *@param dbSourceSquids dbSourceSquid集合对象
	 *@param out 异常处理
	 *@return
	 */
	public List<InfoPacket> createDBSourceSquids(List<DBSourceSquid> dbSourceSquids,ReturnValue out);
	/**
	 * 创建extractSquid对象集合
	 * 作用描述：
	 * extractSquid对象集合不能为空且必须有数据
	 * extractSquid对象集合里的每个Key必须有值
	 * 根据传入的extractSquid对象集合创建extractSquid对象
	 * 创建成功——根据extractSquid集合对象里的Key查询相对应的ID
	 * 修改说明：
	 *@param extractSquids extractSquid集合对象
	 *@param out 异常处理
	 *@return
	 */
	public List<InfoPacket> createExtractSquids(List<TableExtractSquid> extractSquids,ReturnValue out);
	
}