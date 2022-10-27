package com.eurlanda.datashire.sprint7.service.squidflow;
/**
 * DataShirService DataShir基础数据处理Server
 * 主要处理基础数据方法从socket层映射过来的数据进行相应业务处理调用相对应的process预处理类
 * Title : 
 * Description: 
 * Author :赵春花 2013-9-4
 * update :赵春花 2013-9-4
 * Department :  JAVA后端研发部
 * Copyright : ©2012-2013 悦岚（上海）数据服务有限公司
 */
public interface IDataShirService{
/**
	 * 创建Project对象
	 * 从客户端接收到json数据包
	 * 传ProjectProcess处理类
	 * 对数据包序列化成Project对象集合调用业务处理方法
	 */
	public String createProjects(String info);
	/**
	 * 创建SquidFlow对象
	 * 作用描述：
	 * 从客户端接收到json数据包调用SquidFlowProcess序列成SquidFlow对象集合调用业务处理类
	 * 修改说明：
	 *@param info socket层映射过来的Json数据
	 *@return 返回InfoPacket集合类型的Json串
	 */
	public String createSquidFlows(String info);
	/**
	 * 创建Transformation对象
	 * 作用描述：
	 * 从客户端接收到json数据包
	 * 对数据包序列化成Transformation对象集合调用TransformationProcess处理类
	 * 修改说明：
	 *@param info socket层映射过来的Json数据
	 *@return 返回InfoPacket集合类型的Json串
	 */
	public String createTransformations(String info);
	/**
	 * 创建TransformationLink对象
	 * 作用描述：
	 * 从客户端接收到json数据包调用TransformationLinkProcess处理类
	 * 修改说明：
	 *@param info socket层映射过来的Json数据
	 *@return 返回InfoPacket集合类型的Json串
	 */
	public String createTransformationLinks(String info);
	/**
	 * 创建column对象
	 * 作用描述：
	 * 从客户端接收到json数据包调用ColumnProcess处理类
	 * 修改说明：
	 *@param info socket层映射过来的Json数据
	 *@return 返回InfoPacket集合类型的Json串
	 */
	public String createColumns(String info);
	/**
	 * 创建column对象
	 * 作用描述：
	 * 1、将Json对象传入SquidProcess处理类接收返回结果
	 * 修改说明：
	 *@param info socket层映射过来的Json数据
	 *@return 返回InfoPacket集合类型的Json串
	 */
	public String createDBsourceSquid(String info);
	/**
	 * 创建 stage squid (客户端从工具栏直接拖拽出来的)
	 * 		涉及两张表："DS_SQUID", "DS_DATA_SQUID"
	 * @param info
	 * @return
	 */
	public String createStageSquids(String info);
	/**
	 * 创建ExtractSquid对象
	 * 作用描述：
	 * 从客户端接收到json数据包
	 * 对数据包序列化成ExtractSquid对象集合
	 * 修改说明：
	 *@param info socket层映射过来的Json数据
	 *@return 返回InfoPacket集合类型的Json串
	 */
	public String createExtractSquids(String info);
	/**
	 * 创建DestinationSquid对象
	
	 * 作用描述：
	 * 从客户端接收到json数据包
	 * 对数据包序列化成DestinationSquid对象集合
	 * 修改说明：
	 *@param info socket层映射过来的Json数据
	 *@return 返回InfoPacket集合类型的Json串
	 */
	public String createDestinationSquids(String info);
	/**
	 * 创建Repository对象集合
	 * 作用描述：
	 * 接到Socket传入信息调用RepositoryProcess处理类序列化Json格式信息获得Repository对象信息
	 * RepositoryProcess调用下层业务处理类新增Repository信息并获得信息返回
	 * 修改说明：
	 *@param info socket层映射过来的Json数据
	 *@return 返回InfoPacket集合类型的Json串
	 */
	public String createRepositorys(String info);
	/** 
	 * 修改project对象集合
	 * 作用描述：
	 * 从客户端接收到json数据包
	 * 对数据包序列化成User对象集合
	 * 传入projectProcess调用Update批roject()方法
	 *	调用方法前确保Info对象不为空
	 * 修改说明：
	 *@param info socket层映射过来的Json数据
	 *@return 返回InfoPacket集合类型的Json串
	 */
	public String updateProjects(String info);
	/**
	 * 修改SquidLink对象集合
	 * 作用描述：
	 * 从客户端接收到json数据包
	 * 对数据包序列化成SquidLink对象集合
	 * 传入SquidLinkProcess调用UpdateSquidLink()方法
	 *	调用方法前确保Info对象不为空
	 * 修改说明：
	 *@param info socket层映射过来的Json数据
	 *@return 返回InfoPacket集合类型的Json串
	 */
	public String updateSquidLinks(String info);
	/**
	 * 注册一个新的Repository
	
	 * 作用描述：
	 * 获得Json数据信息调用RepositoryProcess处理类
	 * 修改说明：
	 *@param info socket层映射过来的Json数据
	 *@return 返回InfoPacket集合类型的Json串
	 */
	public String createRegisterRepositorys(String info);
	/**
	 * 修改SquidFlow对象集合
	 * 作用描述：
	 * 从客户端接收到json数据包
	 * 对数据包序列化成SquidFlow对象集合
	 * 传入SquidFlowProcess调用UpdateSquidFlow()方法
	 *	调用方法前确保Info对象不为空
	 * 修改说明：
	 *@param info socket层映射过来的Json数据
	 *@return 返回InfoPacket集合类型的Json串
	 */
	public String updateSquidFlows(String info);
	/**
	 * 修改Transformation的坐标
	 * @param info
	 * @return
	 */
	public String updateTransLocations(String info);
	/**
	 * 修改TransformationLink对象集合
	
	 * 作用描述：
	 * 从客户端接收到json数据包
	 * 对数据包序列化成TransformationLink对象集合
	 * 传入TransformationLinkProcess调用UpdateTransformationLink()方法
	 *	调用方法前确保Info对象不为空
	 * 修改说明：
	 *@param info socket层映射过来的Json数据
	 *@return 返回InfoPacket集合类型的Json串
	 */
	public String updateTransformationLinks(String info);
	/**
	 * 修改Column对象集合
	
	 * 作用描述：
	 * 从客户端接收到json数据包
	 * 对数据包序列化成Column对象集合
	 * 传入Column调用UpdateColumns()方法
	 *	调用方法前确保Info对象不为空
	 * 修改说明：
	 *@param info socket层映射过来的Json数据
	 *@return 返回InfoPacket集合类型的Json串
	 */
	public String updateColumns(String info);
	/**
	 * 修改Squid(只修改位置，前台界面退出时调用)
	 * 作用描述：
	 * 调用SquidProcess预处理类进行验证数据并得到数据返回
	 * 修改说明：
	 *@param info socket层映射过来的Json数据
	 *@return 返回InfoPacket集合类型的Json串
	 */
	public String updateSquids(String info);
	/**
	 * 修改dbsourcesquid对象集合
	 * 作用描述：
	 * 从客户端接收到json数据包
	 * 对数据包序列化成dbsourcesquid对象集合
	 * 传入dbsourcesquid调用Updatedbsourcesquids()方法
	 *	调用方法前确保Info对象不为空
	 * 修改说明：
	 *@param info
	 *@return
	 */
	public String updateDBsourcesquids(String info);
	/**
	 * 修改extractSquid对象集合
	 * 作用描述：
	 * 从客户端接收到json数据包
	 * 对数据包序列化成extractSquid对象集合
	 * 传入extractSquid调用UpdateExtractSquids()方法
	 *	调用方法前确保Info对象不为空
	 * 修改说明：
	 *@param info
	 *@return
	 */
	public String updateExtractSquids(String info);
	/**
	 * 修改stageSquid对象集合
	 * 作用描述：
	 * 从客户端接收到json数据包
	 * 对数据包序列化成stageSquid对象集合
	 * 传入stageSquid调用UpdatestageSquids()方法
	 *	调用方法前确保Info对象不为空
	 * 修改说明：
	 *@param info
	 *@return
	 */
	public String updateStageSquids(String info);
	/**
	 * 修改DestinationSquid对象集合
	 * 作用描述：
	 * 从客户端接收到json数据包
	 * 对数据包序列化成DestinationSquid对象集合
	 * 传入DestinationSquid调用UpdateDestinationSquids()方法
	 *	调用方法前确保Info对象不为空
	 * 修改说明：
	 *@param info
	 *@return
	 */
	public String updateDestinationSquids(String info);
	/**
	 * 修改Repository
	
	 * 作用描述：
	 * 修改说明：
	 *@param info
	 *@return
	 */
	public String updateRepositorys(String info);
	/**
	 * 删除Repository信息统一方法
	 * 作用描述：
	 * 根据传值过来的类型删除相应的数据有关联关系的数据一并清除
	 * 修改说明：
	 *@param cond
	 *@return
	 */
	public String deleteRepositoryObject(String info);
	/**
	 * 云端删除项目接口
	 */
	public String cloudDeleteRepositoryObject(String info);
	/**
	 * 根据ProjectID获得SquidFlow信息
	 * 作用描述：
	 * 根据客户端传入的ProjectID获得SquidFlow对象集合信息
	
	 * 修改说明：
	 *@param info
	 *@return
	 */
	public String queryAllSquidFlows(String info);
	/**
	 * 获得Project
	 * 作用描述：
	 * 根据客户端传入的Repository对象获得Name信息查询相对应路径下的物理数据查询所有Project对象集合返回
	 * 修改说明：
	 *@param info
	 *@return
	 */
	public String queryAllProjects(String info);
	/**
	 * 获得所有的Repository对象集合
	 * 作用描述：
	 * 根据客户端传入的TEAMID信息查询相对应的Repostory对象集合信息返回
	 * 修改说明：
	 *@param info
	 *@return
	 */
	public String querylAllRepositorys(String info);
	/**
	 * 创建ExtractSquid
	 * 作用描述：
	 * 根据传入的ExtractSquid对象和SquidLink对象创建ExtractSquid信息并将相应的关联数据返回
	 * 修改说明：
	 *@param info
	 *@return
	 */
	public String createMoreExtractSquid(String info);
	/** 创建目标列和对应的虚拟变换(从引用列拖拽导入) */
	public String createMoreStageColumn(String info);
	/**
	 * 创建StageSquid信息
	 * 作用描述：
	 * 接收到Socket层传入的JSon信息传入DataShirProcess预处理类的createMoreStageSquidAndSquidLink()验证数据完整性
	 * 修改说明：
	 *@param info
	 *@return
	 */
	public String createMoreStageSquidLink(String info);
	/**
	 * 从datasquid的目标列选择若干column放到stagesquid上 
	 * @param info
	 * @return
	 */
	public String drag2StageSquid(String info);
	/**
	 * 根据SquidFlowID查询Squid And SquidLink
	 * 作用描述：
	 * 接收到Socket层传入的JSon信息传入RepositoryProcess预处理类进行数据序列化以及数据验证
	 * 该类使用数据推送机制
	 * 修改说明：
	 *@param info
	 *@return
	 */
	public String querySquidAndSquidLink(String info);
	/**
	 * 获得变幻序列中的数据
	 * 作用描述：
	 * 修改说明：
	 *@author
	 *@param info
	 *@return
	 */
	public String queryRuntimeData(String info);
	/**
	 * 得数据
	 * 作用描述：
	 * 修改说明：
	 *@param info
	 *@return
	 */
	public String queryData(String info);
	/**
	 * 根据SquidFlowId调用变换引擎接口，运行一个SquidFlow
	 * @param info
	 * @return
	 */
	public String runSquidFlow(String info);
	/**
	 * 根据前端传入的信息进行SquidJoin的创建
	 * @author lei.bin
	 * @return SpecialSquidJoin
	 */
	public String createSquidJoin(String info);
	/**
	 * 根据前端传入的信息进行SquidJoin的更新
	 * 
	 * @author lei.bin
	 * @return SpecialSquidJoin
	 */
	public String updateSquidJoin(String info);
	/**
	 * 创建SquidLink同时添加Join信息
	
	 * 作用描述：
	 * 创建SquidLink，触发创建squidJoin
	
	 * 修改说明：
	 *@param info
	 *@return
	 */
	public String createSquidLinks(String info);
	/**
	 * 根据前端传送的dbsquid信息,查询数据源信息，存储DS_SOURCE_TABLE，DS_SOURCE_COLUMN,
	 * 并且将表和列的信息返回前端
	 * @param info
	 * @author binlei
	 * @return
	 * @throws Exception 
	 */
	public String createConnect(String info) throws Exception;
	/**
	 * 拖拽source column集合到空的extract，并生成transformation (或link)
	 * @param info
	 * @return
	 */
	public String drag2EmptyExtractSquid(String info);
	/**
	 * 拖拽source column集合到空白区，并生成extract、link和transformation
	 * @param info
	 * @return
	 */
	public String drag2NewExtractSquid(String info);
	/**
	 * 拖动transformation打断transformationlink
	 * 先删除原先的link,然后创建2个新的link
	 * @return
	 */
	public String drapTransformationAndLink(String info);
	/**
	 * 根据前端传送过来的ReferenceColumnGroup进行批量更新
	 * @param info
	 * @return
	 */
	public String updateReferenceColumnGroup(String info);

	/**
	 * 删除Squidflow内所有的Squid
	 * @param info
	 * @return
     */
	public String deleteAllSquidsInSquidflow(String info);
}