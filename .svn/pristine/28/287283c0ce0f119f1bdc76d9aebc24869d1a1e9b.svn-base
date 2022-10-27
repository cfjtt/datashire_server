package com.eurlanda.datashire.sprint7.plug;

import com.eurlanda.datashire.entity.*;
import com.eurlanda.datashire.entity.operation.DataEntity;
import com.eurlanda.datashire.enumeration.DataStatusEnum;
import org.apache.log4j.Logger;

import java.sql.Timestamp;
import java.util.List;

/**
 * 将Object对象转换成DAtaEntity对象
 * 将Object转换成DataEntity对象进行下次业务处理
 * <p>
 * Title : 
 * </p>
 * <p>
 * Description: 
 * </p>
 * <p>
 * Author :赵春花 2013-9-4
 * </p>
 * <p>
 * update :赵春花 2013-9-4
 * </p>
 * <p>
 * Department :  JAVA后端研发部
 * </p>
 * Copyright : ©2012-2013 悦岚（上海）数据服务有限公司
 * </p>
 * @param <T>
 */
public class GetParam {
	static Logger logger = Logger.getLogger(ColumnPlug.class);// 记录日志

	/**
	 * 将Repository对象转换为DataEntity对象
	 * <p>
	 * 作用描述：
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 *@param repository repository对象
	 *@param dataEntitys DataEntity集合
	 */
	public  void getRepository(Repository repository, List<DataEntity> dataEntitys) {
		logger.debug(String.format("getRepository-repository=%s", repository));
		DataEntity dataEntity = null;
		String attributeValue = null;
		int attributeIntValue = 0;
		//team_id
		attributeIntValue = repository.getTeam_id();
		dataEntity = new DataEntity("TEAM_ID", DataStatusEnum.INT, attributeIntValue);
		dataEntitys.add(dataEntity);
		// 唯一标识
		attributeValue =  repository.getKey();
		if(attributeValue!=null){
			attributeValue = "'"+attributeValue+"'";
			dataEntity = new DataEntity("KEY", DataStatusEnum.STRING, attributeValue);
			dataEntitys.add(dataEntity);
		}			
		//Name
		attributeValue = repository.getName();
		if(attributeValue!=null){
			attributeValue = "'"+attributeValue+"'";
		}
		dataEntity = new DataEntity("NAME", DataStatusEnum.STRING, attributeValue);
		dataEntitys.add(dataEntity);
		//描述
		attributeValue =  repository.getDescription();
		if(attributeValue!=null){
			attributeValue = "'"+attributeValue+"'";
			dataEntity = new DataEntity("DESCRIPTION", DataStatusEnum.STRING, attributeValue);
			dataEntitys.add(dataEntity);
		}
		//
		attributeIntValue = repository.getRepository_type();
		dataEntity = new DataEntity("TYPE", DataStatusEnum.INT, attributeIntValue);
		dataEntitys.add(dataEntity);
		//STATUS_id
		attributeIntValue = repository.getStatus();
		if(attributeIntValue==0){
			attributeIntValue = 1;
		}
		dataEntity = new DataEntity("STATUS_ID", DataStatusEnum.INT, attributeIntValue);
		dataEntitys.add(dataEntity);
		//数据库名
		attributeValue =  repository.getRepository_db_name();
		if(attributeValue!=null){
			attributeValue = "'"+attributeValue+"'";
		}
		dataEntity = new DataEntity("REPOSITORY_DB_NAME", DataStatusEnum.STRING, attributeValue);
		dataEntitys.add(dataEntity);
		
		logger.debug(String.format("getRepository-DataEntityList=%s", dataEntitys));
	}
	
	/**
	 * 将Project对象转换成DataEntity对象集合
	 * <p>
	 * 作用描述：
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 *@param project  project对象
	 *@param dataEntitys dataEntity集合
	 */
	public  void getProject(Project project, List<DataEntity> dataEntitys) {
		logger.debug(String.format("getProjectr-project=%s", project));
		DataEntity dataEntity = null;
		String attributeValue = null;
		// 名称
		attributeValue = project.getName();
		if(attributeValue!=null && !attributeValue.equals("")){
			attributeValue = "'"+attributeValue+"'";
			dataEntity = new DataEntity("NAME", DataStatusEnum.STRING,attributeValue );
			dataEntitys.add(dataEntity);
		}
		

		// 默认系统时间
		Timestamp date = new Timestamp(System.currentTimeMillis());

		// 描述
		attributeValue =  project.getDescription();
		if(attributeValue!=null && !attributeValue.equals("")){
			attributeValue = "'"+attributeValue+"'";
			dataEntity = new DataEntity("DESCRIPTION", DataStatusEnum.STRING,attributeValue);
			dataEntitys.add(dataEntity);
		}
		


		// 修改时间
		dataEntity = new DataEntity("MODIFICATION_DATE", DataStatusEnum.DATE,"'"+date+"'");
		dataEntitys.add(dataEntity);

		// 创建人
		int creator =  project.getCreator();
		//if(attributeValue!=null && !attributeValue.equals("")){
			//attributeValue = "'"+attributeValue+"'";
			dataEntity = new DataEntity("CREATOR", DataStatusEnum.INT, creator);
			dataEntitys.add(dataEntity);
		//}
		

		// 唯一标识
		attributeValue =   project.getKey();
		if(attributeValue!=null && !attributeValue.equals("")){
			attributeValue = "'"+attributeValue+"'";
			dataEntity = new DataEntity("KEY", DataStatusEnum.STRING,attributeValue);
			dataEntitys.add(dataEntity);
		}
		
		
		logger.debug(String.format("getProjectr-project=%s", project));
	}
	
	
	/**
	 * 将squidLink对象转换成DataEntity对象集合
	 * <p>
	 * 作用描述：
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 *@param squidLink squidLink对象
	 *@param dataEntitys dataEntity集合
	 */
	public  void getSquidLink(SquidLink squidLink, List<DataEntity> dataEntitys) {
		DataEntity dataEntity = null;
		String attributeValue = null;
		int attributeIntValue = 0;
		// 源SquidID
		attributeIntValue = squidLink.getFrom_squid_id();
		dataEntity = new DataEntity("FROM_SQUID_ID", DataStatusEnum.INT, attributeIntValue);
		dataEntitys.add(dataEntity);
		
		// 面板id
		attributeIntValue = squidLink.getSquid_flow_id();
		dataEntity = new DataEntity("SQUID_FLOW_ID", DataStatusEnum.INT, attributeIntValue);
		dataEntitys.add(dataEntity);
		
		// 目标squid id
		attributeIntValue = squidLink.getTo_squid_id();
		dataEntity = new DataEntity("TO_SQUID_ID", DataStatusEnum.INT, attributeIntValue);
		dataEntitys.add(dataEntity);
		
		//箭头样式
		attributeIntValue = squidLink.getArrows_style();
		dataEntity = new DataEntity("ARROWS_STYLE", DataStatusEnum.INT, attributeIntValue);
		dataEntitys.add(dataEntity);
		
		//结束X
		attributeIntValue = squidLink.getEnd_x();
		dataEntity = new DataEntity("END_X", DataStatusEnum.DOUBLE, attributeIntValue);
		dataEntitys.add(dataEntity);
		
		//结束Y
		attributeIntValue = squidLink.getEnd_y();
		dataEntity = new DataEntity("END_Y", DataStatusEnum.DOUBLE, attributeIntValue);
		dataEntitys.add(dataEntity);
		
		//靠近结束的X
		attributeIntValue = squidLink.getEndmiddle_x();
		dataEntity = new DataEntity("ENDMIDDLE_X", DataStatusEnum.DOUBLE, attributeIntValue);
		dataEntitys.add(dataEntity);
		
		//靠近结束的Y
		attributeIntValue = squidLink.getEndmiddle_y();
		dataEntity = new DataEntity("ENDMIDDLE_Y", DataStatusEnum.DOUBLE, attributeIntValue);
		dataEntitys.add(dataEntity);
		//线的颜色
		attributeValue = squidLink.getLine_color();
		if (attributeValue != null && attributeValue.equals("")) {
			attributeValue = "'" + attributeValue + "'";
			dataEntity = new DataEntity("LINE_COLOR", DataStatusEnum.STRING, attributeValue);
			dataEntitys.add(dataEntity);
		}
		
		//线的样式
		attributeIntValue = squidLink.getLine_type();
		dataEntity = new DataEntity("LINE_TYPE", DataStatusEnum.INT, attributeIntValue);
		dataEntitys.add(dataEntity);
		
		//开始X
		attributeIntValue = squidLink.getStart_x();
		dataEntity = new DataEntity("START_X", DataStatusEnum.DOUBLE, attributeIntValue);
		dataEntitys.add(dataEntity);
		
		//开始Y
		attributeIntValue = squidLink.getStart_y();
		dataEntity = new DataEntity("START_Y", DataStatusEnum.DOUBLE, attributeIntValue);
		dataEntitys.add(dataEntity);
		
		//靠近开始的X
		attributeIntValue = squidLink.getStartmiddle_x();
		dataEntity = new DataEntity("STARTMIDDLE_X", DataStatusEnum.DOUBLE, attributeIntValue);
		dataEntitys.add(dataEntity);
		
		//靠近开始的Y
		attributeIntValue = squidLink.getStartmiddle_y();
		dataEntity = new DataEntity("STARTMIDDLE_Y", DataStatusEnum.DOUBLE, attributeIntValue);
		dataEntitys.add(dataEntity);
		
		// 唯一标识
		attributeValue = squidLink.getKey();
		if (attributeValue != null) {
			attributeValue = "'" + attributeValue + "'";
		}
		dataEntity = new DataEntity("KEY", DataStatusEnum.STRING, attributeValue);
		dataEntitys.add(dataEntity);
	}
	
	/**
	 * 将SquidFlow对象转换成DataEntity对象集合
	 * <p>
	 * 作用描述：
	 * 将SquidFlow对象转换成DataEntity对象
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 *@param squidFlow squidFlow对象
	 *@param dataEntitys dataEntity集合
	 */
	public  void getSquidFlow(SquidFlow squidFlow, List<DataEntity> dataEntitys) {
		logger.debug(String.format("getsquidFlow-squidFlow=%s", squidFlow));
		DataEntity dataEntity = null;
		String attributeValue = null;
		int attributeIntValue = 0;
		// 默认系统时间
		Timestamp date = new Timestamp(System.currentTimeMillis());

		// 1设置KEY
		// 唯一标识
		attributeValue = squidFlow.getKey();
		if (attributeValue != null) {
			attributeValue = "'" + attributeValue + "'";
			dataEntity = new DataEntity("KEY", DataStatusEnum.STRING, attributeValue);
			dataEntitys.add(dataEntity);
		}
		
		
		// 2名字
		attributeValue = squidFlow.getName();
		if (attributeValue != null) {
			attributeValue = "'" + attributeValue + "'";
			dataEntity = new DataEntity("NAME", DataStatusEnum.STRING, attributeValue);
			dataEntitys.add(dataEntity);
		}
		


		//修改时间
		dataEntity = new DataEntity("MODIFICATION_DATE", DataStatusEnum.DATE, "'"+date+"'");
		dataEntitys.add(dataEntity);

		// 4创建人
		attributeValue = squidFlow.getCreator();
		if (attributeValue != null) {
			attributeValue = "'" + attributeValue + "'";
			dataEntity = new DataEntity("CREATOR", DataStatusEnum.STRING, attributeValue);
			dataEntitys.add(dataEntity);
		}
	

		// 5项目ID
		attributeIntValue = squidFlow.getProject_id();
		dataEntity = new DataEntity("PROJECT_ID", DataStatusEnum.INT, attributeIntValue);
		dataEntitys.add(dataEntity);
		

		// 6描述
		attributeValue = squidFlow.getDescription();
		if (attributeValue != null) {
			attributeValue = "'" + attributeValue + "'";
			dataEntity = new DataEntity("DESCRIPTION", DataStatusEnum.STRING, attributeValue);
			dataEntitys.add(dataEntity);
		}
	
	}
	
	/**
	 * 将Transformation转换成DataEntity对象集合
	 * <p>
	 * 作用描述：
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 *@param transformation transformation对象
	 *@param dataEntitys dataEntity集合
	 */
	public  void getTransformation(Transformation transformation, List<DataEntity> dataEntitys) {
		logger.debug(String.format("Transformation-transformation=%s", transformation));
		DataEntity dataEntity = null;
		String attributeValue = null;
		int attributeIntValue = 0;
		// SquidID
		attributeIntValue = transformation.getSquid_id();
		dataEntity = new DataEntity("SQUID_ID", DataStatusEnum.INT, attributeIntValue);
		dataEntitys.add(dataEntity);

		// 类型
			attributeIntValue = transformation.getTranstype();
		dataEntity = new DataEntity("TRANSFORMATION_TYPE_ID", DataStatusEnum.INT, attributeIntValue);
		dataEntitys.add(dataEntity);

		// X坐标
		attributeIntValue = transformation.getLocation_x();
		dataEntity = new DataEntity("LOCATION_X", DataStatusEnum.DOUBLE, attributeIntValue);
		dataEntitys.add(dataEntity);

		// Y坐标
		attributeIntValue = transformation.getLocation_y();
		dataEntity = new DataEntity("LOCATION_Y", DataStatusEnum.INT,attributeIntValue);
		dataEntitys.add(dataEntity);

		// 列
		attributeIntValue = transformation.getColumn_id();
		dataEntity = new DataEntity("COLUMN_ID", DataStatusEnum.INT, attributeIntValue);
		dataEntitys.add(dataEntity);
		
		// 唯一标识
		attributeValue = transformation.getKey();
		if (attributeValue != null) {
			attributeValue = "'" + attributeValue + "'";
		}
		dataEntity = new DataEntity("KEY", DataStatusEnum.STRING, attributeValue);
		dataEntitys.add(dataEntity);
	}
	
	/**
	 * 将TransformationLink转换成DataEntity对象集合
	 * <p>
	 * 作用描述：
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 *@param transformationLink 对象
	 *@param dataEntitys dataEntity集合
	 */
	public  void getTransformationLink(TransformationLink transformationLink, List<DataEntity> dataEntitys) {
		logger.debug(String.format("getTransformationLink-transformationLink=%s", transformationLink));
		DataEntity dataEntity = null;
		String attributeValue = null;
		int attributeIntValue = 0;
		// 来自哪
		attributeIntValue = transformationLink.getFrom_transformation_id();
		dataEntity = new DataEntity("FROM_TRANSFORMATION_ID", DataStatusEnum.INT,
				attributeIntValue);
		dataEntitys.add(dataEntity);

		// 放到哪
		attributeIntValue = transformationLink.getTo_transformation_id();
		dataEntity = new DataEntity("TO_TRANSFORMATION_ID", DataStatusEnum.INT,
				attributeIntValue);
		dataEntitys.add(dataEntity);

		// 命令
		attributeIntValue = transformationLink.getIn_order();
		dataEntity = new DataEntity("IN_ORDER", DataStatusEnum.INT,
				attributeIntValue);
		dataEntitys.add(dataEntity);

		// 唯一标识
		attributeValue = transformationLink.getKey();
		if(attributeValue!=null){
			attributeValue = "'"+attributeValue+"'";
		}
		dataEntity = new DataEntity("KEY", DataStatusEnum.STRING, attributeValue);
		dataEntitys.add(dataEntity);
	}
	
	/**
	 * Column对象转换成DataEntity对象集合
	 * <p>
	 * 作用描述：
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 *@param column column对象
	 *@param dataEntitys dataEntity集合
	 */
	public  void getColumn(Column column, List<DataEntity> dataEntitys) {
		logger.debug(String.format("getColumn-column=%s", column));
		DataEntity dataEntity = null;
		String attributeValue = null;
		int attributeIntValue = 0;
		//相对位置
		attributeIntValue = column.getRelative_order();
		dataEntity = new DataEntity("RELATIVE_ORDER", DataStatusEnum.INT, attributeIntValue);
		dataEntitys.add(dataEntity);
		// SquidID
		attributeIntValue = column.getSquid_id();
		dataEntity = new DataEntity("SQUID_ID", DataStatusEnum.INT, attributeIntValue);
		dataEntitys.add(dataEntity);

		// 名称
		attributeValue = column.getName();
		if (attributeValue != null) {
			attributeValue = "'" + attributeValue + "'";
			dataEntity = new DataEntity("NAME", DataStatusEnum.STRING, attributeValue);
			dataEntitys.add(dataEntity);
		}
		

		// 类型
		dataEntitys.add(new DataEntity("DATA_TYPE", DataStatusEnum.STRING, column.getData_type()));


		// 是否为空
		attributeIntValue = column.isNullable() ? 1 :0 ;
		dataEntity = new DataEntity("NULLABLE", DataStatusEnum.BOOLEAN, attributeIntValue);
		dataEntitys.add(dataEntity);

		// 唯一标识
		attributeValue = column.getKey();
		if (attributeValue != null) {
			attributeValue = "'" + attributeValue + "'";
		}
		dataEntity = new DataEntity("KEY", DataStatusEnum.STRING, attributeValue);
		dataEntitys.add(dataEntity);
		// 表名
		//attributeIntValue = column.getDb_source_table_id();
		//dataEntity = new DataEntity("DB_SOURCE_TABLE_ID", DataStatusEnum.INT, attributeIntValue);
		//dataEntitys.add(dataEntity);
	}
	
	/**将squid对象转换成DataEntity对象
	 * 
	 * <p>
	 * 作用描述：
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 *@param squid squid对象
	 *@param dataEntitys DataEntity集合
	 */
	public static final void getSquid(DataSquid squid, List<DataEntity> dataEntitys) {
		//key
		if(squid.getKey()!=null){
			dataEntitys.add(new DataEntity("KEY", DataStatusEnum.STRING, "'"+squid.getKey()+"'"));
		}
		//SquidFlowID
		dataEntitys.add(new DataEntity("SQUID_FLOW_ID", DataStatusEnum.INT, squid.getSquidflow_id()));
		//名称
		if(squid.getName()!=null){
			dataEntitys.add(new DataEntity("NAME", DataStatusEnum.STRING, "'"+squid.getName()+"'"));
		}
		//描述
		if(squid.getDescription()!=null){
			dataEntitys.add(new DataEntity("DESCRIPTION", DataStatusEnum.STRING, "'"+squid.getDescription()+"'"));
		}
		//squid类型
		dataEntitys.add(new DataEntity("SQUID_TYPE_ID", DataStatusEnum.INT, squid.getSquid_type()));
		//location_x
		dataEntitys.add(new DataEntity("LOCATION_X", DataStatusEnum.INT, squid.getLocation_x()));
		//location_y
		dataEntitys.add(new DataEntity("LOCATION_Y", DataStatusEnum.INT, squid.getLocation_y()));
		//SQUID_HEIGHT
		dataEntitys.add(new DataEntity("SQUID_HEIGHT", DataStatusEnum.INT, squid.getSquid_height()));
		//squid_width
		dataEntitys.add(new DataEntity("SQUID_WIDTH", DataStatusEnum.INT, squid.getSquid_width()));
		//table_name
		dataEntitys.add(new DataEntity("TABLE_NAME", DataStatusEnum.STRING, "'"+squid.getTable_name()+"'"));
		//is_show_all是否显示非变换列
		dataEntitys.add(new DataEntity("IS_SHOW_ALL", DataStatusEnum.INT, squid.isIs_show_all() ? 1 :0 ));
		// source_is_show_all 是否显示源非变换列
		dataEntitys.add(new DataEntity("SOURCE_IS_SHOW_ALL", DataStatusEnum.INT, squid.isSource_is_show_all() ? 1 : 0));
		//transformation_group_x
		dataEntitys.add(new DataEntity("COLUMN_GROUP_X", DataStatusEnum.INT, squid.getTransformation_group_x()));
		//transformation_group_y
		dataEntitys.add(new DataEntity("COLUMN_GROUP_Y", DataStatusEnum.INT, squid.getTransformation_group_y()));
		//source_transformation_group_x
		dataEntitys.add(new DataEntity("REFERENCE_GROUP_X", DataStatusEnum.INT, squid.getSource_transformation_group_x()));
		//source_transformation_group_y
		dataEntitys.add(new DataEntity("REFERENCE_GROUP_Y", DataStatusEnum.INT, squid.getSource_transformation_group_y()));
	}

	/**将sourceSquid对象转换成DataEntity对象
	 * 
	 * <p>
	 * 作用描述：
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 *@param squid squid对象
	 *@param dataEntitys DataEntity集合
	 */
	public  void getSourceSquid(DbSquid squid, List<DataEntity> dataEntitys) {
		logger.debug(String.format("getSourceSquid-squid=%s", squid));
		DataEntity dataEntity = null;
		String attributeValue = null;
		int attributeIntValue = 0;
		//key
		attributeValue = squid.getKey();
		if(attributeValue!=null){
			attributeValue ="'"+attributeValue+"'";
			dataEntity = new DataEntity("KEY", DataStatusEnum.STRING, attributeValue);
			dataEntitys.add(dataEntity);
			
		}
		
		//SquidFlowID
		attributeIntValue = squid.getSquidflow_id();
		dataEntity = new DataEntity("SQUID_FLOW_ID", DataStatusEnum.INT, attributeIntValue==0?1:attributeIntValue);
		dataEntitys.add(dataEntity);
		//名称
		attributeValue = squid.getName();
		if(attributeValue!=null){
			attributeValue ="'"+attributeValue+"'";
			dataEntity = new DataEntity("NAME", DataStatusEnum.STRING, attributeValue);
			dataEntitys.add(dataEntity);
		}
		
		//描述
		attributeValue = squid.getDescription();
		if(attributeValue!=null){
			attributeValue ="'"+attributeValue+"'";
			dataEntity = new DataEntity("DESCRIPTION", DataStatusEnum.STRING, attributeValue);
			dataEntitys.add(dataEntity);
		}
	
		//squid类型
			attributeIntValue = squid.getSquid_type();
			dataEntity = new DataEntity("SQUID_TYPE_ID", DataStatusEnum.INT, attributeIntValue);
			dataEntitys.add(dataEntity);
	
		//location_x
		attributeIntValue = squid.getLocation_x();
		dataEntity = new DataEntity("LOCATION_X", DataStatusEnum.INT, attributeIntValue);
		dataEntitys.add(dataEntity);
		//location_y
		attributeIntValue = squid.getLocation_y();
		dataEntity = new DataEntity("LOCATION_Y", DataStatusEnum.INT, attributeIntValue);
		dataEntitys.add(dataEntity);
		//SQUID_HEIGHT
		attributeIntValue = squid.getSquid_height();
		dataEntity = new DataEntity("SQUID_HEIGHT", DataStatusEnum.INT, attributeIntValue);
		dataEntitys.add(dataEntity);
		//squid_width
		attributeIntValue = squid.getSquid_width();
		dataEntity = new DataEntity("SQUID_WIDTH", DataStatusEnum.INT, attributeIntValue);
		dataEntitys.add(dataEntity);
		/*//table_name
		attributeValue = squid.getName();
		if(attributeValue!=null){
			attributeValue ="'"+attributeValue+"'";
			dataEntity = new DataEntity("TABLE_NAME", DataStatusEnum.STRING, attributeValue);
			dataEntitys.add(dataEntity);
		}
		*/
	}
	
	public  void getSourceSquid2(DbSquid squid, List<DataEntity> dataEntitys) {
		logger.debug(String.format("getSourceSquid-squid=%s", squid));
		DataEntity dataEntity = null;
		String attributeValue = null;
		int attributeIntValue = 0;
		//key
		attributeValue = squid.getKey();
		if(attributeValue!=null){
			attributeValue ="'"+attributeValue+"'";
			dataEntity = new DataEntity("KEY", DataStatusEnum.STRING, attributeValue);
			dataEntitys.add(dataEntity);
			
		}
		
		//SquidFlowID
		attributeIntValue = squid.getSquidflow_id();
		dataEntity = new DataEntity("SQUID_FLOW_ID", DataStatusEnum.INT, attributeIntValue==0?1:attributeIntValue);
		dataEntitys.add(dataEntity);
		//名称
		attributeValue = squid.getName();
		if(attributeValue!=null){
			attributeValue ="'"+attributeValue+"'";
			dataEntity = new DataEntity("NAME", DataStatusEnum.STRING, attributeValue);
			dataEntitys.add(dataEntity);
		}
		
		//描述
		attributeValue = squid.getDescription();
		if(attributeValue!=null){
			attributeValue ="'"+attributeValue+"'";
			dataEntity = new DataEntity("DESCRIPTION", DataStatusEnum.STRING, attributeValue);
			dataEntitys.add(dataEntity);
		}
		
		//squid类型
		attributeIntValue = squid.getSquid_type();
		dataEntity = new DataEntity("SQUID_TYPE_ID", DataStatusEnum.INT, attributeIntValue);
		dataEntitys.add(dataEntity);
		
		//location_x
		/*attributeIntValue = squid.getLocation_x();
		dataEntity = new DataEntity("LOCATION_X", DataStatusEnum.INT, attributeIntValue);
		dataEntitys.add(dataEntity);
		//location_y
		attributeIntValue = squid.getLocation_y();
		dataEntity = new DataEntity("LOCATION_Y", DataStatusEnum.INT, attributeIntValue);
		dataEntitys.add(dataEntity);
		//SQUID_HEIGHT
		attributeIntValue = squid.getSquid_height();
		dataEntity = new DataEntity("SQUID_HEIGHT", DataStatusEnum.INT, attributeIntValue);
		dataEntitys.add(dataEntity);
		//squid_width
		attributeIntValue = squid.getSquid_width();
		dataEntity = new DataEntity("SQUID_WIDTH", DataStatusEnum.INT, attributeIntValue);
		dataEntitys.add(dataEntity);*/
		/*//table_name
		attributeValue = squid.getName();
		if(attributeValue!=null){
			attributeValue ="'"+attributeValue+"'";
			dataEntity = new DataEntity("TABLE_NAME", DataStatusEnum.STRING, attributeValue);
			dataEntitys.add(dataEntity);
		}
		 */
	}
	
	/**
	 * 将sourceSquid对象转换成DataEntity对象
	 * <p>
	 * 作用描述：
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 *@param squid squid对象
	 *@param dataEntitys DataEntity对象
	 */
	public  void getDBDestinationSquid(DbSquid squid, List<DataEntity> dataEntitys) {
		logger.debug(String.format("getDBDestinationSquid-squid=%s", squid));
		DataEntity dataEntity = null;
		String attributeValue = null;
		int attributeIntValue = 0;
		//key
		attributeValue = squid.getKey();
		if(attributeValue!=null){
			attributeValue = "'"+attributeValue+"'";
			dataEntity = new DataEntity("KEY", DataStatusEnum.STRING, attributeValue);
			dataEntitys.add(dataEntity);
		}
		
		
		//SquidFlowID
		attributeIntValue = squid.getSquidflow_id();
		dataEntity = new DataEntity("SQUID_FLOW_ID", DataStatusEnum.INT, attributeIntValue);
		dataEntitys.add(dataEntity);
		//名称
		attributeValue = squid.getName();
		if(attributeValue!=null){
			attributeValue = "'"+attributeValue+"'";
			dataEntity = new DataEntity("NAME", DataStatusEnum.STRING, attributeValue);
			dataEntitys.add(dataEntity);
		}
		
		//描述
		attributeValue = squid.getDescription();
		if(attributeValue!=null){
			attributeValue = "'"+attributeValue+"'";
			dataEntity = new DataEntity("DESCRIPTION", DataStatusEnum.STRING, attributeValue);
			dataEntitys.add(dataEntity);
		}
	
		//squid类型
			attributeIntValue = squid.getSquid_type();
			dataEntity = new DataEntity("SQUID_TYPE_ID", DataStatusEnum.INT, attributeIntValue);
			dataEntitys.add(dataEntity);
		//location_x
		attributeIntValue = squid.getLocation_x();
		dataEntity = new DataEntity("LOCATION_X", DataStatusEnum.INT, attributeIntValue);
		dataEntitys.add(dataEntity);
		//location_y
		attributeIntValue = squid.getLocation_y();
		dataEntity = new DataEntity("LOCATION_Y", DataStatusEnum.INT, attributeIntValue);
		dataEntitys.add(dataEntity);
		//SQUID_HEIGHT
		attributeIntValue = squid.getSquid_height();
		dataEntity = new DataEntity("SQUID_HEIGHT", DataStatusEnum.INT, attributeIntValue);
		dataEntitys.add(dataEntity);
		//squid_width
		attributeIntValue = squid.getSquid_width();
		dataEntity = new DataEntity("SQUID_WIDTH", DataStatusEnum.INT, attributeIntValue);
		dataEntitys.add(dataEntity);
		/*//table_name
		attributeValue = squid.getName();
		dataEntity = new DataEntity("TABLE_NAME", DataStatusEnum.STRING, attributeValue);
		dataEntitys.add(dataEntity);*/
	}
	
	/**
	 *将dbSourceTable转换成DataEntity对象集合
	 * <p>
	 * 作用描述：
	 * </p>
	 * <p>
	 * 修改说明：
	 * </p>
	 * 
	 * @param dbSourceTable dbSourceTable对象
	 * @param dataEntitys DataEntity集合
	 */
	public void getDBSourceTable(DBSourceTable dbSourceTable, List<DataEntity> dataEntitys) {
		DataEntity dataEntity = null;
		// 相关命令
		// SquidID
		dataEntity = new DataEntity("SQUID_ID", DataStatusEnum.INT, dbSourceTable.getSource_squid_id());
		dataEntitys.add(dataEntity);

		// 名称
		String name = dbSourceTable.getTable_name();
		if (name != null) {
			name = "'" + name + "'";
		}
		dataEntity = new DataEntity("TABLE_NAME", DataStatusEnum.STRING, name);
		dataEntitys.add(dataEntity);

	}

}
