package com.eurlanda.datashire.enumeration;

/**
 * 消息气泡 - 错误码
 * 
 *  1. Squid的过滤条件、增量抽取条件，和squid join的join_condition条件
 *  
	2. 孤立squid: 新创建（除extract squid从tablelist拖拽出来）；或没有/删除link
	3. 孤立transformation (no link)
	4. Connection连接信息是否完整、正确
	5. DataSquid.tableName（如果勾选落地，或者没勾选但是有destination squid link,表名不符合一般关系型数据库表名规范的，是错误消息级别）
	6. 发现上游的column被删除  错误消息级别
	7. 发现目标列（左边列）存在没有参与变换的 错误消息级别
	8. 同一个destination squid的上游data squid的table name不能重复
	9. 一个data squid的column 的name不能重复（否则不能落地）
	10. 发现source table 或 column 被删除 (前台点击connect，刷新db_squid的source_table_list缓存)
	
 * @author dang.lu 2013.11.22
 */
public enum MessageBubbleCode {
    
	
	WARN_SQUID_NO_LINK(90001), // 孤立squid: 新创建，删除link，加载时发现没有link(或者删除squid，导致其他关联squid孤立)
	WARN_TRANSFORMATION_NO_LINK(90002), // 孤立transformation: 新创建，删除link，加载时发现没有link
	WARN_NO_SQL_EXPRESSION(90003),// SQL表达式为空
	WARN_COLUMN_NO_TRANSFORMATION(90004),// StageSquid中没定义transformation的column
	WARN_DATASQUID_NO_SQUIDINDEXES(90005),// dataSquid索引,索引设置不合法
	WARN_WEB_CONNECTION_NO_URL_LIST(90006), // WebConnection的URL未设置
	WARN_DATA_SQUID_NO_BUSINESS_KEY(90007), // 追踪历史开关已打开，但没设置Business Key
	WARN_DATA_SQUID_NO_EXCEPTION_SQUID(90008), // dataSquid的异常squid未定义
	WARN_REPORT_SQUID_NO_PUBLISHING_FOLDER(90009), // ReportSquid发布路径未设置
	WARN_DATA_MININGSQUID_TRAINING_PERCENTAGE(90010), // 训练数据设置过小，可能会影响模型
	WARN_PREDICT_MODEL_SQUID_BY_UPDATED(90011), // 引用的模型有更新
	WARN_NO_JOIN_CONDITION(90012),// join连接条件

	WARN_REPORT_SQUID_COLUMNS_BY_DELETED(90088),//报表squid的源数据被修改，可能导致报表运行不正常
	
	ERROR_DB_CONNECTION(91001), // Connection连接信息不完整或错误
	ERROR_DB_TABLE_NAME(91002), // 表名不合法(DataSquid.tableName 如果勾选落地，或者没勾选但是有destination squid link)
	ERROR_REFERENCE_COLUMN_DELETED(91003),// 上游的column被删除
	ERROR_COLUMN_NO_TRANSFORMATION(91004), // 目标列（左边列）没有参与变换(1.squidflow加载，2.translink删除，3.创建squidlink)
	
	// 表达式校验失败：SQL语法错误
	ERROR_SQL_SYNTAX_FILTER(91005),  // 过滤条件不合法
	ERROR_SQL_SYNTAX_INCREMENT(91006),  // 增量抽取条件不合法
	ERROR_SQL_SYNTAX_JOIN(91007),  // 连接条件不合法
	 
	ERROR_SQL_GROUPBY(91008), // GROUPBY错误 ：要查询的列（除聚合函数外）不在GROUP BY子句中
	
	ERROR_SQUID_NAME(91009), // Squid的Name是否为空
	ERROR_NO_DB_TABLE_NAME(91010), // stageSquid的persist属性为true，但table_name为空,已设置为落地，但没指定落地表名
	ERROR_SQUID_NO_NAME(91011), // 各种squid的名字为空
	ERROR_SQUID_CONNECTION_NO_HOST(91012), // 各种Squid Connection连接主机未设置
	ERROR_SQUID_CONNECTION_NO_RDBMSTYPE(91013), // RDBMSType未设置
	ERROR_SQUID_CONNECTION_NO_DATABASE_NAME(91014), // 数据库未设置
	ERROR_SQUID_CONNECTION_NO_FILE_PATH(91015), // 文件路径未设置
	ERROR_WEIBO_CONNECTION_NO_SERVICE_PROVIDER(91016), // WeiboConnection的ServiceProvider微博提供商未设置
	ERROR_SQUID_FLOW_DUPLICATE_NAME(91017), // 在一个SquidFlow内，Squid的Name不可重复
	ERROR_DATA_SQUID_NO_INCREMENTAL_EXPRESSION(91018), // 增量抽取条件设置为空
	ERROR_DOC_EXTRACT_SQUID_NO_DELIMITER(91019), // 分隔符未设置
	ERROR_DOC_EXTRACT_SQUID_NO_FIELD_LENGTH(91020), // 字段长度设置非法
	ERROR_WEBLOG_EXTRACT_SQUID_NO_SOURCE_DATA(91021), // 尚未完成元数据设置
	ERROR_SQUID_LINK_LOOP(91022), // SquidLink闭环，交叉引用
	ERROR_TRAN_LINK_LOOP(91023), // TranLink闭环，交叉引用
	ERROR_TRAN_PARAMETERS(91024), // Transform的实参个数和其定义的形参个数
	ERROR_TERMEXTRACT_REG_EXPRESSION_SYNTAX(91025), // TermExtract表达式设置语法错误
	ERROR_REPORT_SQUID_NO_TEMPLATE_DEFINITION(91026), // 报表模板设置非法
	ERROR_PREDICT_MODEL_SQUID_BY_DELETED(91027), // 引用的模型有更新
	ERROR_COLUMN_DATA_TYPE(91028),// 校验squid column中出现类型为’UNKNOWN’类型的,弹错误消息泡”XXX类型为UNKNOWN, 必须为其指定一个具体的映射类型
	ERROR_AGGREGATION_OR_GROUP(91029),// Aggregation和Group设置错误
	ERROR_SYNCHRONIZED(91030),// 同步失败
	ERROR_SYNTAX_TABLE_NAME(91031),// 落地表名不合法
	ERROR_NOT_PERSISTENT_TABLE(91032), // 未创建落地对象
	ERROR_INDEXES_NOT_SYNCHRONIZED(91033),// 索引未同步
	
	WARN_GISMAP_SQUID_NO_PUBLISHING_FOLDER(92001), // ReportSquid发布路径未设置
	ERROR_GISMAP_SQUID_NO_TEMPLATE_DEFINITION(92002); // 报表模板设置非法
	
	private int value;
	public static final int WARN=0;
	public static final int ERROR=1;
	
	private MessageBubbleCode(int v) {
		value = v;
	}
	
	public int value() {
		return value;
	}
	
	public static int level(int t){
		return t >= ERROR_DB_CONNECTION.value ? ERROR : WARN;
	}
	
    public static MessageBubbleCode parse(int t) {
        for (MessageBubbleCode result : values()) {
            if (result.value() == t) {
                return result;
            }
        }
        return null;
    }
    
    public static MessageBubbleCode parse(String name) {
    	return (MessageBubbleCode)Enum.valueOf(MessageBubbleCode.class, name);
    }
}
