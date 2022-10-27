package com.eurlanda.datashire.validator;

import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.entity.MessageBubble;
import com.eurlanda.datashire.enumeration.MessageBubbleCode;
import com.eurlanda.datashire.server.utils.TokenUtil;
import com.eurlanda.datashire.sprint7.service.squidflow.subservice.MessageBubbleService;

import java.sql.SQLException;

/**
 * 消息泡验证
 * @author bo.dang
 * @date 2014年6月5日
 */
public class SquidValidationTask implements Runnable {
    private String token;
    private IRelationalDataManager adapter;
    private MessageBubble bubble;
    private Boolean status;
    private String text;
    private Boolean pushFlag = false;

    public SquidValidationTask(String token, IRelationalDataManager adapter,
            MessageBubble bubble) {
        super();
        this.token = token;
        this.adapter = adapter;
        this.bubble = bubble;
    }
    
    public SquidValidationTask(String token,
            MessageBubble bubble,
            Boolean status,
            String text) {
        super();
        this.token = token;
        this.bubble = bubble;
        this.status = status;
        this.text = text;
    }
    
/*    public SquidValidationTask(String token, IDBAdapter adapter,
            MessageBubble bubble) {
        super();
        this.token = token;
        if(null == adapter && null == this.adapter){
            this.adapter = DataAdapterFactory.newInstance().getDataManagerByToken(
                    token);
            this.adapter.openSession();
        } else {
           // this.adapter = adapter;
        }
        this.bubble = bubble;
    }*/

    @Override
    public void run() {

        int squidId = 0;
        int childId = 0;
        String name = null;
        int code = 0;
//        List<MessageBubble> messageBubbleList = new ArrayList<MessageBubble>();
        squidId = bubble.getSquid_id();
        childId = bubble.getChild_id();
        name = bubble.getName();
        code = bubble.getBubble_code();
        String call = null;
        int level = 0;
        //String text = null;
        if(squidId != 0){
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            pushFlag = false;
        }
        else {
        	pushFlag = true;
        }
        
        try {
        //new MessageBubbleService(token, adapter).prepareCall(squidId, childId, name, code);
        switch (MessageBubbleCode.parse(code)) {
/*        case ERROR_SQUID_NO_NAME:
            // 1.Name是否为空 || 9.1 Name：string，可改，不能为空，否则出错误消息泡。在一个squidFlow内，不可重复。
            //new MessageBubbleService(token, adapter).validateSquidName(squidId, childId, name, code);
            call = "{ ? = call validateSquidName(?, ?, ?, ?) }";
            level = MessageBubbleCode.ERROR;
            text = "Name不能为空";
//            messageBubbleList.add(new MessageBubbleService(token, adapter).isolateSquid(squidId, childId, name, code));
            //new MessageBubbleService(token, adapter).validateIsolateSquid(squidId, childId, name, code);
            new MessageBubbleService(token, adapter).prepareCall(squidId, childId, name, code, call, level, text);
            break;*/
/*        case ERROR_SQUID_FLOW_DUPLICATE_NAME:
            call = "{ ? = call validateDuplicateSquidName(?, ?, ?, ?) }";
            level = MessageBubbleCode.ERROR;
            text = "在同一个SquidFlow中，Name不能重复";
//            messageBubbleList.add(new MessageBubbleService(token, adapter).isolateSquid(squidId, childId, name, code));
            //new MessageBubbleService(token, adapter).validateIsolateSquid(squidId, childId, name, code);
            new MessageBubbleService(token, adapter).prepareCall(squidId, childId, name, code, call, level, text);
            break;*/
        case ERROR_SQL_SYNTAX_FILTER:
            // 2.的 filter 过滤条件
            new MessageBubbleService(TokenUtil.getToken(), adapter).validateSquidFilter(squidId, childId, name, code);
            break;
        case ERROR_SQL_SYNTAX_JOIN:
            // 2.校验Join
            new MessageBubbleService(TokenUtil.getToken(), adapter).validateJoin(squidId, childId, name, code);
            break;
        case ERROR_SQL_SYNTAX_INCREMENT:
            // 2.32.33incremental expression 校验增量抽取表达式
            new MessageBubbleService(TokenUtil.getToken(), adapter).validateIncrement(squidId, childId, name, code);
            break;
        case WARN_SQUID_NO_LINK:
            // 3.校验孤立的Squid
            call = "{ ? = call validateIsolateSquid(?, ?, ?, ?) }";
            level = MessageBubbleCode.WARN;
            text = "已定义，但未参与有效的工作流";
//            messageBubbleList.add(new MessageBubbleService(token, adapter).isolateSquid(squidId, childId, name, code));
            //new MessageBubbleService(token, adapter).validateIsolateSquid(squidId, childId, name, code);
            new MessageBubbleService(TokenUtil.getToken(), adapter).prepareCall(squidId, childId, name, code, call, level, text);
            break;
        // index为1的消息泡校验逻辑
        case WARN_TRANSFORMATION_NO_LINK:
            // 4.校验transformation是否孤立
            call = "{ ? = call validateIsolateTransformation(?, ?, ?, ?) }";
            level = MessageBubbleCode.WARN;
            text = "已定义，但未参与有效的数据变换";
            new MessageBubbleService(TokenUtil.getToken(), adapter).validateIsolateTransformation(squidId, childId, name, code, call, level, text);
//            new MessageBubbleService(token, adapter).prepareCall(squidId, childId, name, code, call, level, text);
            break;
        case WARN_COLUMN_NO_TRANSFORMATION:
            call = "{ ? = call validateIsolateColumn(?, ?, ?, ?) }";
            level = MessageBubbleCode.WARN;
            text = "没定义变换逻辑，运行时将导入空值(NULL)";
            // 5.StageSquid中没定义transformation的column
//            new MessageBubbleService(token, adapter).validateIsolateColumn(squidId, childId, name, code);
            new MessageBubbleService(TokenUtil.getToken(), adapter).prepareCall(squidId, childId, name, code, call, level, text);
            break;
        case ERROR_NO_DB_TABLE_NAME:
            // 6.dataSquid落地的表名
//            new MessageBubbleService(token, adapter).validateDestination(squidId, childId, name, code);
            call = "{ ? = call validateDestinationTableName(?, ?, ?, ?) }";
            level = MessageBubbleCode.ERROR;
            text = "已设置为落地，但没指定落地表名";
            // 5.StageSquid中没定义transformation的column
//            new MessageBubbleService(token, adapter).validateIsolateColumn(squidId, childId, name, code);
            new MessageBubbleService(TokenUtil.getToken(), adapter).prepareCall(squidId, childId, name, code, call, level, text);
            break;
        case WARN_DATASQUID_NO_SQUIDINDEXES:
            // 8.29.dataSquid索引
//            new MessageBubbleService(token, adapter).validateDestinationTableName(squidId, childId, name, code);
            call = "{ ? = call validateSquidIndexes(?, ?, ?, ?) }";
            level = MessageBubbleCode.WARN;
            text = "索引设置不合法";
            // 5.StageSquid中没定义transformation的column
//            new MessageBubbleService(token, adapter).validateIsolateColumn(squidId, childId, name, code);
            new MessageBubbleService(TokenUtil.getToken(), adapter).prepareCall(squidId, childId, name, code, call, level, text);
            break;
        case ERROR_SQUID_CONNECTION_NO_HOST:
            // 10.校验HostAddress
            call = "{ ? = call validateHostAddress(?, ?, ?, ?) }";
            level = MessageBubbleCode.ERROR;
            text = "连接主机未设置";
//            new MessageBubbleService(token, adapter).validateHostAddress(squidId, childId, name, code);
            new MessageBubbleService(TokenUtil.getToken(), adapter).prepareCall(squidId, childId, name, code, call, level, text);
            break;
        case ERROR_SQUID_CONNECTION_NO_RDBMSTYPE:
            // 11.校验RDBMSType
            call = "{ ? = call validateRDBMSType(?, ?, ?, ?) }";
            level = MessageBubbleCode.ERROR;
            text = "RDBMSType不能为空";
//            new MessageBubbleService(token, adapter).validateRDBMSType(squidId, childId, name, code);
            new MessageBubbleService(TokenUtil.getToken(), adapter).prepareCall(squidId, childId, name, code, call, level, text);
            break;
        case ERROR_SQUID_CONNECTION_NO_DATABASE_NAME:
            // 12.校验DataBaseName
            call = "{ ? = call validateDataBaseName(?, ?, ?, ?) }";
            level = MessageBubbleCode.ERROR;
            text = "数据库名字未设置";
//            new MessageBubbleService(token, adapter).validateDataBaseName(squidId, childId, name, code);
            new MessageBubbleService(TokenUtil.getToken(), adapter).prepareCall(squidId, childId, name, code, call, level, text);
            break;
        case ERROR_SQUID_CONNECTION_NO_FILE_PATH:
            // 15.18.21.校验FilePath
            call = "{ ? = call validateFilePath(?, ?, ?, ?) }";
            level = MessageBubbleCode.ERROR;
            text = "文件路径未设置";
//            new MessageBubbleService(token, adapter).validateFilePath(squidId, childId, name, code);
            new MessageBubbleService(TokenUtil.getToken(), adapter).prepareCall(squidId, childId, name, code, call, level, text);
            break;
        case ERROR_WEIBO_CONNECTION_NO_SERVICE_PROVIDER:
            // 23.校验ServiceProvider微博提供商设置
            call = "{ ? = call validateServiceProvider(?, ?, ?, ?) }";
            level = MessageBubbleCode.ERROR;
            text = "微博提供商未设置";
//            new MessageBubbleService(token, adapter).validateServiceProvider(squidId, childId, name, code);
            new MessageBubbleService(TokenUtil.getToken(), adapter).prepareCall(squidId, childId, name, code, call, level, text);
            break;
        case WARN_WEB_CONNECTION_NO_URL_LIST:
            // 25.校验WebConnection的URL未设置
            call = "{ ? = call validateURLList(?, ?, ?, ?) }";
            level = MessageBubbleCode.WARN;
            text = "URL未设置";
//            new MessageBubbleService(token, adapter).validateURLList(squidId, childId, name, code);
            new MessageBubbleService(TokenUtil.getToken(), adapter).prepareCall(squidId, childId, name, code, call, level, text);
            break;
        case WARN_DATA_SQUID_NO_EXCEPTION_SQUID:
            // 31.异常squid已定义
            //new MessageBubbleService(token, adapter).validateExceptionSquid(squidId, childId, name, code);
            break;
        case ERROR_DOC_EXTRACT_SQUID_NO_DELIMITER:
            // 34.文档抽取Squid(DocExtractSquid)验证Delimiter
            call = "{ ? = call validateDelimiter(?, ?, ?, ?) }";
            level = MessageBubbleCode.ERROR;
            text = "分隔符未设置";
//            new MessageBubbleService(token, adapter).validateDelimiter(squidId, childId, name, code);
            new MessageBubbleService(TokenUtil.getToken(), adapter).prepareCall(squidId, childId, name, code, call, level, text);
            break;
        case ERROR_DOC_EXTRACT_SQUID_NO_FIELD_LENGTH:
            // 35.FieldLength字段长度设置非法
            call = "{ ? = call validateFieldLength(?, ?, ?, ?) }";
            level = MessageBubbleCode.ERROR;
            text = "字段长度设置非法";
//            new MessageBubbleService(token, adapter).validateFieldLength(squidId, childId, name, code);
            new MessageBubbleService(TokenUtil.getToken(), adapter).prepareCall(squidId, childId, name, code, call, level, text);
            break;
        case ERROR_WEBLOG_EXTRACT_SQUID_NO_SOURCE_DATA:
            // 36.校验WebLogExtractSquid是否完成获取元数据
            call = "{ ? = call validateWebLogExtractSquidSourceData(?, ?, ?, ?) }";
            level = MessageBubbleCode.ERROR;
            text = "尚未完成元数据设置";
//            new MessageBubbleService(token, adapter).validateWebLogExtractSquidSourceData(squidId, childId, name, code);
            new MessageBubbleService(TokenUtil.getToken(), adapter).prepareCall(squidId, childId, name, code, call, level, text);
            break;
        case ERROR_SQUID_LINK_LOOP:
            // 37.校验显式SquidLink直接构成闭环和复杂闭环
            //new MessageBubbleService(token, adapter).validateSquidLinkLoop(squidId, childId, name, code);
            break;
        case ERROR_TRAN_LINK_LOOP:
            // 39.40.TranLink显式和阴式闭环校验
            //new MessageBubbleService(token, adapter).validateTranLinkLoop(squidId, childId, name, code);
            break;
        case ERROR_TRAN_PARAMETERS:
            // 41.Transform的实参个数和其定义的形参个数
            new MessageBubbleService(TokenUtil.getToken(), adapter).validateTranParameters(squidId, childId, name, code);
            break;
        case ERROR_TERMEXTRACT_REG_EXPRESSION_SYNTAX:
            // 42.校验TermExtract表达式设置语法错误
            new MessageBubbleService(TokenUtil.getToken(), adapter).validateTermExtract(squidId, childId, name, code);
            break;
        case WARN_REPORT_SQUID_NO_PUBLISHING_FOLDER:
            // 43.校验发布路径未设置
            call = "{ ? = call validatePublishingFolder(?, ?, ?, ?) }";
            level = MessageBubbleCode.WARN;
            text = "发布路径未设置";
//            new MessageBubbleService(TokenUtil.getToken(), adapter).validatePublishingFolder(squidId, childId, name, code);
            new MessageBubbleService(TokenUtil.getToken(), adapter).prepareCall(squidId, childId, name, code, call, level, text);
            break;
        case ERROR_REPORT_SQUID_NO_TEMPLATE_DEFINITION:
            // 44.校验报表模板定义
            call = "{ ? = call validateTemplateDefinition(?, ?, ?, ?) }";
            level = MessageBubbleCode.ERROR;
            text = "报表模板设置非法";
//            new MessageBubbleService(TokenUtil.getToken(), adapter).validateTemplateDefinition(squidId, childId, name, code);
            new MessageBubbleService(TokenUtil.getToken(), adapter).prepareCall(squidId, childId, name, code, call, level, text);
            break;
        case WARN_DATA_MININGSQUID_TRAINING_PERCENTAGE:
            // 47.校验Training_percentage训练集所占比例
            call = "{ ? = call validateTrainingPercentage(?, ?, ?, ?) }";
            level = MessageBubbleCode.WARN;
            text = "训练数据设置过小，可能会影响模型";
//            new MessageBubbleService(TokenUtil.getToken(), adapter).validateTrainingPercentage(squidId, childId, name, code);
            new MessageBubbleService(TokenUtil.getToken(), adapter).prepareCall(squidId, childId, name, code, call, level, text);
            break;
        case ERROR_PREDICT_MODEL_SQUID_BY_DELETED:
            // 48.校验ModelSquidId
            new MessageBubbleService(TokenUtil.getToken(), adapter).validatePredictModelSquid(squidId, childId, name, code);
            break;
        case WARN_REPORT_SQUID_COLUMNS_BY_DELETED:
            // 49.校验报表squid的源数据是否被删除或者修改
            new MessageBubbleService(TokenUtil.getToken(), adapter).validateReportSquidColumnsByDEL(squidId, childId, name, code);
            break;
        case ERROR_COLUMN_DATA_TYPE:
            // 50.校验Column的映射类型
            call = "{ ? = call validateColumnDataType(?, ?, ?, ?) }";
            level = MessageBubbleCode.ERROR;
            text = "类型为UNKNOWN, 必须为其指定一个具体的映射类型";
            new MessageBubbleService(TokenUtil.getToken(), adapter).prepareCall(squidId, childId, name, code, call, level, text);
            break;
            // fixed bug 847 start by bo.dang
        case ERROR_AGGREGATION_OR_GROUP:
            // 51.校验Aggregation或者Group设置
            call = "{ ? = call validateAggregationOrGroup(?, ?, ?, ?) }";
            level = MessageBubbleCode.ERROR;
            text = "对应的Aggregation或者Group设置错误";
            new MessageBubbleService(TokenUtil.getToken(), adapter).validateAggregationOrGroup(squidId, childId, name, code);
            //new MessageBubbleService(token, adapter).prepareCall(squidId, childId, name, code, call, level, text);
            break;
        case ERROR_SYNTAX_TABLE_NAME:
        case ERROR_NOT_PERSISTENT_TABLE:
        case ERROR_INDEXES_NOT_SYNCHRONIZED:
            // fixed bug 847 end by bo.dang
        case ERROR_SYNCHRONIZED:
            // 52.同步检查出现消息泡
            level = MessageBubbleCode.ERROR;
            new MessageBubbleService(TokenUtil.getToken()).checkSynchronized(squidId, childId, name, code, status, text);
            break;
        default:
            break;
        }
        // closeSession
        if (pushFlag) {
			closeSessionForMessageBubble();
		}
        
        } catch (Exception e) {
			e.printStackTrace();
		}
        // 根据前面校验的结果，把消息泡推给客户端
        //PushMessagePacket.push(messageBubbleList, token);
    }

    /**
     * 关闭adapter
     * @author bo.dang
     */
    public void closeSessionForMessageBubble(){
 	   if(null != this.adapter.getConnection()){
           try {
			if(!this.adapter.getConnection().isClosed()){
			       this.adapter.closeSession();
			   }
		} catch (SQLException e) {
			
			// TODO Auto-generated catch block e.printStackTrace();
			
		}
 	   }
    }
    
    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
    }

    

    public IRelationalDataManager getAdapter() {
        return adapter;
    }

    public void setAdapter(IRelationalDataManager adapter) {
        this.adapter = adapter;
    }

    public MessageBubble getBubble() {
        return bubble;
    }

    public void setBubble(MessageBubble bubble) {
        this.bubble = bubble;
    }

}
