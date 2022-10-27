package com.eurlanda.datashire.socket;

import com.eurlanda.datashire.enumeration.DMLType;
import com.eurlanda.datashire.enumeration.DSObjectType;
import com.eurlanda.datashire.socket.protocol.AuthenticationProtocol;
import com.eurlanda.datashire.socket.protocol.PrivilegeConf;
import com.eurlanda.datashire.socket.protocol.ServiceConf;
import com.eurlanda.datashire.socket.protocol.SimpleMessageProtocol;

import java.util.HashMap;
import java.util.Map;
/**
 * 
 * <p>
 * Title : 功能定义类
 * </p>
 * <p>
 * Description: 用于定义服务调用的类、方法
 * </p>
 * <p>
 * Author :何科敏 Sep 5, 2013
 * </p>
 * <p>
 * update :何科敏 Sep 5, 2013
 * </p>
 * <p>
 * Department :  JAVA后端研发部
 * </p>
 * Copyright : ©2012-2013 悦岚（上海）数据服务有限公司
 * </p>
 */
public class Agreement {
    
    /**
     * 定义类关系 命令号>类名称
     */
    public static Map<String,String> AGREEMENT_CLASS;
    
    
    /**
     * 定义方法关系 命令号>方法串 <cmdId, ServiceConf>
     */
    public static final Map<String, ServiceConf> ServiceConfMap=new HashMap<String, ServiceConf>();
    
    /**
     * 定义处理协议
     */
//    public static String AGREEMENT_AUTHEN = "com.eurlanda.datashire.socket.protocol.AuthenticationProtocol";
    public static String AGREEMENT_AUTHEN = AuthenticationProtocol.class.getCanonicalName();

    /**
     * 定义处理协议对象(其他提供外部调用业务类都采用此协议)
     */
//    public static String AGREEMENT_SIMPLE = "com.eurlanda.datashire.socket.protocol.SimpleMessageProtocol";
    public static String AGREEMENT_SIMPLE = SimpleMessageProtocol.class.getCanonicalName();

    /**
     * 定义协议关系 命令号>协议串
     */
    public static Map<String, String> AGREEMENT_PROTOCOL;
    
    static {
        boolean SEND_RESPONSE_YES = true;
        boolean SEND_RESPONSE_NO = false;
        
        AGREEMENT_PROTOCOL = new HashMap<String, String>();
        AGREEMENT_PROTOCOL.put("0000", AGREEMENT_AUTHEN); // 登录、登出
        AGREEMENT_PROTOCOL.put("0001", AGREEMENT_SIMPLE);
        AGREEMENT_PROTOCOL.put("0002", AGREEMENT_SIMPLE);
        AGREEMENT_PROTOCOL.put("0005", AGREEMENT_SIMPLE); // 权限相关
        AGREEMENT_PROTOCOL.put("0006", AGREEMENT_SIMPLE);//repository校验
        AGREEMENT_PROTOCOL.put("0007", AGREEMENT_SIMPLE);//取消
        AGREEMENT_PROTOCOL.put("0008", AGREEMENT_SIMPLE);//游览导入操作
        AGREEMENT_PROTOCOL.put("0009", AGREEMENT_SIMPLE);//树实现
        // ReportFolder added by bo.dang
        AGREEMENT_PROTOCOL.put("1002", AGREEMENT_SIMPLE);
        // ReportSquid added by bo.dang
        AGREEMENT_PROTOCOL.put("1003", AGREEMENT_SIMPLE);
        AGREEMENT_PROTOCOL.put("1001", AGREEMENT_SIMPLE);
        //新的命令约定
        AGREEMENT_PROTOCOL.put("1000", AGREEMENT_SIMPLE);//新接口
          //新的命令约定
        AGREEMENT_PROTOCOL.put("1004", AGREEMENT_SIMPLE);//新接口
        //新的命令约定
        AGREEMENT_PROTOCOL.put("1005", AGREEMENT_SIMPLE);//新接口
        //新的命令约定
        AGREEMENT_PROTOCOL.put("1006", AGREEMENT_SIMPLE);//新接口
        //新的命令约定
        AGREEMENT_PROTOCOL.put("1007", AGREEMENT_SIMPLE);//新接口
        //新的命令约定
        AGREEMENT_PROTOCOL.put("1008", AGREEMENT_SIMPLE);//新接口
        //新的命令约定
        AGREEMENT_PROTOCOL.put("1010", AGREEMENT_SIMPLE);//新接口
        //新的命令约定 yi.zhou
        AGREEMENT_PROTOCOL.put("1011", AGREEMENT_SIMPLE);//新接口
        //任务调度和启动运行日志
        AGREEMENT_PROTOCOL.put("1012", AGREEMENT_SIMPLE);//新接口
        //停止运行日志
        AGREEMENT_PROTOCOL.put("1013", AGREEMENT_SIMPLE);//新接口
        //新的命令约定 yi.zhou
        AGREEMENT_PROTOCOL.put("1015", AGREEMENT_SIMPLE);//新接口
        //新的命令约定 yi.zhou
        AGREEMENT_PROTOCOL.put("1016", AGREEMENT_SIMPLE);//新接口
        //加解锁
        AGREEMENT_PROTOCOL.put("1017", AGREEMENT_SIMPLE);//新接口
        //注册用户新接口(过渡方式)
        AGREEMENT_PROTOCOL.put("1018", AGREEMENT_SIMPLE);//新接口
        //httpExtractsquid http源
        AGREEMENT_PROTOCOL.put("1019", AGREEMENT_SIMPLE);//新接口
        //webServiceExtractsquit webService源
        AGREEMENT_PROTOCOL.put("1020", AGREEMENT_SIMPLE);//新接口
        //variable added by yi.zhou 2014-12-10
        AGREEMENT_PROTOCOL.put("1021", AGREEMENT_SIMPLE);//新接口
        //import、exprot added by yi.zhou 2015-01-19
        AGREEMENT_PROTOCOL.put("1022", AGREEMENT_SIMPLE);//新接口
        AGREEMENT_PROTOCOL.put("1023", AGREEMENT_SIMPLE);//新接口
        //处理类名称注入
        AGREEMENT_CLASS=new HashMap<String,String>();
        // 一、 登录、登出
        AGREEMENT_CLASS.put("0000", "loginService");
      
        //DataShirService类名称注入
        AGREEMENT_CLASS.put("0001", "dataShirService");
         //DataShirService类名称注入
        AGREEMENT_CLASS.put("0002", "dataShirService");
       
        // 二、 权限信息（增删查改）相关
        AGREEMENT_CLASS.put("0005", "permissionService");
        
        //RepositoryValidationService类名称注入
        AGREEMENT_CLASS.put("0006", "repositoryValidationService");
        
        //ValidationCancleService方法注入
        AGREEMENT_CLASS.put("0007", "validationCancleService");
        
        AGREEMENT_CLASS.put("0008", "metadataImportAction");
        
        AGREEMENT_CLASS.put("0009", "metadataTreeActionImpl");
        // ReportFolder added by bo.dang
        AGREEMENT_CLASS.put("1002", "reportFolderService");
        // ReportSquid added by bo.dang
        AGREEMENT_CLASS.put("1003", "reportSquidService");
        
        AGREEMENT_CLASS.put("1000", "repositoryService");
        AGREEMENT_CLASS.put("1001", "dataShirService");
        // DocExtractSquid added by bo.dang
        AGREEMENT_CLASS.put("1004", "docExtractSquidService");
        // XmlExtractSquid added by bo.dang
        AGREEMENT_CLASS.put("1005", "xmlExtractSquidService");
        // WebLogExtractSquid added by bo.dang
        AGREEMENT_CLASS.put("1006", "webLogExtractSquidService");
        // WebLogExtractSquid added by bo.dang
        AGREEMENT_CLASS.put("1007", "weiBoExtractSquidService");
        // WebExtractSquid added by bo.dang
        AGREEMENT_CLASS.put("1008", "webExtractSquidService");
        // DataMiningSquid added by bo.dang
        AGREEMENT_CLASS.put("1010", "dataMiningSquidService");
        // DataMiningSquid added by yi.zhou
        AGREEMENT_CLASS.put("1011", "managerSquidFlowService");
        //任务调度和运行日志
        AGREEMENT_CLASS.put("1012", "jobScheduleService");
        //停止运行日志
        AGREEMENT_CLASS.put("1013", "jobLogService");
        // SquidIndexsService added by bo.dang
        AGREEMENT_CLASS.put("1015", "squidIndexsService");
        AGREEMENT_CLASS.put("1017", "lockSquidFlowService");
        //登陆以及用户操作
        AGREEMENT_CLASS.put("1018", "userNewService");
        //httpExtractsquid
        AGREEMENT_CLASS.put("1019", "httpExtractSquidService");
        //webserviceExtractsquid
        AGREEMENT_CLASS.put("1020", "webserviceExtractSquidService");
        //variable add yi.zhou
        AGREEMENT_CLASS.put("1021", "variableService");
        
        //SquidFlow import\export add yi.zhou
        AGREEMENT_CLASS.put("1022", "repositoryExportService");
        AGREEMENT_CLASS.put("1023", "repositoryImportService");

        // es 落地squid
        AGREEMENT_CLASS.put("1024", "destEsSquidService");
        AGREEMENT_CLASS.put("1025", "esColumnService");
        //Kafka SquidModelBase
        AGREEMENT_CLASS.put("1026", "kafkaConnectionSquidService");
        AGREEMENT_CLASS.put("1027", "kafkaExtractSquidService");
        // 服务端系统功能类
        AGREEMENT_CLASS.put("1028", "eurlandaSystemService");
        // Hbase SquidModelBase
        AGREEMENT_CLASS.put("1029", "hbaseSquidService");
        AGREEMENT_CLASS.put("1030", "hbaseExtractSquidService");
        AGREEMENT_CLASS.put("1031", "streamStageSquidService");
        //管理落地  by dzp
        AGREEMENT_CLASS.put("1032", "persistManagerService");
        //HDFS 落地squid  by dzp
        AGREEMENT_CLASS.put("1033", "destHDFSSquidService");
        AGREEMENT_CLASS.put("1034", "hdfsColumnService");
        //impala落地quid  by dzp
        AGREEMENT_CLASS.put("1035", "destImpalaSquidService");
        AGREEMENT_CLASS.put("1036", "impalaColumnService");
        //编译检查
        AGREEMENT_CLASS.put("1037", "compileValidateService");
        //版本控制(生成历史版本)
        AGREEMENT_CLASS.put("1038","squidVersionService");
        //新的接口(修改原来的有问题的接口)
        AGREEMENT_CLASS.put("1039","newSquidFlowService");
        //数列云注册新接口
        AGREEMENT_CLASS.put("1040","cloudRegisterService");
        //新squid GroupTagging
        AGREEMENT_CLASS.put("1041","groupTaggingSquidService");
        //新squid SystemHive
        AGREEMENT_CLASS.put("1051","systemHiveService");
        //新Squid Cassandra
        AGREEMENT_CLASS.put("1061","cassandraService");
        //用户自定义squid
        AGREEMENT_CLASS.put("1071","userDefinedService");
        //hive落地
        AGREEMENT_CLASS.put("1072","destHiveSquidService");
        //统计算法
        AGREEMENT_CLASS.put("1073","statisticsService");
        //Cassandra 落地
        AGREEMENT_CLASS.put("1074","destCassandraService");
        /////////////////////// [通讯接口参数统一配置] //////////////////

        /*
         * 说明：
         *  1. ServiceConfMap将来可能取代AGREEMENT_FIELD
         *  2. ServiceConf PrivilegeConf Required 名称以及放在哪个package可以重新定义, set get方法请自行补充
         */
        /******************************************team的操作************************************/
        /*
         * 说明：更新team
         * 入参:List<Team>
         * 返回: ListMessagePacket<Team>
         */
        ServiceConfMap.put("00050003", new ServiceConf("updateTeam", String.class, 
                new PrivilegeConf(DSObjectType.TEAM, DMLType.UPDATE)));
        
        /*
         * 说明：添加team
         * 入参: List<Team>
         * 返回: ListMessagePacket<Team>
         */
        ServiceConfMap.put("00050002", new ServiceConf("addTeam",String.class, 
                new PrivilegeConf(DSObjectType.TEAM, DMLType.INSERT)));
        
        /******************************************group的操作************************************/
        
        /*
         * 说明：添加Group
         * 入参: List<Group>
         * 返回: ListMessagePacket<InfoPacket>
         */
        ServiceConfMap.put("00050004", new ServiceConf("addGroup",String.class, 
                new PrivilegeConf(DSObjectType.GROUP, DMLType.INSERT)));
        /*
         * 说明：更新Group
         * 入参: List<Group>
         * 返回: ListMessagePacket<InfoPacket>
         */
        ServiceConfMap.put("00050005", new ServiceConf("updateGroup",String.class, 
                new PrivilegeConf(DSObjectType.GROUP, DMLType.UPDATE)));
        
        /******************************************role的操作************************************/
        /*
         * 说明：添加Role
         * 入参: List<Role>
         * 返回: ListMessagePacket<InfoPacket>
         */
    
        ServiceConfMap.put("00050006", new ServiceConf("addRole",String.class, 
                new PrivilegeConf(DSObjectType.ROLE, DMLType.INSERT)));
        /*
         * 说明：更新Role
         * 入参: List<Role>
         * 返回: ListMessagePacket<InfoPacket>
         */
        ServiceConfMap.put("00050007", new ServiceConf("updateRole",String.class, 
                new PrivilegeConf(DSObjectType.ROLE, DMLType.UPDATE)));
        
        /******************************************user的操作************************************/
        /*
         * 说明：添加User
         * 入参: List<User>
         * 返回: ListMessagePacket<InfoPacket>
         */
        
        ServiceConfMap.put("00050008", new ServiceConf("addUser",String.class, 
                new PrivilegeConf(DSObjectType.USER, DMLType.INSERT)));
        /*
         * 说明：更新User
         * 入参: List<User>
         * 返回: ListMessagePacket<InfoPacket>
         */
        ServiceConfMap.put("00050009", new ServiceConf("updateUser",String.class, 
                new PrivilegeConf(DSObjectType.USER, DMLType.UPDATE)));
        /*
         * 说明：获取所有user
         * 入参: 无
         * 返回: ListMessagePacket<User>
         */
        
        ServiceConfMap.put("00050015", new ServiceConf("getAllUsers",Void.TYPE, 
                new PrivilegeConf(DSObjectType.USER, DMLType.SELECT)));
        /*
         * 说明：根据teamid获取user
         * 入参: String (只需要一个属性：team.team_id)
         * 返回: ListMessagePacket<User>
         */
        ServiceConfMap.put("00050010", new ServiceConf("getUsersByTeamId", String.class, 
                new PrivilegeConf(DSObjectType.USER, DMLType.SELECT)));
        /******************************************repository的操作************************************/
        /*
         * 说明：创建Repository
         * 入参: List<Repository>
         * 返回: List<InfoPacket>
         */
        /****modify add by binlei 20140916无用的接口*****/
       /* ServiceConfMap.put("00010002", new ServiceConf("createRepositorys",String.class, 
                new PrivilegeConf(DSObjectType.REPOSITORY, DMLType.INSERT)));*/
        
         /*
             * 说明：更新Repository
             * 入参: List<Repository>
             * 返回: List<InfoPacket>
             */
        /****modify add by binlei 20140916无用的接口*****/
/*        ServiceConfMap.put("00010038", new ServiceConf("updateRepositorys",String.class, 
                    new PrivilegeConf(DSObjectType.REPOSITORY, DMLType.UPDATE)));*/
         /*
         * 说明：查询Repository
         * 入参: IdentityCard
         * 返回: List<Repository>
         */
        /****modify add by binlei 20140916无用的接口*****/
        /*ServiceConfMap.put("00010028", new ServiceConf("querylAllRepositorys",String.class, 
                new PrivilegeConf(DSObjectType.REPOSITORY, DMLType.SELECT)));*/
        
        /******************************************project的操作************************************/
         /*
         * 说明：创建Project
         * 入参: List<Project>
         * 返回: List<InfoPacket>
         */
       ServiceConfMap.put("10010003", new ServiceConf("createProjects",String.class, null));
       
         /*
         * 说明：更新Project
         * 入参: List<Project>
         * 返回: List<InfoPacket>
         */
       /****modify add by yi.zhou 20140917无用的接口*****/
       /*ServiceConfMap.put("00010015", new ServiceConf("updateProjects",String.class, 
                new PrivilegeConf(DSObjectType.PROJECT, DMLType.UPDATE)));*/

       /*
         * 说明：查询Project
         * 入参: repository
         * 返回: DataMessagePacket<T>
         */
       ServiceConfMap.put("10010027", new ServiceConf("queryAllProjects",String.class, 
                new PrivilegeConf(DSObjectType.PROJECT, DMLType.SELECT)));
       /******************************************squid的操作************************************/

        ServiceConfMap.put("10010100", new ServiceConf("createDBsourceSquid",
                String.class, new PrivilegeConf(DSObjectType.SQUID,
                        DMLType.INSERT)));
        /****modify add by binlei 20140916无用的接口*****/
        /*        ServiceConfMap.put("00010010", new ServiceConf("createExtractSquids", String.class,null, SEND_RESPONSE_NO));*/
        ServiceConfMap.put("10010039", new ServiceConf("createTableExtractSquids", String.class,null, SEND_RESPONSE_NO));
        
        ServiceConfMap.put("10010011", new ServiceConf("createStageSquids",
                String.class, new PrivilegeConf(DSObjectType.SQUID,
                        DMLType.INSERT)));
        /****modify add by binlei 20140916无用的ServiceConf接口*****/
       /* ServiceConfMap.put("00010012", new (
                "createDestinationSquids", String.class, new PrivilegeConf(
                        DSObjectType.SQUID, DMLType.INSERT)));*/
        ServiceConfMap.put("10010101", new ServiceConf("updateDBsourcesquids",
                String.class, new PrivilegeConf(DSObjectType.SQUID,
                        DMLType.UPDATE)));
        // 更新Extract的属性面板
        ServiceConfMap.put("10010022", new ServiceConf("updateExtractSquids",
                String.class, new PrivilegeConf(DSObjectType.SQUID,
                        DMLType.UPDATE)));
        ServiceConfMap.put("10010023", new ServiceConf("updateStageSquids",
                String.class, new PrivilegeConf(DSObjectType.SQUID,
                        DMLType.UPDATE)));
        /****modify add by binlei 20140916无用的接口*****/
        /*ServiceConfMap.put("00010024", new ServiceConf(
                "updateDestinationSquids", String.class, new PrivilegeConf(
                        DSObjectType.SQUID, DMLType.UPDATE)));*/
        /****modify add by binlei 20140916无用的接口*****/
        /*ServiceConfMap.put("00010045", new ServiceConf("updateSquids",
                String.class, new PrivilegeConf(DSObjectType.SQUID,
                        DMLType.UPDATE), false));*/
        // 查询squid和squidLink
        /****modify add by binlei 20140916无用的接口*****/
        /*ServiceConfMap.put("00010029", new ServiceConf(
                "querySquidAndSquidLink", String.class, new PrivilegeConf(
                        DSObjectType.SQUID, DMLType.SELECT)));*/
       /******************************************通用删除************************************/
        /*
         * 通用删除deleteRepositoryObject
         */
        ServiceConfMap.put("00010025", new ServiceConf("deleteRepositoryObject", String.class));
        ServiceConfMap.put("00010026", new ServiceConf("deleteAllSquidsInSquidflow", String.class, null, SEND_RESPONSE_YES));
        
         /******************************************不需要权限的部分************************************/
          /*
         * 说明： 用户登录
         * 入参: User (只需要三个属性：user.usrName user.pwd user.team.team_id)
         * 返回: InfoMessagePacket<User> (包含team，team中包含groupList和repositoryList；每个group中又包含roleList)
         */
        ServiceConfMap.put("00000001", new ServiceConf("login", String.class,  null));
        ServiceConfMap.put("00000002", new ServiceConf("logout", Void.TYPE,  null,SEND_RESPONSE_YES));
        /****modify add by binlei 20140916无用的接口*****/
        /*ServiceConfMap.put("00010004", new ServiceConf("createSquidLinks", String.class,  null));*/
        ServiceConfMap.put("10010005", new ServiceConf("createSquidFlows", String.class, null));
        ServiceConfMap.put("00010006", new ServiceConf("createTransformations", String.class,  null));
        ServiceConfMap.put("10010007", new ServiceConf("createTransformationLinks", String.class,  null));
        ServiceConfMap.put("00010008", new ServiceConf("createColumns", String.class,  null));
        ServiceConfMap.put("00010016", new ServiceConf("updateSquidLinks", String.class,  null, false));
        ServiceConfMap.put("00010017", new ServiceConf("updateSquidFlows", String.class,  null));
        ServiceConfMap.put("10010018", new ServiceConf("updateTransLocations", String.class,  null));
        ServiceConfMap.put("00010019", new ServiceConf("updateTransformationLinks", String.class,  null));
        ServiceConfMap.put("00010020", new ServiceConf("updateColumns", String.class,  null));
        ServiceConfMap.put("10010026", new ServiceConf("queryAllSquidFlows", String.class,  null));

        //注册
        ServiceConfMap.put("00010037", new ServiceConf("createRegisterRepositorys", String.class,  null));
        ServiceConfMap.put("00010039", new ServiceConf("createMoreExtractSquid", String.class,  null));
        // 添加column
        ServiceConfMap.put("00010040", new ServiceConf("createMoreStageColumn", String.class,  null));
        ServiceConfMap.put("00010041", new ServiceConf("createMoreStageSquidLink", String.class,  null));
        /*
         * 说明： 获取表数据（包含所有列名、列类型及当前页记录）
         * 入参: SquidTableInfo
         * 返回: InfoMessagePacket<SquidDataSet>
         */
        ServiceConfMap.put("10010042", new ServiceConf("queryData", String.class,  null));
        /*
         * 说明： 获取运行时数据（）
         * 入参: SquidRunTimeProperties
         * 返回: InfoMessagePacket<SquidDataSet>
         */
        ServiceConfMap.put("00010043", new ServiceConf("queryRuntimeData", String.class,  null));
        // 拖拽extract的目标列到stage上
        ServiceConfMap.put("00010044", new ServiceConf("drag2StageSquid", String.class,  null));
      
        /*
         * 说明：创建SquidJoin
         * 入参: info
         * 返回: List<SpecialSquidJoin>
         */
        ServiceConfMap.put("00010046", new ServiceConf("createSquidJoin", String.class,  null));
       
        /*createExtract2StageLink
         * 说明： 更新SquidJoin
         * 入参: info
         * 返回: List<SpecialSquidJoin>
         */
        ServiceConfMap.put("00010047", new ServiceConf("updateSquidJoin", String.class,  null));

        /**
         * 说明：根据transformation创建column 和Link
         */
        ServiceConfMap.put("00010048", new ServiceConf("createColumnAndLinkByTrans", String.class,  null));

        /**
         * 说明：匹配referenceColumn 和column ,类型，名称一致，且column没有连线，则添加link
         */
        ServiceConfMap.put("00010049", new ServiceConf("autoMatchColumnByNameAndType", String.class,  null));
        ServiceConfMap.put("00010050", new ServiceConf("createIDColumn", String.class,  null));
        ServiceConfMap.put("00010051", new ServiceConf("createExtractionDateColumn", String.class,  null));

        //运行一个squidflow
        ServiceConfMap.put("00020049", new ServiceConf("runSquidFlow", String.class,  null));
        // 三、 查询（操作）权限
        /*
         * 说明：查询user操作权限
         * 入参: User (只需要两个属性：user.id user.role_id)
         * 返回: ListMessagePacket<Privilege>
         */
        ServiceConfMap.put("00050011", new ServiceConf("getUserPrivilege", String.class,  null));
    
        /*
         * 说明：查询Role操作权限
         * 入参: Role (只需要两个属性：role.id role.group_id)
         * 返回: ListMessagePacket<Privilege>
         */
        ServiceConfMap.put("00050012", new ServiceConf("getRolePrivilege", String.class,  null));

        /*
         * 说明：查询Group操作权限
         * 入参: Group (只需要两个属性：group.id group.parent_group_id)
         * 返回: ListMessagePacket<Privilege>
         */
        ServiceConfMap.put("00050013", new ServiceConf("getGroupPrivilege", String.class,  null));
       
        // 四、 设置（操作）权限
        /*
         * 说明：设置（操作）权限，已设置则update，未设置则insert
         * 入参: List<Privilege>
         * 返回: ListMessagePacket<InfoPacket>
         */
        ServiceConfMap.put("00050014", new ServiceConf("setPrivilege", String.class,  null));
     
        /*
         * 说明：将数据库所有的表信息都查询出来，返回前端
         * 入参: info
         * 返回: ListMessagePacket<InfoPacket>
         */
        ServiceConfMap.put("00060001", new ServiceConf("queryAllTableName", String.class,  null));
    
        /*
         * 说明：对元数据进行有效性校验，具体内容为ds前缀的表(不包含ds_sys系统表),枚举类型的表，以及具有主外键约束的表
         * 入参: info
         * 返回: ListMessagePacket<InfoPacket>
         */
        ServiceConfMap.put("00060002", new ServiceConf("checkAllTable", String.class,  null, SEND_RESPONSE_NO));
    
        ServiceConfMap.put("00070001", new ServiceConf("cancle", Void.TYPE, null, SEND_RESPONSE_NO));
         /*
         * 说明： 获取所有team列表（登录前加载）
         * 入参: 无
         * 返回: ListMessagePacket<Team>
         */
        ServiceConfMap.put("00050001", new ServiceConf("getAllTeams",
                Void.TYPE, null));
    
        
        
// sprint6 新接口
        
        /*
         * 拖拽source column集合到空的extract，并生成transformation (或link)
         * 入参、出参：ExtractSquidAndSquidLink{ExtractSquid, SquidLink}
         * (将squid flow的key填充到包头;如果link的from_squid_id和to_squid_id都不存在，即表示第一次拖拽，link尚未生成)
         */
        ServiceConfMap.put("00010103", new ServiceConf("drag2EmptyExtractSquid", String.class,  null, true, true));
        
        /*
         * 拖拽source column集合到空白区，并生成extract、link和transformation
         * 入参、出参：ExtractSquidAndSquidLink{ExtractSquid, SquidLink}
         */
        ServiceConfMap.put("00010104", new ServiceConf("drag2NewExtractSquid", 
                String.class,  
                new PrivilegeConf(DSObjectType.SQUID, DMLType.INSERT), // 普通用户需要有创建squid权限
                true));
        
        /* 13.10 上下调整 ColumnList 组的位置
         *         入参 List<ReferenceColumnGroup>
         *         出参 ListMessagePacket<InfoPacket>
         */
        ServiceConfMap.put("00010106", new ServiceConf("updateReferenceColumnGroup"));
        /* 拖动transformation打断transformationlink
         *         入参 List<SpecialTransformationAndLinks>
         *         出参List<SpecialTransformationAndLinks>
         */
        // 更新Transformation断线规则，废除该命令号
        //ServiceConfMap.put("00010107", new ServiceConf("drapTransformationAndLink",String.class,null));
        /*
         * 从数据源获取表和列，同时存到缓存表和缓存列表
         */
        ServiceConfMap.put("00010102", new ServiceConf("createConnect", String.class, null, SEND_RESPONSE_NO));
        
        ServiceConfMap.put("00010109", new ServiceConf("updateReferedColumn", String.class,  null));
    
        
// sprint7 新接口        
        //[00010110] 1. squid flow 中的所有key -> List<InfoPacket>
        //[00010111] 2. 所有squid（包含所有数据），分类型、分批（10个）
        //[00010112] 3. 查询所有link
        ServiceConfMap.put("10010110", new ServiceConf("getAllSquidKey", String.class, null));
        ServiceConfMap.put("10010111", new ServiceConf("pushAllSquid", String.class, null, SEND_RESPONSE_NO));
        // update by yi.zhou 改功能合并到 getAllSquidKey中
        //ServiceConfMap.put("10010112", new ServiceConf("getAllSquidLink", String.class, null));
        // added by bo.dang
        ServiceConfMap.put("10010030", new ServiceConf("reloadColumnsBySquidId", String.class,  null));
        // 更新SquidFlow的编译状态 added by bo.dang
        ServiceConfMap.put("10010031", new ServiceConf("updateSquidFlowCompilationStatus", String.class, null));
        // 新浪微博相关
        ServiceConfMap.put("00010113", new ServiceConf("handleSinaWeibo", String.class, null));
        // 统一位置信息更新
        ServiceConfMap.put("10010114", new ServiceConf("updateLocation", String.class, null, SEND_RESPONSE_NO));
        // 复制粘贴column
        ServiceConfMap.put("10010115", new ServiceConf("copySquidColumns", String.class, null));

        //树操作
        ServiceConfMap.put("00090001", new ServiceConf("addNode", String.class, null));
        ServiceConfMap.put("00090002", new ServiceConf("renameNode", String.class, null));
        ServiceConfMap.put("00090003", new ServiceConf("getSubNodes", String.class, null));
        ServiceConfMap.put("00090004", new ServiceConf("deleteNode", String.class, null));

        //数据库节点游览操作
        ServiceConfMap.put("00080001", new ServiceConf("browseConnectionNode", String.class, null));
//        //ftp游览文件
//        ServiceConfMap.put("00080002", new ServiceConf("browseFtpNode", String.class, null));
        //本地文件游览以及ftp游览文件
        ServiceConfMap.put("00080003", new ServiceConf("browseFileNode", String.class, null));
        //查看(数据库表信息或者文件信息)
        ServiceConfMap.put("00080004", new ServiceConf("getNodeData", String.class, null)); 
//        //查看(数据库列 和文件列信息)
//        ServiceConfMap.put("00080005", new ServiceConf("addNode", String.class, null));
        ServiceConfMap.put("00090005", new ServiceConf("dropToSquidFlow", String.class, null));
        ServiceConfMap.put("00090006", new ServiceConf("dropToSpecialSquid", String.class, null));
        ServiceConfMap.put("00090007", new ServiceConf("dropToSquidFlowCoverSourceSquid", String.class, null));
        ServiceConfMap.put("00090008", new ServiceConf("dropToSquidFlowCoverExtractSquid", String.class, null));
        // ReportFolder added by bo.dang
        ServiceConfMap.put("10020000", new ServiceConf("addReportNode", String.class, null));
        ServiceConfMap.put("10020001", new ServiceConf("updateReportNode", String.class, null));
        ServiceConfMap.put("10020002", new ServiceConf("deleteReportNode", String.class, null));
        ServiceConfMap.put("10020003", new ServiceConf("getReportNodeById", String.class, null));
        ServiceConfMap.put("10020004", new ServiceConf("getSubReportNodesById", String.class, null));
        // ReportSquid added by bo.dang
        ServiceConfMap.put("10030000", new ServiceConf("addReportSquid", String.class, null));
        ServiceConfMap.put("10030001", new ServiceConf("updateReportSquid", String.class, null));
        ServiceConfMap.put("10030002", new ServiceConf("deleteReportSquid", String.class, null));
        ServiceConfMap.put("10030004", new ServiceConf("viewReportByURL", String.class, null));
        ServiceConfMap.put("10030005", new ServiceConf("createReportSquidLink", String.class, null));
        // GISMapSquid added by yi.zhou
        ServiceConfMap.put("10031000", new ServiceConf("addGISMapSquid", String.class, null));
        ServiceConfMap.put("10031001", new ServiceConf("updateGISMapSquid", String.class, null));
        ServiceConfMap.put("10031002", new ServiceConf("viewMapByURL", String.class, null));
        ServiceConfMap.put("10031003", new ServiceConf("editMapByURL", String.class, null));
        
        // added by bo.dang
        ServiceConfMap.put("10000000", new ServiceConf("createExtract2StageLink", String.class, null));
        // added by yi.zhou
        ServiceConfMap.put("10000001", new ServiceConf("deleteSquidLink", String.class, null));
        // added by yi.zhou
        ServiceConfMap.put("10000002", new ServiceConf("createJoin", String.class, null));
        // added by bo.dang
        ServiceConfMap.put("10000003", new ServiceConf("deleteSquidJoin", String.class, null));
        // added by bo.dang
        ServiceConfMap.put("10000004", new ServiceConf("updateSquidJoin", String.class, null));
        // added by bo.dang
        ServiceConfMap.put("10000005", new ServiceConf("updateJoinOrder", String.class, null));
        // added by yi.zhou
        ServiceConfMap.put("10000006", new ServiceConf("deleteTransGroup", String.class, null));
        // added by yi.zhou
        ServiceConfMap.put("10000007", new ServiceConf("updateTransGroupOrder", String.class, null));
        
        // added by yi.zhou
        ServiceConfMap.put("10000008", new ServiceConf("deleteJoins", String.class, null));
        // added by yi.zhou
        ServiceConfMap.put("10000009", new ServiceConf("upDateJoinForTypeCond", String.class, null));
        
        // added by yi.zhou
        ServiceConfMap.put("10000100", new ServiceConf("getTemplateDataByTypes", String.class, null));
        // added by yi.zhou
        ServiceConfMap.put("10000101", new ServiceConf("getAllTemplateDataTypes", String.class, null));
        //add by lei.bin
        ServiceConfMap.put("10000200", new ServiceConf("createFileFolderSquid", String.class, null));
        ServiceConfMap.put("10000201", new ServiceConf("createFtpSquid", String.class, null));
        ServiceConfMap.put("10000202", new ServiceConf("createHdfsSquid", String.class, null));
        ServiceConfMap.put("10000203", new ServiceConf("createWeiBoSquid", String.class, null));
        ServiceConfMap.put("10000204", new ServiceConf("createWebSquid", String.class, null));
        ServiceConfMap.put("10000205", new ServiceConf("updateFileFolderSquid", String.class, null));
        ServiceConfMap.put("10000206", new ServiceConf("updateFtpSquid", String.class, null));
        ServiceConfMap.put("10000207", new ServiceConf("updateHdfsSquid", String.class, null));
        ServiceConfMap.put("10000208", new ServiceConf("updateWeiBoSquid", String.class, null));
        ServiceConfMap.put("10000209", new ServiceConf("updateWebSquid", String.class, null));
        ServiceConfMap.put("10000210", new ServiceConf("connectFileFolderSquid", String.class, null));
        ServiceConfMap.put("10000211", new ServiceConf("connectFtpSquid", String.class, null));
        ServiceConfMap.put("10000212", new ServiceConf("connectHdfsSquid", String.class, null));
        ServiceConfMap.put("10000213", new ServiceConf("connectWeiBoSquid", String.class, null));
        ServiceConfMap.put("10000214", new ServiceConf("connectWebSquid", String.class, null));
        ServiceConfMap.put("10000215", new ServiceConf("viewFileData", String.class, null));
        ServiceConfMap.put("10000216", new ServiceConf("viewFtpData", String.class, null));
        ServiceConfMap.put("10000217", new ServiceConf("viewHdfsData", String.class, null));
        ServiceConfMap.put("10000218", new ServiceConf("createWebUrls", String.class, null));
        ServiceConfMap.put("10000219", new ServiceConf("updateWebUrls", String.class, null));
        
        ServiceConfMap.put("10000220", new ServiceConf("createHttpSquid", String.class, null));
        ServiceConfMap.put("10000221", new ServiceConf("updateHttpSquid", String.class, null));
        ServiceConfMap.put("10000222", new ServiceConf("createWebserviceSquid", String.class, null));
        ServiceConfMap.put("10000223", new ServiceConf("updateWebserviceSquid", String.class, null));
        ServiceConfMap.put("10000224", new ServiceConf("connectWebserviceSquid", String.class, null));
        ServiceConfMap.put("10000225",new ServiceConf("setSquidsDest",String.class,null));
        ServiceConfMap.put("10000231", new ServiceConf("createSourceTable", String.class, null));
        ServiceConfMap.put("10000232", new ServiceConf("updateSourceTable", String.class, null));
        ServiceConfMap.put("10000233", new ServiceConf("updateThirdPartyParams", String.class, null));
        // Transformation Inputs added by yi.zhou
        ServiceConfMap.put("10000300", new ServiceConf("getTransformationInputById", String.class, null));
        ServiceConfMap.put("10000301", new ServiceConf("updTransformation", String.class, null));
        ServiceConfMap.put("10000302", new ServiceConf("updTransformInputs", String.class, null));
        ServiceConfMap.put("10000303", new ServiceConf("createTransAndInputs", String.class, null));
        ServiceConfMap.put("10000304", new ServiceConf("drapTrans2InputsAndLink", String.class, null));
        // SquidFlow、Project Inputs added by yi.zhou
        ServiceConfMap.put("10000400", new ServiceConf("updSquidFlow", String.class, null));
        ServiceConfMap.put("10000401", new ServiceConf("updProject", String.class, null));
        // SquidFlow、Project Inputs added by yi.zhou
        ServiceConfMap.put("10000500", new ServiceConf("createExceptionSquid", String.class, null));
        ServiceConfMap.put("10000501", new ServiceConf("updExceptionSquid", String.class, null));
        // SquidFlow、Project Inputs added by yi.zhou
        ServiceConfMap.put("10000600", new ServiceConf("getAllDataMiningSquidInRepository", String.class, null));
        //云的接口，查找模型
        ServiceConfMap.put("10000603",new ServiceConf("getAllDataMiningSquidInCloud",String.class,null));
        ServiceConfMap.put("10000601", new ServiceConf("getDataMiningVersions", String.class, null));
        ServiceConfMap.put("10000602", new ServiceConf("getAllQuantifySquidInRepository", String.class, null));
        // Join added by by yi.zhou
        ServiceConfMap.put("10000700", new ServiceConf("getSquidJoinValidator", String.class, null, SEND_RESPONSE_NO));
        // CDC added by by yi.zhou
        ServiceConfMap.put("10000800", new ServiceConf("manageDataSquidByCDC", String.class, null));
        // FindResult added by yi.zhou
        ServiceConfMap.put("10000900", new ServiceConf("findObjectsForParams", String.class, null));
        ServiceConfMap.put("10000901", new ServiceConf("openSubitemsOfRepository", String.class, null));
        ServiceConfMap.put("10000902", new ServiceConf("openSubitemsOfProject", String.class, null));
        // Persistence added by yi.zhou
        ServiceConfMap.put("10001000", new ServiceConf("setSquidsPersistence", String.class, null));
        ServiceConfMap.put("10001001", new ServiceConf("cancelSquidsPersistence", String.class, null));
        // RefereceColumn added by yi.zhou
        ServiceConfMap.put("10001002", new ServiceConf("createReferenceColumn", String.class, null));
        ServiceConfMap.put("10001003", new ServiceConf("updateReferenceColumn", String.class, null));
        ServiceConfMap.put("10001004", new ServiceConf("deleteReferenceColumn", String.class, null));
        ServiceConfMap.put("10001005", new ServiceConf("updateRefColumnForOrder", String.class, null));
        
        // DestWsSquid added by yi.zhou 2015-01-27
        ServiceConfMap.put("10001100", new ServiceConf("createDestWsSquid", String.class, null));
        ServiceConfMap.put("10001101", new ServiceConf("updateDestWsSquid", String.class, null));
        
        // AnnotationSquid added by yi.zhou 2015-03-30
        ServiceConfMap.put("10001200", new ServiceConf("createAnnotationSquid", String.class, null));
        ServiceConfMap.put("10001201", new ServiceConf("updateAnnotationSquid", String.class, null));
        
        // MongodbSquid added by yi.zhou 2015-05-18
        ServiceConfMap.put("10001300", new ServiceConf("createNOSQLConnectionSquid", String.class, null));
        ServiceConfMap.put("10001301", new ServiceConf("updateNOSQLConnection", String.class, null));
        ServiceConfMap.put("10001302", new ServiceConf("connectNOSQLConnection", String.class, null));
        ServiceConfMap.put("10001303", new ServiceConf("getNoSQLPreviewData", String.class, null));
        ServiceConfMap.put("10001304", new ServiceConf("createMongodbExtractsSquid", String.class, null, SEND_RESPONSE_NO));
        ServiceConfMap.put("10001305", new ServiceConf("updateMongodbExtract", String.class, null));
        ServiceConfMap.put("10001306", new ServiceConf("getMongodbExtract", String.class, null));
        
        // DocExtractSquidService added by bo.dang
        ServiceConfMap.put("10040000", new ServiceConf("createDocExtractSquid", String.class, null));
        ServiceConfMap.put("10040001", new ServiceConf("getSourceDataByDocExtractSquid", String.class, null));
        ServiceConfMap.put("10040002", new ServiceConf("updateDocExtractSquid", String.class, null));
        
        // DocExtractSquidService added by bo.dang
        ServiceConfMap.put("10050000", new ServiceConf("createXmlExtractSquid", String.class, null));
        ServiceConfMap.put("10050001", new ServiceConf("getSourceDataByXmlExtractSquid", String.class, null));
        ServiceConfMap.put("10050002", new ServiceConf("updateXmlExtractSquid", String.class, null));
        // WebLogExtractSquidService added by bo.dang
        ServiceConfMap.put("10060000", new ServiceConf("createWebLogExtractSquid", String.class, null));
        ServiceConfMap.put("10060001", new ServiceConf("getSourceDataByWebLogExtractSquid", String.class, null));
        ServiceConfMap.put("10060002", new ServiceConf("updateWebLogExtractSquid", String.class, null));
        // WeiBoExtractSquidService added by bo.dang
        ServiceConfMap.put("10070000", new ServiceConf("createWeiBoExtractSquid", String.class, null));
        ServiceConfMap.put("10070002", new ServiceConf("updateWeiBoExtractSquid", String.class, null));
        // WebExtractSquidService added by bo.dang
        ServiceConfMap.put("10080000", new ServiceConf("createWebExtractSquid", String.class, null));
        ServiceConfMap.put("10080002", new ServiceConf("updateWebExtractSquid", String.class, null));
        //httpExtrasquid added by lei.bin
        ServiceConfMap.put("10190001", new ServiceConf("createHttpExtractSquid", String.class, null));
        ServiceConfMap.put("10190002", new ServiceConf("getSourceDataByHttpExtractSquid", String.class, null));
        ServiceConfMap.put("10190003", new ServiceConf("updateHttpExtractSquid", String.class, null));
       //webserviceExtractsquid added by lei.bin
        ServiceConfMap.put("10200001", new ServiceConf("createWebserviceExtractSquid", String.class, null));
        ServiceConfMap.put("10200002", new ServiceConf("getSourceDataByWebserviceExtractSquid", String.class, null));
        ServiceConfMap.put("10200003", new ServiceConf("updateWebserviceExtractSquid", String.class, null));
        // DataMiningSquidService added by bo.dang
        ServiceConfMap.put("10100001", new ServiceConf("createDataMiningSquid", String.class, null));
        ServiceConfMap.put("10100002", new ServiceConf("updateDataMiningSquid", String.class, null));


        // managerSquidFlowService added by yi.zhou
        ServiceConfMap.put("10110000", new ServiceConf("runALLWithDebugging", String.class, null));
        ServiceConfMap.put("10110001", new ServiceConf("resumeEngine", String.class, null));
        ServiceConfMap.put("10110004", new ServiceConf("stopDebug", String.class, null));
        //元数据检查
        ServiceConfMap.put("10110005", new ServiceConf("checkMetaDataOfSquidFlow", String.class, null));
        // ManagerSquidFlowService added by yi.zhou
        ServiceConfMap.put("10110100", new ServiceConf("copySquidFlowForParams", String.class, null,SEND_RESPONSE_NO));
        ServiceConfMap.put("10110101", new ServiceConf("copySquidFlowForSquidFlow", String.class, null));
        ServiceConfMap.put("10110200", new ServiceConf("createIndexBySquidId", String.class, null));
        ServiceConfMap.put("10110201", new ServiceConf("createPersistTableByIds", String.class, null));
        ServiceConfMap.put("10110202", new ServiceConf("dropIndexBySquidId", String.class, null));
        ServiceConfMap.put("10110203", new ServiceConf("dropPersistTableByIds", String.class, null));
        ServiceConfMap.put("10110300", new ServiceConf("getConvertedColumns", String.class, null));
        //流式计算SquidFlow 1011
        ServiceConfMap.put("10110601", new ServiceConf("RunFlowCalculationSquidFlow", String.class, null, SEND_RESPONSE_YES));
        ServiceConfMap.put("10110602", new ServiceConf("StopFlowCalculationSquidFlow", String.class, null, SEND_RESPONSE_YES));
        ServiceConfMap.put("10110603", new ServiceConf("GetApplicationStatus", String.class, null, SEND_RESPONSE_YES));
        ServiceConfMap.put("10110604", new ServiceConf("deleteApplicationStatus", String.class, null, SEND_RESPONSE_YES));

        // synchronize by bo.dang
        ServiceConfMap.put("10110500", new ServiceConf("getSquidsSyncStatus", String.class, null));
        ServiceConfMap.put("10110501", new ServiceConf("synchronizeSquids", String.class, null));
        // JobScheduleService added by lei.bin
        ServiceConfMap.put("10120001", new ServiceConf("getAllSchedules", String.class, null));
        ServiceConfMap.put("10120002", new ServiceConf("updateSchedules", String.class, null));
        ServiceConfMap.put("10120003", new ServiceConf("addSchedules", String.class, null));
        ServiceConfMap.put("10120004", new ServiceConf("startRunningLogs", String.class, null,SEND_RESPONSE_NO));
        ServiceConfMap.put("10120005", new ServiceConf("resumeJobSchedule", String.class, null));
        ServiceConfMap.put("10120006", new ServiceConf("suspendJobSchedule", String.class, null));
        ServiceConfMap.put("10130001", new ServiceConf("stopRunningLogs", Void.TYPE, null));
        // SquidIndexsService added by bo.dang
        ServiceConfMap.put("10150000", new ServiceConf("createSquidIndexs", String.class, null));
        ServiceConfMap.put("10150001", new ServiceConf("updateSquidIndexs", String.class, null));
        ServiceConfMap.put("10150002", new ServiceConf("getSquidIndexsListBySquidId", String.class, null));
        ServiceConfMap.put("10150003", new ServiceConf("createSquidIndexsToDB", String.class, null));
        ServiceConfMap.put("10150004", new ServiceConf("dropSquidIndexsFromDB", String.class, null));
        
        ServiceConfMap.put("10170001", new ServiceConf("unLockSquidFlow", String.class, null));
        ServiceConfMap.put("10179002", new ServiceConf("sendAllClient", Void.TYPE, null));
        ServiceConfMap.put("10180008", new ServiceConf("createUser", String.class, null));
        ServiceConfMap.put("10180009", new ServiceConf("updateUser", String.class, null));
        ServiceConfMap.put("10180015", new ServiceConf("getAllUser", Void.TYPE, null));
        ServiceConfMap.put("10180001", new ServiceConf("login", String.class, null));
        ServiceConfMap.put("10180010", new ServiceConf("deleteUser", String.class, null));
        // license added by yi.zhou
        ServiceConfMap.put("10180020", new ServiceConf("getLicenseKey", Void.TYPE, null));
        ServiceConfMap.put("10180021", new ServiceConf("setLicense", String.class, null));
        ServiceConfMap.put("10180022", new ServiceConf("getSquidAllForCount", Void.TYPE, null));
        // license added by akachi
        ServiceConfMap.put("10180030", new ServiceConf("updateUserNew", String.class, null));
        //license added by chenfei
        ServiceConfMap.put("10180011",new ServiceConf("updateUserPassword",String.class,null));
        
        // variable added by yi.zhou
        ServiceConfMap.put("10210001", new ServiceConf("createVariable", String.class, null));
        ServiceConfMap.put("10210002", new ServiceConf("updateVariable", String.class, null));
        ServiceConfMap.put("10210003", new ServiceConf("deleteVariable", String.class, null));
        ServiceConfMap.put("10210004", new ServiceConf("findVariableReferences", String.class, null));
        ServiceConfMap.put("10210005", new ServiceConf("copyVariable", String.class, null));
        ServiceConfMap.put("10210006", new ServiceConf("findVariableExists", String.class, null));
        
        // import、export added by yi.zhou
        ServiceConfMap.put("10220001", new ServiceConf("exportSquidFlows", String.class, null, SEND_RESPONSE_NO));
        ServiceConfMap.put("10230001", new ServiceConf("importSquidFlows", String.class, null));

        // es 落地squid
        ServiceConfMap.put("10240001", new ServiceConf("createDestESSquid", String.class, null, SEND_RESPONSE_YES));
        ServiceConfMap.put("10240002", new ServiceConf("updateDestESSquid", String.class, null, SEND_RESPONSE_YES));
        ServiceConfMap.put("10240003", new ServiceConf("createDestESSquidLink", String.class, null, SEND_RESPONSE_YES));
        // es column
        ServiceConfMap.put("10250001", new ServiceConf("getESColumns", String.class, null, SEND_RESPONSE_YES));
        ServiceConfMap.put("10250002", new ServiceConf("updateESColumns", String.class, null, SEND_RESPONSE_YES));

        //Kafka SquidModelBase
        ServiceConfMap.put("10260001", new ServiceConf("createKafkaConnectionSquid", String.class, null, SEND_RESPONSE_YES));
        ServiceConfMap.put("10260002", new ServiceConf("updateKafkaConnectionSquid", String.class, null, SEND_RESPONSE_YES));
        ServiceConfMap.put("10270001", new ServiceConf("createKafkaExtractSquid", String.class, null, SEND_RESPONSE_YES));
        ServiceConfMap.put("10270002", new ServiceConf("updateKafkaExtractSquid", String.class, null, SEND_RESPONSE_YES));
        ServiceConfMap.put("10270003", new ServiceConf("getKafkaExtractMetadata", String.class, null, SEND_RESPONSE_YES));

        //服务端系统功能
        ServiceConfMap.put("10280001", new ServiceConf("getServiceVersionInfo", String.class, null, SEND_RESPONSE_YES));

        //Hbase SquidModelBase
        ServiceConfMap.put("10290001", new ServiceConf("createHbaseConnectionSquid", String.class, null, SEND_RESPONSE_YES));
        ServiceConfMap.put("10290002", new ServiceConf("updateHbaseConnectionSquid", String.class, null, SEND_RESPONSE_YES));
        ServiceConfMap.put("10290003", new ServiceConf("connectHbaseConnectionSquid", String.class, null, SEND_RESPONSE_YES));
        ServiceConfMap.put("10290004", new ServiceConf("previewHbaseSourceTableData", String.class, null, SEND_RESPONSE_YES));
        ServiceConfMap.put("10300001", new ServiceConf("createHbaseExtractSquid", String.class, null, SEND_RESPONSE_YES));
        ServiceConfMap.put("10300002", new ServiceConf("updateHbaseExtractSquid", String.class, null, SEND_RESPONSE_YES));
        ServiceConfMap.put("10300003", new ServiceConf("getHbaseExtractSquidMetadata", String.class, null, SEND_RESPONSE_YES));

        //Stream stage squid
        ServiceConfMap.put("10310001", new ServiceConf("createStreamStageSquid", String.class, null, SEND_RESPONSE_YES));
        ServiceConfMap.put("10310002", new ServiceConf("updateStreamStageSquid", String.class, null, SEND_RESPONSE_YES));

        //DataSquid落地新功能  by dzp
        ServiceConfMap.put("10320001", new ServiceConf("getSQLForCreateTable", String.class, null, SEND_RESPONSE_YES));
        ServiceConfMap.put("10320002", new ServiceConf("createTableBySQL", String.class, null, SEND_RESPONSE_YES));

        //HDFS落地squid  by dzp
        ServiceConfMap.put("10330001", new ServiceConf("createDestHDFSSquid", String.class, null, SEND_RESPONSE_YES));
        ServiceConfMap.put("10330002", new ServiceConf("updateDestHDFSSquid", String.class, null,SEND_RESPONSE_YES));
        ServiceConfMap.put("10330003", new ServiceConf("createDestHDFSSquidLink", String.class, null,SEND_RESPONSE_YES));
        //hdfs落地squid的column  by dzp
        ServiceConfMap.put("10340001", new ServiceConf("getDestHDFSColumn", String.class, null, SEND_RESPONSE_YES));
        ServiceConfMap.put("10340002", new ServiceConf("updateDestHDFSColumns", String.class, null, SEND_RESPONSE_YES));

        //Impala落地squid  by dzp
        ServiceConfMap.put("10350001", new ServiceConf("createDestImpalaSquid", String.class, null,SEND_RESPONSE_YES));
        ServiceConfMap.put("10350002", new ServiceConf("updateDestImpalaSquid", String.class, null,SEND_RESPONSE_YES));
        ServiceConfMap.put("10350003", new ServiceConf("creatDestImpalaSquidLink", String.class, null,SEND_RESPONSE_YES));
        //Impala落地squid的column  by dzp
        ServiceConfMap.put("10360001", new ServiceConf("getDestImpalaColumn", String.class, null, SEND_RESPONSE_YES));
        ServiceConfMap.put("10360002", new ServiceConf("updateDestImpalaColumns", String.class, null, SEND_RESPONSE_YES));

        //编译检查class
        ServiceConfMap.put("10370001",new ServiceConf("compileValidate",String.class,null,SEND_RESPONSE_NO));
        //调度的时候，先编译状态检查
        ServiceConfMap.put("10370002",new ServiceConf("compileJobSchedule",String.class,null,SEND_RESPONSE_NO));
        //版本控制(生成历史版本)
        ServiceConfMap.put("10380001",new ServiceConf("createSquidVersion",String.class,null,SEND_RESPONSE_NO));
        //基于当前squidflow历史版本列表
        ServiceConfMap.put("10380002",new ServiceConf("getSquidFlowList",String.class,null,SEND_RESPONSE_YES));
        //基于respository面板获取历史版本列表
        ServiceConfMap.put("10380006",new ServiceConf("getRespositorySquidFlowList",String.class,null,SEND_RESPONSE_YES));
        //根据当前squidflow创建squidflow
        ServiceConfMap.put("10380003",new ServiceConf("createSquidFlowByVersion",String.class,null,SEND_RESPONSE_NO));
        //基于历史版本创建新的squidFlow
        ServiceConfMap.put("10380004",new ServiceConf("createNewSquidFlowByVersion",String.class,null,SEND_RESPONSE_NO));
        //删除历史版本
        ServiceConfMap.put("10380005",new ServiceConf("deleteSquidFlowVersion",String.class,null,SEND_RESPONSE_YES));
        //获取project下面的历史版本列表
        ServiceConfMap.put("10380007",new ServiceConf("getSquidHistoryByProjectId",String.class,null,SEND_RESPONSE_YES));


        //新的接口
        //新的接口，打开squidflow
        ServiceConfMap.put("10390001",new ServiceConf("openSquidFlow",String.class,null,SEND_RESPONSE_NO));
        //新的接口，打开repository
        ServiceConfMap.put("10390002",new ServiceConf("openRepository",String.class,null,SEND_RESPONSE_YES));

        //数列云新接口
        ServiceConfMap.put("10400001",new ServiceConf("cloudProject",String.class,null,SEND_RESPONSE_YES));
        //数列云登录新接口
        ServiceConfMap.put("10400002",new ServiceConf("cloudLogin",String.class,null,SEND_RESPONSE_YES));
        //数猎云云端项目删除接口
        ServiceConfMap.put("00011000", new ServiceConf("cloudDeleteRepositoryObject", String.class));

        //groupTagging
        ServiceConfMap.put("10410001",new ServiceConf("createGroupTaggingSquid",String.class,null,SEND_RESPONSE_YES));
        ServiceConfMap.put("10410002",new ServiceConf("updateGroupTaggingSquid",String.class,null,SEND_RESPONSE_YES));
        ServiceConfMap.put("10410004",new ServiceConf("delTaggingLink",String.class,null,SEND_RESPONSE_YES));
        ServiceConfMap.put("10410003",new ServiceConf("createTaggingLink",String.class,null,SEND_RESPONSE_YES));
        //SystemHive
        ServiceConfMap.put("10510001",new ServiceConf("createSystemHiveSquid",String.class,null,SEND_RESPONSE_YES));
        ServiceConfMap.put("10510002",new ServiceConf("updateSystemHiveSquid",String.class,null,SEND_RESPONSE_YES));
        ServiceConfMap.put("10510003",new ServiceConf("createSystemHiveExtractSquids",String.class,null,SEND_RESPONSE_NO));
        ServiceConfMap.put("10510004",new ServiceConf("createSystemHiveExtractSquid",String.class,null,SEND_RESPONSE_YES));
        ServiceConfMap.put("10510007",new ServiceConf("createSystemHiveExtractSquidByColumn",String.class,null,SEND_RESPONSE_YES));
        ServiceConfMap.put("10510005",new ServiceConf("updateSystemHiveExtractSquid",String.class,null,SEND_RESPONSE_YES));
        ServiceConfMap.put("10510006",new ServiceConf("connectSystemHiveConnectionSquid",String.class,null,SEND_RESPONSE_NO));
        ServiceConfMap.put("10510008",new ServiceConf("previewHiveData",String.class,null,SEND_RESPONSE_YES));
        //Cassandra
        ServiceConfMap.put("10610001",new ServiceConf("createCassandraConnectionSquid",String.class,null,SEND_RESPONSE_YES));
        ServiceConfMap.put("10610002",new ServiceConf("updateCassandraConnectionSquid",String.class,null,SEND_RESPONSE_YES));
        ServiceConfMap.put("10610004",new ServiceConf("createCassandraExtractSquid",String.class,null,SEND_RESPONSE_YES));
        ServiceConfMap.put("10610003",new ServiceConf("createCassandraExtractSquids",String.class,null,SEND_RESPONSE_NO));
        ServiceConfMap.put("10610007",new ServiceConf("createCassandraExtractSquidByColumn",String.class,null,SEND_RESPONSE_YES));
        ServiceConfMap.put("10610005",new ServiceConf("updateCassandraExtractSquid",String.class,null,SEND_RESPONSE_YES));
        ServiceConfMap.put("10610006",new ServiceConf("connectCassandraConnectionSquid",String.class,null,SEND_RESPONSE_NO));
        ServiceConfMap.put("10610008",new ServiceConf("previewCassandraData",String.class,null,SEND_RESPONSE_YES));
        //用户自定义squid
        ServiceConfMap.put("10710001",new ServiceConf("createUserDefinedSquid",String.class,null,SEND_RESPONSE_YES));
        ServiceConfMap.put("10710002",new ServiceConf("updateUserDefinedSquid",String.class,null,SEND_RESPONSE_YES));
        ServiceConfMap.put("10710003",new ServiceConf("createUserDefinedSquidLink",String.class,null,SEND_RESPONSE_YES));
        ServiceConfMap.put("10710004",new ServiceConf("deleteUserDefinedSquidLink",String.class,null,SEND_RESPONSE_YES));
        ServiceConfMap.put("10710005",new ServiceConf("changeUserDefinedClassName",String.class,null,SEND_RESPONSE_YES));
        ServiceConfMap.put("10710006",new ServiceConf("updateUserDefinedMappingColumns",String.class,null,SEND_RESPONSE_YES));
        ServiceConfMap.put("10710007",new ServiceConf("updateUserDefinedParameterColumns",String.class,null,SEND_RESPONSE_YES));
        //hive落地
        ServiceConfMap.put("10720001",new ServiceConf("createDestHiveSquid",String.class,null,SEND_RESPONSE_YES));
        ServiceConfMap.put("10720002",new ServiceConf("updateDestHiveSquid",String.class,null,SEND_RESPONSE_YES));
        ServiceConfMap.put("10720003",new ServiceConf("getDestHiveSquidColumn",String.class,null,SEND_RESPONSE_YES));
        ServiceConfMap.put("10720004",new ServiceConf("updateDestHiveSquidColumn",String.class,null,SEND_RESPONSE_YES));
        ServiceConfMap.put("10720005",new ServiceConf("createDestHiveLink",String.class,null,SEND_RESPONSE_YES));
        ServiceConfMap.put("10720006",new ServiceConf("deleteDestSquidLink",String.class,null,SEND_RESPONSE_YES));
        //statistics统计算法
        ServiceConfMap.put("10730001",new ServiceConf("createStatisticsSquid",String.class,null,SEND_RESPONSE_YES));
        ServiceConfMap.put("10730002",new ServiceConf("updateStatisticsSquid",String.class,null,SEND_RESPONSE_YES));
        ServiceConfMap.put("10730003",new ServiceConf("createStatisticsSquidLink",String.class,null,SEND_RESPONSE_YES));
        ServiceConfMap.put("10730004",new ServiceConf("deleteStatisticsSquidLink",String.class,null,SEND_RESPONSE_YES));
        ServiceConfMap.put("10730005",new ServiceConf("updateStatisticsMappingColumns",String.class,null,SEND_RESPONSE_YES));
        ServiceConfMap.put("10730006",new ServiceConf("updateStatisticsParameterColumns",String.class,null,SEND_RESPONSE_YES));
        ServiceConfMap.put("10730007",new ServiceConf("createDataMappingColumn",String.class,null,SEND_RESPONSE_YES));
        ServiceConfMap.put("10730008",new ServiceConf("deleteDataMappingColumn",String.class,null,SEND_RESPONSE_YES));
        ServiceConfMap.put("10730009",new ServiceConf("changeStatisticsSquid",String.class,null,SEND_RESPONSE_YES));
        //Cassandra落地
        ServiceConfMap.put("10740001",new ServiceConf("createDestCassandraSquid",String.class,null,SEND_RESPONSE_YES));
        ServiceConfMap.put("10740002",new ServiceConf("updateDestCassandraSquid",String.class,null,SEND_RESPONSE_YES));
        ServiceConfMap.put("10740003",new ServiceConf("getDestCassandraColumn",String.class,null,SEND_RESPONSE_YES));
        ServiceConfMap.put("10740004",new ServiceConf("updateDestCassandraColumn",String.class,null,SEND_RESPONSE_YES));
        ServiceConfMap.put("10740005",new ServiceConf("createDestCassandraLink",String.class,null,SEND_RESPONSE_YES));
        ServiceConfMap.put("10740006",new ServiceConf("deleteDestCassandraSquidLink",String.class,null,SEND_RESPONSE_YES));
    }

}
