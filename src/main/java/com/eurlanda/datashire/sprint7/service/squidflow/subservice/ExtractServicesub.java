package com.eurlanda.datashire.sprint7.service.squidflow.subservice;

import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.adapter.datatype.DataTypeConvertor;
import com.eurlanda.datashire.adapter.datatype.TypeMapping;
import com.eurlanda.datashire.dao.IDBSquidDao;
import com.eurlanda.datashire.dao.ISourceColumnDao;
import com.eurlanda.datashire.dao.ISquidDao;
import com.eurlanda.datashire.dao.ITransformationInputsDao;
import com.eurlanda.datashire.dao.impl.DBSquidDaoImpl;
import com.eurlanda.datashire.dao.impl.SourceColumnDaoImpl;
import com.eurlanda.datashire.dao.impl.SquidDaoImpl;
import com.eurlanda.datashire.dao.impl.TransformationInputsDaoImpl;
import com.eurlanda.datashire.entity.*;
import com.eurlanda.datashire.entity.operation.BeyondSquidException;
import com.eurlanda.datashire.entity.operation.ExtractSquidAndSquidLink;
import com.eurlanda.datashire.enumeration.DMLType;
import com.eurlanda.datashire.enumeration.DSObjectType;
import com.eurlanda.datashire.enumeration.ExtractRowFormatEnum;
import com.eurlanda.datashire.enumeration.MessageBubbleCode;
import com.eurlanda.datashire.enumeration.SquidTypeEnum;
import com.eurlanda.datashire.enumeration.TransformationTypeEnum;
import com.eurlanda.datashire.enumeration.datatype.DbBaseDatatype;
import com.eurlanda.datashire.enumeration.datatype.SystemDatatype;
import com.eurlanda.datashire.server.utils.TokenUtil;
import com.eurlanda.datashire.sprint7.plug.ConnectPlug;
import com.eurlanda.datashire.sprint7.service.squidflow.AbstractRepositoryService;
import com.eurlanda.datashire.sprint7.service.squidflow.DataShirServiceplug;
import com.eurlanda.datashire.sprint7.service.squidflow.RepositoryService;
import com.eurlanda.datashire.sprint7.service.squidflow.RepositoryServiceHelper;
import com.eurlanda.datashire.sprint7.service.squidflow.SquidService;
import com.eurlanda.datashire.sprint7.service.squidflow.TransformationService;
import com.eurlanda.datashire.utility.*;
import com.eurlanda.datashire.validator.SquidValidationTask;
import com.mongodb.DB;

import java.io.File;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Extract SquidModelBase 业务处理 1.拖表创建，支持批量创建（从source table list选择一个或多个到面板空白区）
 * 2.拖列创建（从source table中选择一列或多列到面板空白区或空的extract上） 3.详细信息加载（query details when
 * load squid flow）
 *
 * @author dang.lu 2013.11.12
 */
public class ExtractServicesub extends AbstractRepositoryService implements
        IExtractService {

    // 异常处理机制
    ReturnValue out = new ReturnValue();

    public ExtractServicesub(String token) {
        super(token);
    }

    public ExtractServicesub(IRelationalDataManager adapter) {
        super(adapter);
    }

/*    public static List<Column> columnList = null;

    public static List<ReferenceColumn> referenceColumnList = null;

    public static List<Transformation> transformationList = null;

    public static List<TransformationLink> transformationLinkList = null;

    public static List<TransformationInputs> transformationInputsList = null;*/

    public ExtractServicesub(String token, IRelationalDataManager adapter) {
        super(token, adapter);
    }

    public static void main(String[] args) {
        String str = "DDSD‘SS.dd.dd+";
        if (str.contains(".")) {

            System.out.println(str.replace(".","_"));
        }
        System.out.println(str.replace(".","_").replace("+","_").replace("-","_").replace("(","").replace(")","").replace("=",""));
    }

    /**
     * unicode转string字符串 解决压缩类型获取元数据，序列化出现unicode编码的问题
     */
    public static String revert(String str) {
        str = (str == null ? "" : str);
        if (str.indexOf("\\u") == -1)//如果不是unicode码则原样返回
            return str;

        StringBuffer sb = new StringBuffer(1000);

        for (int i = 0; i < str.length() - 6; ) {
            String strTemp = str.substring(i,i + 6);
            String value = strTemp.substring(2);
            int c = 0;
            for (int j = 0; j < value.length(); j++) {
                char tempChar = value.charAt(j);
                int t = 0;
                switch (tempChar) {
                    case 'a':
                        t = 10;
                        break;
                    case 'b':
                        t = 11;
                        break;
                    case 'c':
                        t = 12;
                        break;
                    case 'd':
                        t = 13;
                        break;
                    case 'e':
                        t = 14;
                        break;
                    case 'f':
                        t = 15;
                        break;
                    default:
                        t = tempChar - 48;
                        break;
                }

                c += t * ((int) Math.pow(16,(value.length() - j - 1)));
            }
            sb.append((char) c);
            i = i + 6;
        }
        return sb.toString();
    }

    /*
     * 兼容从source_column列表第一次或重复拖拽到空的extract或空白区(生成新的extract)
     * （如果目标extract已经创建且已经有目标列，需要前台控制新导入的列是否重复或与已存在列冲突，同时需要是同一张表导入的）
     *
     * 1.如果 extract squid 不存在则创建 2.如果 squid link 不存在则创建
     * （from_squid_id需要前台设置，即从哪个db_source_squid拖拽出来的列）
     * 3.创建目标列和引用列（目标列自动增加一列：extraction_date,该列不参与虚拟变换，也不校验其是否参与虚拟变换） 4.如果来源
     * transformation不存在则创建 (虚拟变换) 5.创建目标 transformation (虚拟变换) 6.创建
     * transformation link (虚拟-虚拟，不包括自动增加的那一列)
     */
    public ExtractSquidAndSquidLink drag2ExtractSquid(
            ExtractSquidAndSquidLink vo,ReturnValue out) {
        // 获得ExtractSquid信息
        TableExtractSquid extractSquid = vo.getExtractSquid();
        // 获得SquidLink信息
        SquidLink squidLink = vo.getSquidLink();
        squidLink.setType(DSObjectType.SQUIDLINK.value());
        // 得到来源ID
        int squidFromId = squidLink.getFrom_squid_id();
        List<Column> sourceColumns = extractSquid.getColumns();
        if (sourceColumns == null || sourceColumns.isEmpty()) {
            logger.warn("source squid 列为空！ source_squid_id = " + squidFromId);
            if (extractSquid.getSourceColumns() == null
                    || extractSquid.getSourceColumns().isEmpty()) {
                return vo;
            } else {
                sourceColumns = BOHelper.convert3(extractSquid
                        .getSourceColumns());
            }
        }

        CheckExtractService check = new CheckExtractService(token,adapter);
        String tableName = extractSquid.getTable_name();
        extractSquid.getSource_table_id();
        // 理论上link和extract都应该有squi_flow_id，且值相同（可能前台有一个没有设置）
        int squi_flow_id = 0;
        if (extractSquid.getSquidflow_id() > 0) {
            squi_flow_id = extractSquid.getSquidflow_id();
        }
        if (squidLink.getSquid_flow_id() > 0) {
            squi_flow_id = squidLink.getSquid_flow_id();
        }
        if (squi_flow_id <= 0) {
            logger.warn("squid_flow_id = " + squi_flow_id);
            return vo;
        }
        extractSquid.setSquidflow_id(squi_flow_id);
        squidLink.setSquid_flow_id(squi_flow_id);

        // 需要创建/填充的数据
        List<ReferenceColumn> referenceColumns = new ArrayList<ReferenceColumn>(); // BOHelper.convert(sourceColumns);
        List<Column> columns = new ArrayList<Column>();
        List<Transformation> transformations = new ArrayList<Transformation>();
        List<TransformationLink> transformationLinks = new ArrayList<TransformationLink>();

        // 临时变量
        Map<String, Object> params = new HashMap<String, Object>(); // 查询参数
        List<Transformation> tmpTrans = null; // 看来源 transformation 是否已存在
        List<ReferenceColumnGroup> colGroup = null; // 看引用列组是否存在
        List<Column> tmpCol = null; // 看引用列组是否存在
        int tmpFromTransId = 0;
        int tmpToTransId = 0;
        int tmpIndex = 1;
        int columnSize; // 已经存在的目标列
        Transformation transformationLeft = null; // 看来源 transformation 是否已存在
        Transformation transformationRight = null; // 看来源 transformation 是否已存在
        TransformationLink transformationLink = null;
        int transformationFromId;
        int sourceColumnSize;
        String sourceKey = null; // source squid 的key（推送table是否已抽取）
        try {
            adapter.openSession();
            ITransformationInputsDao transInputsDao = new TransformationInputsDaoImpl(adapter);
            // 更新SourceTabel中的is_extracted
            String sql = "update ds_source_table set is_extracted = 'Y' where id = " + extractSquid.getSource_table_id() + " and source_squid_id = " + squidFromId;
            adapter.execute(sql);
            params.clear();
            params.put("ID",squidFromId);
            sourceKey = adapter.query2Object(true,params,Squid.class)
                    .getKey();

            // 创建 ExtractSquid
            if (StringUtils.isNull(extractSquid.getKey())) {
                extractSquid.setKey(StringUtils.generateGUID());
            }
            if (extractSquid.getId() <= 0) {
                //extractSquid.setSquid_type(SquidTypeEnum.EXTRACT.value());
                SquidService service = new SquidService(token);
                extractSquid.setName(service.getSquidNameAllowChinese(adapter,extractSquid.getTable_name(),extractSquid.getSquidflow_id()));//NAME

                // add zdb 添加split col, split num   2015-7-28
                initExtractSquidSplitParams(extractSquid,adapter);

                extractSquid.setId(adapter.insert2(extractSquid));
            }
            // 创建 SquidLink
            squidLink.setTo_squid_id(extractSquid.getId());
            if (StringUtils.isNull(squidLink.getKey())) {
                squidLink.setKey(StringUtils.generateGUID());
            }
            if (squidLink.getId() <= 0) {
                squidLink.setLine_type(1);
                squidLink.setId(adapter.insert2(squidLink));
            }
            params.clear();
            params.put("ID",squidFromId);
            Squid sourceSquid = adapter.query2Object(true,params,Squid.class);
            if (StringUtils.isNotNull(sourceSquid)) {
                sourceKey = sourceSquid.getKey();
            }

            int dbType = 0;
            ISquidDao squidDao = new SquidDaoImpl(adapter);
            int squidTypeId = squidDao.getSquidTypeById(squidFromId);
            String dbTypeSql = null;
            if(squidTypeId==SquidTypeEnum.CASSANDRA.value()){
                dbTypeSql = "select DB_TYPE_ID from ds_squid where id=" + squidFromId + " limit 0,1";
            } else {
                dbTypeSql = "select DB_TYPE_ID from ds_squid where id=" + squidFromId + " limit 0,1";
            }
            Map<String, Object> dbTpMap = adapter.query2Object(true,dbTypeSql,null);
            if (dbTpMap != null && dbTpMap.containsKey("DB_TYPE_ID")) {
                dbType = dbTpMap.get("DB_TYPE_ID") == null ? 0 : Integer.parseInt(dbTpMap.get("DB_TYPE_ID") + "");
            }
            // 创建引用列组
            ReferenceColumnGroup columnGroup = null;
            params.clear();
            params.put("reference_squid_id",extractSquid.getId());
            colGroup = adapter.query2List2(true,params,
                    ReferenceColumnGroup.class);
            if (colGroup == null || colGroup.isEmpty()
                    || colGroup.get(0) == null) {
                columnGroup = new ReferenceColumnGroup();
                columnGroup.setKey(StringUtils.generateGUID());
                columnGroup.setReference_squid_id(extractSquid.getId());
                columnGroup.setRelative_order(1);
                columnGroup.setId(adapter.insert2(columnGroup));
            } else {
                columnGroup = colGroup.get(0);
            }
            referenceColumns = new ArrayList<ReferenceColumn>();
            columns = new ArrayList<Column>();
            transformations = new ArrayList<Transformation>();
            transformationLinks = new ArrayList<TransformationLink>();
            sourceColumnSize = sourceColumns.size();
            Column column = null;
            Column columnInfo = null;
            ReferenceColumn referenceColumn = null;
            int extractSquidId = extractSquid.getId();
            //Transformation transformation = null;
            int transformationToId = 0;
            TransformationService transformationService = new TransformationService(token);
            int relativeOrder = 0;
            int count = 0;

            // 源列和目标列初始相同，生成后用户可以对目标列进行调整
            for (tmpIndex = 0; tmpIndex < sourceColumnSize; tmpIndex++) {
                relativeOrder++;
                count = relativeOrder;
                //需要先增加SourceColumn
                columnInfo = sourceColumns.get(tmpIndex);
                // 目标ExtractSquid引用列（变换面板右边，引用db_source_squid）
                referenceColumn = transformationService.initReference(adapter,columnInfo,columnInfo.getId(),count,sourceSquid,extractSquidId,columnGroup);
                transformationRight = transformationService.initTransformation(
                        adapter,extractSquidId,referenceColumn.getId(),
                        TransformationTypeEnum.VIRTUAL.value(),
                        referenceColumn.getData_type(),1,relativeOrder + 1);
                referenceColumns.add(referenceColumn);
                transformations.add(transformationRight);
                transformationFromId = transformationRight.getId();

                if (dbType > 0) {
                    columnInfo = DataTypeConvertor.getInColumnByColumn(dbType,columnInfo);
                }
                // 目标ExtractSquid真实列（变换面板左边）
                // 替换掉特殊字符
                columnInfo.setName(StringUtils.replace(columnInfo.getName()));
                // fixed bug 980 end by bo.dang
                column = transformationService.initColumn(adapter,columnInfo,count,extractSquidId,null);
                transformationLeft = transformationService.initTransformation(
                        adapter,extractSquidId,column.getId(),
                        TransformationTypeEnum.VIRTUAL.value(),
                        column.getData_type(),1);
                columns.add(column);

                transformations.add(transformationLeft);
                transformationToId = transformationLeft.getId();

                // 创建 transformation link
                transformationLink = transformationService.initTransformationLink(adapter,transformationFromId,transformationToId,0);
                transformationLinks.add(transformationLink);
                // 更新TransformationInputs
                transInputsDao.updTransInputs(transformationLink,transformationLeft);

            }

            extractSquid.setColumns(columns);
            extractSquid.setSourceColumns(referenceColumns);
            extractSquid.setTransformations(transformations);
            extractSquid.setTransformationLinks(transformationLinks);

            // 调用是否抽取方法
            if (StringUtils.isNotNull(tableName)) {
                check.updateExtract(tableName,squidFromId,"create",null,
                        sourceKey,token);
            }
        } catch (Exception e) {
            logger.error("创建extract squid异常",e);
            try {
                adapter.rollback();
            } catch (SQLException e1) { // 数据库回滚失败（程序不能处理该异常）！
                logger.fatal("rollback err!",e1);
            }
            out.setMessageCode(MessageCode.SQL_ERROR);
        } finally {
            adapter.closeSession();
        }
        return vo;
    }

    /**
     * 生成 extract squid split column
     *
     * @param extractSquid
     * @param adapter
     * @throws SQLException
     */
    public void initExtractSquidSplitParams(TableExtractSquid extractSquid,IRelationalDataManager adapter) throws SQLException {
        int sourceTableId = extractSquid.getSource_table_id();

        ISourceColumnDao sourceColumnDao = new SourceColumnDaoImpl(adapter);

        List<SourceColumn> sourceColumns = sourceColumnDao.getSourceColumnByTableId(sourceTableId);

        // 获取数据库类型
        IDBSquidDao dbSquidDao = new DBSquidDaoImpl(adapter);
        int dbType = dbSquidDao.getDBTypeBySourceTableId(sourceTableId);

        // 获取PK
        List<SourceColumn> scs = new ArrayList<>();

        for (SourceColumn sc : sourceColumns) {
            if (sc.isIspk()) {
                scs.add(sc);
            }
        }

        // 切分的列
        SourceColumn splitColumn;

        // 如果存在PK,取其中数字类型的或者时间类型的切分
        if (scs.size() > 0) {
            if (scs.size() == 1) {
                splitColumn = scs.get(0);
            } else {
                splitColumn = getSplitColumn(scs,dbType);
            }
        } else {
            // 如果不存在PK，取其中数字类型或者时间类型切分
            splitColumn = getSplitColumn(sourceColumns,dbType);
        }

        if (splitColumn != null) {
            extractSquid.setSplit_col(splitColumn.getName());
        }
        extractSquid.setSplit_num(0);

    }

    /**
     * 生成HiveExtract 的split column
     * @param extractSquid
     * @param adapter
     */
    public void initHiveExtractSquidSplitParams(SystemHiveExtractSquid extractSquid, IRelationalDataManager adapter) throws SQLException {
        int sourceTableId = extractSquid.getSource_table_id();

        ISourceColumnDao sourceColumnDao = new SourceColumnDaoImpl(adapter);

        List<SourceColumn> sourceColumns = sourceColumnDao.getSourceColumnByTableId(sourceTableId);

        // 获取数据库类型
        IDBSquidDao dbSquidDao = new DBSquidDaoImpl(adapter);
        int dbType = dbSquidDao.getDBTypeBySourceTableId(sourceTableId);

        // 获取PK
        List<SourceColumn> scs = new ArrayList<>();

        for (SourceColumn sc : sourceColumns) {
            if (sc.isIspk()) {
                scs.add(sc);
            }
        }

        // 切分的列
        SourceColumn splitColumn;

        // 如果存在PK,取其中数字类型的或者时间类型的切分
        if (scs.size() > 0) {
            if (scs.size() == 1) {
                splitColumn = scs.get(0);
            } else {
                splitColumn = getSplitColumn(scs,dbType);
            }
        } else {
            // 如果不存在PK，取其中数字类型或者时间类型切分
            splitColumn = getSplitColumn(sourceColumns,dbType);
        }

        if (splitColumn != null) {
            extractSquid.setSplit_col(splitColumn.getName());
        }
        extractSquid.setSplit_num(0);

    }
    /**
     * 从column中获取其中的切分列
     * 优先级：
     * 1. 寻找时间类型的
     * 2. 寻找数字类型的
     * 3. 其余类型
     *
     * @param sourceColumns
     * @return
     */
    private SourceColumn getSplitColumn(List<SourceColumn> sourceColumns,final int dbType) {

        // 过滤
        Set<Integer> typeFilter = CommonConsts.SPLIT_COLUMN_FILTER.get(dbType);
        List<SourceColumn> list = null;
        if (typeFilter != null) {
            list = new ArrayList<>();
            for (SourceColumn sc : sourceColumns) {
                if (!typeFilter.contains(sc.getData_type())) {
                    list.add(sc);
                }
            }
            if (list.size() == 0) {
                return null;
            }
        } else {
            list = sourceColumns;
        }

        Collections.sort(list,new Comparator<SourceColumn>() {

            private int typeToInt(SourceColumn sc) {
                TypeMapping typeMapping = DataTypeConvertor.getInTypeMappingBySourceColumn(dbType,sc);
                if (typeMapping == null) {
                    return 100;
                }
                switch (SystemDatatype.parse(typeMapping.getSysDataType())) {
                    case DATETIME:
                        return 1;
                    case INT:
                    case TINYINT:
                    case SMALLINT:
                    case BIGINT:
                        return 2;
                    case FLOAT:
                        return 3;
                    case DECIMAL:
                        return 4;
                    case BIT:
                        return 5;
                    default:
                        return 6;
                }
            }

            @Override
            public int compare(SourceColumn o1,SourceColumn o2) {
                return -(typeToInt(o2) - typeToInt(o1));
            }
        });
        return list.get(0);
    }

    /**
     * 直接从table list拖拽创建，有可能多选或全选(需要考虑性能问题，可以持续优化)
     */
    public synchronized void drag2ExtractSquid(
            List<ExtractSquidAndSquidLink> voList,ReturnValue out) {
        // 需要创建/填充的数据
        List<ReferenceColumn> referenceColumns;
        List<Column> columns;
        List<Transformation> transformations;
        List<TransformationLink> transformationLinks;
        // 临时变量
        Map<String, Object> params = new HashMap<String, Object>(3); // 查询参数
        Transformation transformationLeft = null; // 看来源 transformation 是否已存在
        Transformation transformationRight = null; // 看来源 transformation 是否已存在
        TransformationLink transformationLink = null;
        int transformationFromId;
        int tmpIndex;
        int sourceColumnSize;
        String sourceKey = null; // source squid 的key（推送table是否已抽取）
        String sql = null;
        try {
            CheckExtractService check = new CheckExtractService(token,adapter);
            adapter.openSession(); // 在循环外开启事务，所有操作都必须在同一个事务中，成功则提交，失败回滚
            ITransformationInputsDao transInputsDao = new TransformationInputsDaoImpl(adapter);
            ISquidDao squidDao = new SquidDaoImpl(adapter);
            SquidService service = new SquidService(TokenUtil.getToken());
            for (int i = 0; i < voList.size(); i++) {
                // 获得ExtractSquid信息
                TableExtractSquid extractSquid = voList.get(i)
                        .getExtractSquid();
                // 获得SquidLink信息
                SquidLink squidLink = voList.get(i).getSquidLink();
                squidLink.setType(DSObjectType.SQUIDLINK.value());
                // 给当前ExtractSquid的source_table_id赋值
                int from_squid_id = squidLink.getFrom_squid_id();

                int dbType = 0;
                int squidTypeId=squidDao.getSquidTypeById(from_squid_id);
                String dbTypeSql = null;
                if(squidTypeId==SquidTypeEnum.CASSANDRA.value()){
                    dbTypeSql = "select DB_TYPE_ID from DS_SQUID where id="+from_squid_id+" limit 0,1";
                } else {
                    dbTypeSql = "select DB_TYPE_ID from DS_SQUID where id=" + from_squid_id + " limit 0,1";
                }
                Map<String, Object> dbTpMap = adapter.query2Object(true,dbTypeSql,null);
                if (dbTpMap != null && dbTpMap.containsKey("DB_TYPE_ID")) {
                    dbType = dbTpMap.get("DB_TYPE_ID") == null ? 0 : Integer.parseInt(dbTpMap.get("DB_TYPE_ID") + "");
                }
                // 得到来源ID
                int squidFromId = squidLink.getFrom_squid_id();
                String tableName = extractSquid.getTable_name();
                List<SourceColumn> sourceColumns = adapter
                        .query2List(
                                true,
                                "select c.* from DS_SOURCE_TABLE t, DS_SOURCE_COLUMN c"
                                        + " where t.id=c.source_table_id and t.source_squid_id="
                                        + squidFromId + " and t.id="
                                        + extractSquid.getSource_table_id(),null,
                                SourceColumn.class);
                if (sourceColumns == null || sourceColumns.isEmpty()) {
                    logger.warn("source squid 列为空！ source_squid_id = "
                            + squidFromId + ", tableName = " + tableName);
                    continue;
                }

                params.clear();
                params.put("ID",squidFromId);
                Squid sourceSquid = adapter.query2Object(true,params,Squid.class);
                if (StringUtils.isNotNull(sourceSquid)) {
                    sourceKey = sourceSquid.getKey();
                }
                // cdc的默认值是SCD0 added by bo.dang
                if (StringUtils.isNull(extractSquid.getCdc())) {
                    extractSquid.setCdc(0);// 0:SCD0
                }
                // 创建 ExtractSquid
                if (StringUtils.isNull(extractSquid.getKey())) {
                    extractSquid.setKey(StringUtils.generateGUID());
                }
                if (extractSquid.getId() <= 0) {
                    //查找出上游的squid是否是hive
                    squidTypeId = squidDao.getSquidTypeById(from_squid_id);
                    if(squidTypeId==SquidTypeEnum.HIVE.value()){
                        extractSquid.setSquid_type(SquidTypeEnum.HIVEEXTRACT.value());
                    } else if(squidTypeId==SquidTypeEnum.CASSANDRA.value()){
                        extractSquid.setSquid_type(SquidTypeEnum.CASSANDRA_EXTRACT.value());
                    } else {
                        extractSquid.setSquid_type(SquidTypeEnum.EXTRACT.value());
                    }
                    // add zdb 添加split col, split num   2015-7-28
                    initExtractSquidSplitParams(extractSquid,adapter);
                    extractSquid.setId(adapter.insert2(extractSquid));
                }
                // 创建 SquidLink
                squidLink.setTo_squid_id(extractSquid.getId());
                if (StringUtils.isNull(squidLink.getKey())) {
                    squidLink.setKey(StringUtils.generateGUID());
                }
                if (squidLink.getId() <= 0) {
                    squidLink.setLine_type(1);
                    squidLink.setId(adapter.insert2(squidLink));
                }
                // 更新SourceTabel中的is_extracted,ref_squid
                sql = "update ds_source_table set is_extracted = 'Y' where id = " + extractSquid.getSource_table_id() + " and source_squid_id = " + squidFromId;

                adapter.execute(sql);
                // 创建引用列组
                ReferenceColumnGroup columnGroup = new ReferenceColumnGroup();
                columnGroup.setKey(StringUtils.generateGUID());
                columnGroup.setReference_squid_id(extractSquid.getId());
                columnGroup.setRelative_order(1);
                columnGroup.setId(adapter.insert2(columnGroup));

                referenceColumns = new ArrayList<ReferenceColumn>();
                columns = new ArrayList<Column>();
                transformations = new ArrayList<Transformation>();
                transformationLinks = new ArrayList<TransformationLink>();
                sourceColumnSize = sourceColumns.size();
                Column column = null;
                SourceColumn columnInfo = null;
                ReferenceColumn referenceColumn = null;
                int extractSquidId = extractSquid.getId();

                int transformationToId = 0;
                TransformationService transformationService = new TransformationService(token);
                int relativeOrder = 0;
                int count = 0;
                // updated by yi.zhou bug 1911
                Map<String, String> tempMap = new HashMap<String, String>();

                // 源列和目标列初始相同，生成后用户可以对目标列进行调整
                Map<String, Object> nameMap = new HashMap<String, Object>();
                for (tmpIndex = 0; tmpIndex < sourceColumnSize; tmpIndex++) {
                    relativeOrder++;
                    count = relativeOrder;
                    columnInfo = sourceColumns.get(tmpIndex);
                    // update DbBaseDataType start by yi.zhou
                    // 目标ExtractSquid引用列（变换面板右边，引用db_source_squid）
                    referenceColumn = transformationService.initReference(adapter,columnInfo,columnInfo.getId(),count,sourceSquid,extractSquidId,columnGroup);
                    referenceColumns.add(referenceColumn);
                    if (dbType > 0) {
                        columnInfo = DataTypeConvertor.getInColumnBySourceColumn(dbType,columnInfo); /*格局根据数据库转换Column的类型*/
                    }
                    // 目标ExtractSquid真实列（变换面板左边）
                    // 替换掉特殊字符
                    columnInfo.setName(StringUtils.replace(columnInfo.getName()));
                    // fixed bug 980 start by bo.dang
                    if (tempMap.containsKey(columnInfo.getName().toLowerCase())) {
                        String newColumnName = this.getSysColumnName(columnInfo.getName().toLowerCase(),tempMap,1);
                        columnInfo.setName(newColumnName);
                    }
                    tempMap.put(columnInfo.getName().toLowerCase(),columnInfo.getName().toLowerCase());
                    // fixed bug 980 end by bo.dang
                    column = transformationService.initColumn(adapter,columnInfo,count,extractSquidId,nameMap);
                    transformationLeft = transformationService.initTransformation(
                            adapter,extractSquidId,column.getId(),
                            TransformationTypeEnum.VIRTUAL.value(),
                            column.getData_type(),1);
                    columns.add(column);
                    transformations.add(transformationLeft);

                    //目标ExtractSquid的引用列类型
                    transformationRight = transformationService.initTransformation(
                            adapter,extractSquidId,referenceColumn.getId(),
                            TransformationTypeEnum.VIRTUAL.value(),
                            referenceColumn.getData_type(),1);
                    transformations.add(transformationRight);

                    //update DbBaseDataType end by yi.zhou

                    //设置Transformation连线
                    transformationToId = transformationLeft.getId();
                    transformationFromId = transformationRight.getId();
                    // 创建 transformation link
                    transformationLink = transformationService.initTransformationLink(adapter,transformationFromId,transformationToId,count);
                    transformationLinks.add(transformationLink);
                    // 更新TransformationInputs
                    transInputsDao.updTransInputs(transformationLink,transformationLeft);
                }
                extractSquid.setColumns(columns);
                extractSquid.setSourceColumns(referenceColumns);
                extractSquid.setTransformations(transformations);
                extractSquid.setTransformationLinks(transformationLinks);
                // 调用是否抽取方法
                if (StringUtils.isNotNull(tableName)) {
                    check.updateExtract(tableName,squidFromId,"create",null,
                            sourceKey,token);
                }
                if (i >= 1)
                    Thread.sleep(20); // 如果前台一次拖拽多个，需要睡眠，否则cpu会急速上升反而降低性能
/*                // 调用取消孤立squid消息泡
                PushMessagePacket.push(new MessageBubble(sourceKey, sourceKey,
                        MessageBubbleCode.WARN_SQUID_NO_LINK.value(), true),
                        token);*/
                // 消息泡验证
                CommonConsts.addValidationTask(new SquidValidationTask(token,adapter,MessageBubbleService.setMessageBubble(from_squid_id,from_squid_id,null,MessageBubbleCode.WARN_SQUID_NO_LINK.value())));
                CommonConsts.addValidationTask(new SquidValidationTask(token,adapter,MessageBubbleService.setMessageBubble(from_squid_id,from_squid_id,null,MessageBubbleCode.ERROR_DATA_SQUID_NO_INCREMENTAL_EXPRESSION.value())));
                CommonConsts.addValidationTask(new SquidValidationTask(token,adapter,MessageBubbleService.setMessageBubble(from_squid_id,from_squid_id,null,MessageBubbleCode.ERROR_SQL_SYNTAX_INCREMENT.value())));
                CommonConsts.addValidationTask(new SquidValidationTask(token,adapter,MessageBubbleService.setMessageBubble(from_squid_id,from_squid_id,null,MessageBubbleCode.ERROR_DB_TABLE_NAME.value())));
            }
        } catch (BeyondSquidException e) {
            try {
                if (adapter != null) adapter.rollback();
            } catch (SQLException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }
            out.setMessageCode(MessageCode.ERR_SQUID_COUNT_MAX);
            logger.error("创建extract squid异常",e);
        } catch (Exception e) {
            logger.error("创建extract squid异常",e);
            try {
                adapter.rollback();
            } catch (SQLException e1) { // 数据库回滚失败（程序不能处理该异常）！
                logger.fatal("rollback err!",e1);
            }
            out.setMessageCode(MessageCode.SQL_ERROR);
        } finally {
            adapter.closeSession();
        }
    }

    /**
     * 获取ExtractSquid详细信息（如果不在同一个事务里，adapter连接打开、关闭需要调用函数自己处理）
     */
    public void setExtractSquidData(String token,
                                    final TableExtractSquid squid,boolean inSession,
                                    IRelationalDataManager adapter) {
        squid.setType(DSObjectType.EXTRACT.value());
        if (squid != null) {
            logger.debug("(extract) squid detail, id=" + squid.getId()
                    + ", name=" + squid.getName() + ", table_name="
                    + squid.getTable_name() + ", type="
                    + SquidTypeEnum.parse(squid.getSquid_type()));
        }
        Map<String, String> paramMap = new HashMap<String, String>(1);
        Map<String, Object> paramMap2;
        List<ReferenceColumn> sourceColumnlist = null;
        List<Integer> sourceColumnId = new ArrayList<Integer>();
        List<Integer> transId = new ArrayList<Integer>();
        // boolean host_column_deleted = false;
        int source_squid_id = 0;
        try {
            if (!inSession) {
                adapter.openSession();
            }
            // 所有目标列
            if (squid.getColumns() == null || squid.getColumns().isEmpty()) {
                paramMap.clear();
                paramMap.put("squid_id",Integer.toString(squid.getId(),10));
                squid.setColumns(adapter.query2List(true,paramMap,
                        Column.class));
            }
            // 是否增量抽取及表达式
            paramMap2 = adapter.query2Object(true,
                    "SELECT IS_INCREMENTAL, INCREMENTAL_EXPRESSION FROM ds_squid WHERE ID="
                            + squid.getId(),null);
            if (paramMap2 != null && !paramMap2.isEmpty()) {
                squid.setIs_incremental("Y".equals(StringUtils.valueOf2(
                        paramMap2,"is_incremental")));
                squid.setIncremental_expression(StringUtils.valueOf2(paramMap2,
                        "incremental_expression"));
            } else {
                paramMap2 = new HashMap<String, Object>(5);
            }
            // 所有引用列
            sourceColumnlist = adapter
                    .query2List(
                            true,
                            "select c.id,r.host_squid_id squid_id,r.*, nvl2(c.id, 'N','Y') host_column_deleted from"
                                    + " DS_SOURCE_COLUMN c right join DS_REFERENCE_COLUMN r on r.column_id=c.id"
                                    + " where r.reference_squid_id="
                                    + squid.getId(),null,
                            ReferenceColumn.class);
            squid.setSourceColumns(sourceColumnlist);

            // 所有ColumnId
            if (sourceColumnlist != null && !sourceColumnlist.isEmpty()) {
                for (int i = 0; i < sourceColumnlist.size(); i++) {
                    if (!sourceColumnId.contains(sourceColumnlist.get(i)
                            .getColumn_id())) {
                        sourceColumnId.add(sourceColumnlist.get(i)
                                .getColumn_id());
                    }
                    // if(sourceColumnlist.get(i).isHost_column_deleted()){
                    // logger.warn("host_column_deleted: "+sourceColumnlist.get(i));
                    // host_column_deleted = true; // 只要有一个上游列被删除就表示有问题
                    // }
                    if (sourceColumnlist.get(i).getSquid_id() > 0) {
                        source_squid_id = sourceColumnlist.get(i).getSquid_id();
                    }
                }
            }

            // 所有transformation（包括虚拟、真实变换）
            // logger.debug("get all transformation by squid_id = "+squid.getId());
            paramMap.clear();
            paramMap.put("squid_id",Integer.toString(squid.getId(),10));
            List<Transformation> transformations = adapter.query2List(true,
                    paramMap,Transformation.class);
            if (transformations != null && !transformations.isEmpty()) {
                for (int i = 0; i < transformations.size(); i++) {
                    transId.add(transformations.get(i).getId());
                }
            }

            // 所有上游引用列的虚拟变换
            if (sourceColumnId != null && !sourceColumnId.isEmpty()) {
                paramMap2.clear();
                if (source_squid_id > 0) {
                    paramMap2.put("squid_id",source_squid_id);
                }
                paramMap2.put("column_id",sourceColumnId);
                paramMap2.put("transformation_type_id",
                        TransformationTypeEnum.VIRTUAL.value());
                StringUtils.addAll(transformations,adapter.query2List2(true,
                        paramMap2,Transformation.class));
            }
            squid.setTransformations(transformations);

            // 所有transformation link
            List<TransformationLink> transformationLinks = null;
            if (transId != null && !transId.isEmpty()) {
                paramMap2.clear();
                paramMap2.put("TO_TRANSFORMATION_ID",transId);
                transformationLinks = adapter.query2List2(true,paramMap2,
                        TransformationLink.class);
                squid.setTransformationLinks(transformationLinks);
            }
            // 上游的column被删除
            // PushMessagePacket.push(
            // new MessageBubble(squid.getKey(), squid.getKey(),
            // MessageBubbleCode.ERROR_REFERENCE_COLUMN_DELETED.value(),
            // !host_column_deleted),
            // token);
            if (DebugUtil.isDebugenabled())
                logger.debug(DebugUtil.squidDetail(squid));
        } catch (Exception e) {
            logger.error("getExtractSquidData-datas",e);
        } finally {
            if (!inSession) {
                adapter.closeSession();
            }
        }
    }

    /**
     * @param sourceColumnList
     * @param sourceTableId
     * @return
     * @author bo.dang
     * @date 2014年5月10日
     */
    public List<SourceColumn> compareSourceColumnList(
            List<SourceColumn> sourceColumnList,int sourceTableId) {
        int executeResult = 1;// sql语句执行状态,或者主键返回值
        List<SourceColumn> sourceColumnCacheList = new ConnectPlug(adapter)
                .getColumnById(true,sourceTableId);
        List<SourceColumn> resultSourceColumnList = null;
        try {
            if ((StringUtils.isNull(sourceColumnCacheList)
                    || sourceColumnCacheList.isEmpty()) && StringUtils.isNotNull(sourceColumnList)) {
                SourceColumn sourceColumn = null;
                resultSourceColumnList = new ArrayList<SourceColumn>();
                for (int i = 0; i < sourceColumnList.size(); i++) {
                    sourceColumn = sourceColumnList.get(i);
                    sourceColumn.setId(adapter.insert2(sourceColumn));
                    resultSourceColumnList.add(sourceColumn);
                }

                return resultSourceColumnList;
            }
            Map<String, String> paramMap = new HashMap<String, String>();

            // 比较同字段名列的记录，将缓存表中有而数据源中没有的列删除
            List<SourceColumn> deleteColumns = this.compareColumns(
                    sourceColumnList,sourceColumnCacheList);
            for (SourceColumn deleteColumn : deleteColumns) {
                logger.debug("[需要删除的列名=]" + deleteColumn.getName());
                paramMap.clear();
                paramMap.put("id",Integer.toString(deleteColumn.getId(),10));
                paramMap.put("source_table_id",
                        String.valueOf(deleteColumn.getSource_table_id()));
                adapter.delete(paramMap,SourceColumn.class);


            }
            // 增加数据源中有而缓存表中没有的列
            List<SourceColumn> addColumns = this.compareColumns(
                    sourceColumnCacheList,sourceColumnList);
            for (SourceColumn addColumn : addColumns) {
                logger.debug("[需要增加的列名=]" + addColumn.getName());
                executeResult = adapter.insert2(addColumn);
                if (executeResult < 1) {
                    break;
                }
            }
            if (executeResult < 1) {
                out.setMessageCode(MessageCode.SQL_ERROR);
            }
            // 根据source_table_id查询所有的列
            resultSourceColumnList = new ConnectPlug(adapter).getColumnById(
                    true,sourceTableId);
            logger.debug("[最终的列长度==]" + resultSourceColumnList.size());
            // 更新最新缓存列和数据源列同名的数据(暂时未做)
        } catch (Exception e) {
            out.setMessageCode(MessageCode.SQL_ERROR);
            logger.error("创建extract squid异常",e);
            try {
                adapter.rollback();
            } catch (SQLException e1) { // 数据库回滚失败（程序不能处理该异常）！
                logger.fatal("rollback err!",e1);
            }
        } finally {
            //adapter.closeSession();
        }
        return resultSourceColumnList;
    }

    /**
     * 抽取list2中有，而list1中没有的元素
     *
     * @param sourceColumnList
     * @param sourceColumnCacheList
     * @return
     */
    public List<SourceColumn> compareColumns(List<SourceColumn> sourceColumnList,
                                             List<SourceColumn> sourceColumnCacheList) {
        List<SourceColumn> list3 = new ArrayList<SourceColumn>();
        HashMap<String, SourceColumn> hashold = new HashMap<String, SourceColumn>();
        HashMap<String, SourceColumn> hashnew = new HashMap<String, SourceColumn>();
        for (int i = 0; i < sourceColumnList.size(); i++) {
            hashold.put(sourceColumnList.get(i).getName(),sourceColumnList.get(i));
        }
        for (int j = 0; j < sourceColumnCacheList.size(); j++) {
            // 如果sourceColumnCacheList中的元素不在sourceColumnList中
            if (!hashold.containsKey(sourceColumnCacheList.get(j).getName())) {
                hashnew.put(sourceColumnCacheList.get(j).getName(),sourceColumnCacheList.get(j));
            }
        }
        for (Map.Entry<String, SourceColumn> entry : hashnew.entrySet()) {
            String key = entry.getKey().toString();
            SourceColumn value = entry.getValue();
            System.out.println("key=" + key + " value=" + value);
            list3.add(value);
        }
        return list3;
    }

    /**
     * 建立MongoDB链接
     *
     * @param infoMap
     * @param out
     * @Author akachi.tao
     */
    public Map<String, Object> getMongodbExtracts(Map infoMap,ReturnValue out) {
        logger.info("获取元数据：getMongodbExtracts");
        MongodbExtractSquid mongodbExtractSquid = null;
        NOSQLConnectionSquid nosql = null;
        String tableName = null;
        // 对应的ExtractSquid的ID
        int extractSquidId = 0;
        int sourceTableId = 0;
        int squidType = 0;
        List<SourceColumn> tempList = null;
        SquidLink squidLink = null;
        int squidLinkId = 0;
        String name = null;
        int postProcess = 0;
        Map<String, Object> resultMap = new HashMap<String, Object>();
        // 源列集合
        List<SourceColumn> sourceColumnList = null;
        List<ReferenceColumn> referenceColumnList = new ArrayList<ReferenceColumn>();
        List<Column> columnList = new ArrayList<Column>();
        List<Transformation> transformationList = new ArrayList<Transformation>();
        List<TransformationLink> transformationLinkList = new ArrayList<TransformationLink>();
        try {
            adapter.openSession(); // 在循环外开启事务，所有操作都必须在同一个事务中，成功则提交，失败回滚
            // 如果是MongodbExtractsSquid
            mongodbExtractSquid = JsonUtil.toGsonBean(infoMap
                    .get("MongoDBExtractSquid").toString(),MongodbExtractSquid.class);
            adapter.update2(mongodbExtractSquid);
            ISquidDao squidDao = new SquidDaoImpl(adapter);
            // 设置squid type
            squidType = mongodbExtractSquid.getSquid_type();
            extractSquidId = mongodbExtractSquid.getId();
            sourceTableId = mongodbExtractSquid.getSource_table_id();
            //获取真正的MongoDB集合名称
            Map<String, Object> searchParam = new HashMap<>();
            searchParam.put("id",sourceTableId);

            List<DBSourceTable> currentSourceTables = adapter.query2List2(true,searchParam,DBSourceTable.class);

            if (currentSourceTables != null && currentSourceTables.size() > 0) {
                tableName = currentSourceTables.get(0).getTable_name();
            }

            name = mongodbExtractSquid.getName();
            postProcess = mongodbExtractSquid.getPost_process();
            // 获得quidLink
            if (infoMap.containsKey("SquidLink")) {
                // 得到生成的SquidLink对象
                squidLink = JsonUtil.toGsonBean(
                        infoMap.get("SquidLink").toString(),SquidLink.class);
            }
            // 临时变量
            Map<String, Object> paramMap = new HashMap<>(); // 查询参数
            RepositoryService rs = new RepositoryService();
            rs.setToken(token);
            paramMap.clear();
            paramMap.put("id",Integer.toString(mongodbExtractSquid.getSource_table_id()));
            DBSourceTable sourceTable = adapter.query2Object(true,paramMap,DBSourceTable.class);
            // 文件格式
            // 获取元数据列 mongodbExtract
            paramMap.clear();
            paramMap.put("id",Integer.toString(squidLink.getFrom_squid_id()));
            nosql = squidDao.getSquidForCond(squidLink.getFrom_squid_id(),NOSQLConnectionSquid.class);
            DB db = NoSqlConnectionUtil.createMongoDBConnection(nosql);
            // 表名
            String collectionName = sourceTable.getTable_name();
            List<Map<String, Object>> listMap = NoSqlServiceSub.getDataListByCollection(db,tableName,100);
            Map<String, MongoDbObjectType> columnMap = NoSqlServiceSub.getColumnListByCollection(listMap);
            tempList = NoSqlServiceSub.getCSVList(listMap,columnMap,mongodbExtractSquid);
            // 获取元数据为NULL,那么返回
            if (StringUtils.isNull(tempList) || tempList.isEmpty()) {
                return null;
            }

            paramMap.clear();
            paramMap.put("source_table_id",Integer.toString(sourceTableId));
            List<SourceColumn> sourcColumnTempList = adapter.query2List2(true,paramMap,SourceColumn.class);
            if (StringUtils.isNotNull(sourcColumnTempList) && !sourcColumnTempList.isEmpty()) {
                DataShirServiceplug dataShirServiceplug = new DataShirServiceplug(token);
                dataShirServiceplug.deleteSourceColumnList(adapter,nosql.getId(),extractSquidId,sourceTableId,out);
            }


            if (StringUtils.isNull(tempList) || tempList.isEmpty()) {
                return null;
            }
            // insert到SourceColumn
            sourceColumnList = new ArrayList<>();
            for (int i = 0; i < tempList.size(); i++) {
                tempList.get(i).setId(adapter.insert2(tempList.get(i)));
                sourceColumnList.add(tempList.get(i));
            }



            /* params.clear();
            params.put("ID", squidLinkFromId);
            sourceKey = adapter.query2Object(true, params, SquidModelBase.class)
                    .getKey();*/
            // 创建引用列
            TransformationService transformationService = new TransformationService(token);
            ReferenceColumnGroup columnGroup = new ReferenceGroupService(token).mergeReferenceColumnGroup(adapter,
                    nosql.getId(),extractSquidId);
            // 创建目标列和引用列
            Column column = null;
            paramMap.clear();
            paramMap.put("id",Integer.toString(squidLink.getFrom_squid_id()));
//            SquidModelBase sourceSquid = adapter.query2Object2(true, paramMap, SquidModelBase.class); 获得nosql

            ReferenceColumn referenceColumn = null;
            Transformation transformationLeft = null;
            Transformation transformationRight = null;
            TransformationLink transformationLink = null;
            int transformationFromId = 0;
            int transformationToId = 0;
            int relativeOrder = 0;

            Map<String, Object> nameColumns = new HashMap<String, Object>();

            // 源列和目标列初始相同，生成后用户可以对目标列进行调整
            ITransformationInputsDao transInputsDao = new TransformationInputsDaoImpl(adapter);
            for (int i = 0; i < sourceColumnList.size(); i++) {
                SourceColumn sourceColumn = sourceColumnList.get(i);
                int type = sourceColumn.getData_type();
                sourceColumn.setData_type(SystemDatatype.NVARCHAR.value());
                relativeOrder = sourceColumn.getRelative_order();
                // 目标ExtractSquid引用列（变换面板右边，引用db_source_squid）
                referenceColumn = transformationService.initReference_fileFolder(adapter,sourceColumn,sourceColumn.getId(),relativeOrder + 1,nosql,extractSquidId,columnGroup);
                transformationRight = transformationService.initTransformation(
                        adapter,extractSquidId,referenceColumn.getId(),
                        TransformationTypeEnum.VIRTUAL.value(),
                        referenceColumn.getData_type(),1);

                //同步下游
                RepositoryServiceHelper helper = new RepositoryServiceHelper(token,adapter);
                helper.synchronousInsertRefColumn(adapter,referenceColumn,DMLType.INSERT.value());
                referenceColumnList.add(referenceColumn);
                transformationList.add(transformationRight);
                transformationFromId = transformationRight.getId();

                // 目标ExtractSquid真实列（变换面板左边）
                // 替换掉特殊字符
                sourceColumn.setName(StringUtils.replace(sourceColumn.getName()));
                // fixed bug 980 end by bo.dang
                sourceColumn.setData_type(type);
                column = transformationService.initColumn(adapter,sourceColumn,relativeOrder,extractSquidId,nameColumns);
                transformationLeft = transformationService.initTransformation(
                        adapter,extractSquidId,column.getId(),
                        TransformationTypeEnum.VIRTUAL.value(),
                        column.getData_type(),1);

                //同步下游
                helper.synchronousInsertColumn(adapter,column.getSquid_id(),column,DMLType.INSERT.value(),false);

                columnList.add(column);
                //CommonConsts.addValidationTask(new SquidValidationTask(token, adapter, MessageBubbleService.setMessageBubble(extractSquidId, transformationLeft.getId(), name, MessageBubbleCode.WARN_TRANSFORMATION_NO_LINK.value())));
                transformationList.add(transformationLeft);
                transformationToId = transformationLeft.getId();
                // 创建 transformation link
                transformationLink = transformationService.mergeTransformationLink(adapter,transformationFromId,transformationToId,relativeOrder + 1);
                transformationLinkList.add(transformationLink);
                // 更新TransformationInputs
                transInputsDao.updTransInputs(transformationLink,transformationLeft);
            }
        } catch (Exception e) {
            logger.error("抽取mongo db extract squid异常",e);
            try {
                adapter.rollback();
            } catch (SQLException e1) { // 数据库回滚失败（程序不能处理该异常）！
                logger.fatal("rollback err!",e1);
            }
            out.setMessageCode(MessageCode.SQL_ERROR);
        } finally {
            adapter.closeSession();
        }
        if (squidType == SquidTypeEnum.WEIBOEXTRACT.value() || squidType == SquidTypeEnum.WEBEXTRACT.value()) {
            // 设置newSquidId
            resultMap.put("newSquidId",extractSquidId);
            resultMap.put("newSquidLinkId",squidLinkId);
        } else {
            // 设置newSquidId
            resultMap.put("newSquidId",null);
            resultMap.put("newSquidLinkId",null);
        }

        if (squidType == SquidTypeEnum.WEBLOGEXTRACT.value()) {
            CommonConsts.addValidationTask(new SquidValidationTask(token,adapter,MessageBubbleService.setMessageBubble(extractSquidId,0,null,MessageBubbleCode.ERROR_WEBLOG_EXTRACT_SQUID_NO_SOURCE_DATA.value())));
        }

        // 设置目标列集合ColumnList
        resultMap.put("ColumnList",columnList);
        // 设置源列集合referenceColumnList
        resultMap.put("ReferenceColumnList",referenceColumnList);
        // 设置虚拟转换集合transformationList
        resultMap.put("TransformationList",transformationList);
        // 设置虚拟转换Link集合referenceColumnList
        resultMap.put("TransformationLinkList",transformationLinkList);

        return resultMap;
    }

    /**
     * @param infoMap
     * @param out
     * @return
     * @author bo.dang
     * @date 2014年5月10日
     */
    public Map<String, Object> getSourceDataByExtractSquid(Map<String, Object> infoMap,ReturnValue out) throws Exception {
        logger.info("获取元数据：getSourceDataByExtractSquid");
        DocExtractSquid docExtractSquid = null;
        XmlExtractSquid xmlExtractSquid = null;
        WebLogExtractSquid webLogExtractSquid = null;
        String tableName = null;
        WeiBoExtractSquid weiBoExtractSquid = null;
        WebExtractSquid webExtractSquid = null;
        // 对应的ExtractSquid的ID
        int extractSquidId = 0;
        int sourceTableId = 0;
        int squidType = 0;
        int squidLinkFromId = 0;
        SourceColumn sourceColumn = null;
        List<SourceColumn> tempList = null;
        List<WebLogExtractColumn> webLogExtractColumnList = null;
        SquidLink squidLink = null;
        int squidLinkId = 0;
        String name = null;
        int postProcess = 0;
        Map<String, Object> resultMap = new HashMap<String, Object>();
        List<String> docList = new ArrayList<String>();
        // 源列集合
        List<SourceColumn> sourceColumnList = null;
        List<ReferenceColumn> referenceColumnList = new ArrayList<ReferenceColumn>();
        List<Column> columnList = new ArrayList<Column>(); //DocExtractSquid的数据源
        List<Transformation> transformationList = new ArrayList<Transformation>();
        List<TransformationLink> transformationLinkList = new ArrayList<TransformationLink>();
        try {
            adapter.openSession(); // 在循环外开启事务，所有操作都必须在同一个事务中，成功则提交，失败回滚
            // 如果是DocExtractSquid
            if (infoMap.containsKey("DocExtractSquid")) {
                docExtractSquid = JsonUtil.toGsonBean(infoMap
                        .get("DocExtractSquid").toString(),DocExtractSquid.class);
                adapter.update2(docExtractSquid);
                // 设置squid type
                squidType = docExtractSquid.getSquid_type();
                //docExtractSquid.setSquid_type(SquidTypeEnum.DOC_EXTRACT.value());
                extractSquidId = docExtractSquid.getId();
                sourceTableId = docExtractSquid.getSource_table_id();
                tableName = docExtractSquid.getTable_name();
                name = docExtractSquid.getName();
                postProcess = docExtractSquid.getPost_process();
            }
            // 如果是XmlExtractSquid
            else if (infoMap.containsKey("XmlExtractSquid")) {
                xmlExtractSquid = JsonUtil.toGsonBean(infoMap
                        .get("XmlExtractSquid").toString(),XmlExtractSquid.class);
                adapter.update2(xmlExtractSquid);
                // 设置squid type
                squidType = xmlExtractSquid.getSquid_type();
                //xmlExtractSquid.setSquid_type(SquidTypeEnum.XML_EXTRACT.value());
                extractSquidId = xmlExtractSquid.getId();
                sourceTableId = xmlExtractSquid.getSource_table_id();
                tableName = xmlExtractSquid.getTable_name();
                name = xmlExtractSquid.getName();
                postProcess = xmlExtractSquid.getPost_process();
            }
            // 如果是WebLogExtractSquid
            else if (infoMap.containsKey("WebLogExtractSquid")) {
                webLogExtractSquid = JsonUtil.toGsonBean(
                        infoMap.get("WebLogExtractSquid").toString(),
                        WebLogExtractSquid.class);
                adapter.update2(webLogExtractSquid);
                squidType = webLogExtractSquid.getSquid_type();
                //webLogExtractSquid.setSquid_type(SquidTypeEnum.WEBBLOGEXTRACT.value());
                extractSquidId = webLogExtractSquid.getId();
                sourceTableId = webLogExtractSquid.getSource_table_id();
                tableName = webLogExtractSquid.getTable_name();
                name = webLogExtractSquid.getName();
                postProcess = webLogExtractSquid.getPost_process();
            }
            // 如果是WeiBoExtractSquid
            else if (infoMap.containsKey("WeiBoExtractSquid")) {
                weiBoExtractSquid = JsonUtil.toGsonBean(
                        infoMap.get("WeiBoExtractSquid").toString(),WeiBoExtractSquid.class);
                weiBoExtractSquid.setId(adapter.insert2(weiBoExtractSquid));
                extractSquidId = weiBoExtractSquid.getId();
                squidType = weiBoExtractSquid.getSquid_type();
                sourceTableId = weiBoExtractSquid.getSource_table_id();
                tableName = weiBoExtractSquid.getTable_name();
                name = weiBoExtractSquid.getName();
            }
            // 如果是WebExtractSquid
            else if (infoMap.containsKey("WebExtractSquid")) {
                webExtractSquid = JsonUtil.toGsonBean(
                        infoMap.get("WebExtractSquid").toString(),WebExtractSquid.class);
                webExtractSquid.setId(adapter.insert2(webExtractSquid));
                extractSquidId = webExtractSquid.getId();
                squidType = webExtractSquid.getSquid_type();
                sourceTableId = webExtractSquid.getSource_table_id();
                tableName = webExtractSquid.getTable_name();
                name = webExtractSquid.getName();
            }
            // 如果是WebLogExtractSquid
            if (infoMap.containsKey("WebLogExtractColumnList")) {
                webLogExtractColumnList = JsonUtil.toGsonList(
                        infoMap.get("WebLogExtractColumnList").toString(),WebLogExtractColumn.class);
            }
            // 获得quidLink
            if (infoMap.containsKey("SquidLink")) {
                // 得到生成的SquidLink对象
                squidLink = JsonUtil.toGsonBean(
                        infoMap.get("SquidLink").toString(),SquidLink.class);
                // 获得ID
                squidLinkFromId = squidLink.getFrom_squid_id();
            }
            // 临时变量
            Map<String, String> paramMap = new HashMap<String, String>(); // 查询参数
            String sourceKey; // source squid 的key（推送table是否已抽取）
            CheckExtractService check = new CheckExtractService(token,adapter);
            // 对重复获取元数据的逻辑判断
            if (squidType == SquidTypeEnum.DOC_EXTRACT.value()
                    || squidType == SquidTypeEnum.XML_EXTRACT.value()
                    || squidType == SquidTypeEnum.WEBLOGEXTRACT.value()) {
                // 如果是DocExtractSquid
                if (squidType == SquidTypeEnum.DOC_EXTRACT.value()) {
                    RepositoryService rs = new RepositoryService();
                    rs.setToken(token);
                    paramMap.clear();
                    paramMap.put("id",Integer.toString(docExtractSquid.getSource_table_id(),10));
                    DBSourceTable sourceTable = adapter.query2Object2(true,paramMap,DBSourceTable.class);
                    // 文件格式
                    int doc_format = docExtractSquid.getDoc_format();
                    // 文件名
                    String fileName = sourceTable.getTable_name();
                    // 数据格式
                    ExtractRowFormatEnum row_format = ExtractRowFormatEnum.parse(docExtractSquid.getRow_format());
                    // 分隔符
                    String delimiter = docExtractSquid.getDelimiter();

                    // 得到DocExtractSquid的数据源
                    docList = rs.getDocExtractSourceList(adapter,squidLinkFromId,extractSquidId);

                    if (StringUtils.isNull(docList) || docList.isEmpty()) {
                        return null;
                    } else {
                        //去除特殊字符，乱码问题(主要是针对压缩文件不选择压缩类型时)
                        for(int i=0;i<docList.size();i++){
                            String  str = docList.get(i);
                            str=str.replaceAll("[\b\f]"," ");
                            str = str.replaceAll("\\v",System.getProperty("line.separator"));
                            str = str.replaceAll("[\0\1\2\3\4\5\6\7]","");
                            docList.set(i,str);
                        }
                    }
                    // 得到DocExtractSquid的列集合信息
                    tempList = new DocExtractSquidServiceSub(token).getDocList(docList,docExtractSquid);
                } else if (squidType == SquidTypeEnum.XML_EXTRACT.value()) {
                    RepositoryService rs = new RepositoryService();
                    rs.setToken(token);
                    // 得到XmlExtractSquid的path
                    String xsd_dtd_file = xmlExtractSquid.getXsd_dtd_file();
                    String xsd_dtd_path = xmlExtractSquid.getXsd_dtd_path();
                    String xmlPath = null;
                    Boolean delFlag = false;
                    Map<String, Object> temptMap = rs.getXmlExtractSourcePath(adapter,squidLinkFromId,xsd_dtd_file);
                    if (StringUtils.isNotNull(temptMap)) {
                        xmlPath = temptMap.get("filePath") == null ? null : temptMap.get("filePath").toString();
                        delFlag = (temptMap.get("delFlag") == null ? null : (Boolean) temptMap.get("delFlag"));
                    }
                    if (StringUtils.isNull(xmlPath)) {
                        return null;
                    }
                    List<XMLNodeUtils> xmlNodeList = null;
                    // 如果是xsd文件
                    if (xmlPath.endsWith(".xsd")) {
                        xmlNodeList = ReadXSDUtils.paserXSD(xmlPath,xsd_dtd_path,out);
                    }
                    // 如果是DTD文件
                    else if (xmlPath.endsWith(".dtd")) {
                        xmlNodeList = ReadDTDUtils.parseDTD(xmlPath,xsd_dtd_path,out);
                    }
                    // 删除下载到本地的临时文件
                    if (delFlag) {
                        File tempFile = new File(xmlPath);
                        if (tempFile.exists()) {
                            tempFile.delete();
                        }
                    }

                    if (StringUtils.isNull(xmlNodeList) || xmlNodeList.isEmpty()) {
                        return null;
                    }
                    XMLNodeUtils xsdNode = null;
                    tempList = new ArrayList<SourceColumn>();
                    for (int j = 0; j < xmlNodeList.size(); j++) {
                        xsdNode = xmlNodeList.get(j);
                        sourceColumn = new SourceColumn();
                        sourceColumn.setName(xsdNode.getName());
                        sourceColumn.setData_type(xsdNode.getType());
                        if (xsdNode.getType() == DbBaseDatatype.DECIMAL.value()) {
                            sourceColumn.setPrecision(xsdNode.getLength());
                        } else {
                            sourceColumn.setLength(xsdNode.getLength());
                        }
                        sourceColumn.setSource_table_id(sourceTableId);
                        sourceColumn.setRelative_order(j + 1);
                        sourceColumn.setNullable(true);
                        tempList.add(sourceColumn);
                    }
                    // 获取处理后的数据
//                    sourceColumnList = compareSourceColumnList(tempList, sourceTableId);
                } else if (squidType == SquidTypeEnum.WEBLOGEXTRACT.value()) {
                    // 获取WeblogExtractSquid的元数据
                    WebLogExtractColumn webLogExtractColumn = null;
                    tempList = new ArrayList<SourceColumn>();
                    int count = 0;
                    for (int j = 0; j < webLogExtractColumnList.size(); j++) {
                        webLogExtractColumn = webLogExtractColumnList.get(j);
                        if (StringUtils.isNotNull(webLogExtractColumn) && webLogExtractColumn.isIs_selected()) {
                            sourceColumn = new SourceColumn();
                            sourceColumn.setName(webLogExtractColumn.getName());
                            sourceColumn.setData_type(webLogExtractColumn.getType());
                            if (SystemDatatype.NVARCHAR.value() == webLogExtractColumn.getType()) {
                                sourceColumn.setLength(256);
                            }
                            sourceColumn.setSource_table_id(sourceTableId);
                            sourceColumn.setRelative_order(++count);
                            sourceColumn.setNullable(true);
                            tempList.add(sourceColumn);
                        }
                    }
                    // 获取处理后的数据
                    //sourceColumnList = compareSourceColumnList(tempList, sourceTableId);
                }
                // 获取元数据为NULL,那么返回
                if (StringUtils.isNull(tempList) || tempList.isEmpty()) {
                    return null;
                }

                paramMap.clear();
                paramMap.put("source_table_id",Integer.toString(sourceTableId,10));
                List<SourceColumn> sourcColumnTempList = adapter.query2List(true,paramMap,SourceColumn.class);
                // 如果重复获取元数据，需要删除前一次获取的元数据
                DataShirServiceplug dataShirServiceplug = new DataShirServiceplug(token);
                dataShirServiceplug.deleteSourceColumnList(adapter,squidLinkFromId,extractSquidId,sourceTableId,out);
                // insert到SourceColumn
                sourceColumnList = new ArrayList<SourceColumn>();
                String columnName = null;
                int defaultNameCount = 0;
                int duplicateNameCount = 0;
                Map<String, String> duplicateMap = new HashMap<String, String>();
                SourceColumn[] sourceColumns = tempList.toArray(new SourceColumn[tempList.size()]);
                //List<List<Object>> params = new ArrayList<>();
                for (int i = 0; i < sourceColumns.length; i++) {
                    //List<Object> param = new ArrayList<>();
                    sourceColumn = sourceColumns[i];
                    columnName = sourceColumn.getName();
                    columnName = columnName.replaceAll("\\{","");
                    columnName = columnName.replaceAll("}","");
                    // 判断列名
                    if (StringUtils.isNull(columnName)) {
                        defaultNameCount++;
                        if (1 == defaultNameCount) {
                            columnName = "defaultName";
                        } else {
                            columnName = "defaultName_" + (defaultNameCount - 1);
                        }
                    }
                    // 如果有重复的列名
                    if (StringUtils.isNotNull(duplicateMap) && duplicateMap.containsKey(columnName)) {
                        duplicateNameCount++;
                        columnName = columnName + "_copy_" + duplicateNameCount;
                    }
                    //防止数据库字段内容不区分大小写
                    duplicateMap.put(columnName,columnName);
                    if (columnName.length() > 50) {
                        columnName = columnName.substring(0,10);
                    }
                    /**
                     * 将columanName中出现的\,转换成\\,主要是针对压缩文件获取元数据的情况
                     * 另外一种解决方法是将unicode吗转换成string类型
                     */
                    //去除换行
                    if (columnName.contains("\n")) {
                        columnName.replaceAll("\n","");
                    } else {
                        columnName.replaceAll("\r\n","");
                    }
                    sourceColumn.setName(columnName);
                    //String sql="insert into DS_SOURCE_COLUMN (SOURCE_TABLE_ID,NAME,DATA_TYPE,NULLABLE,LENGTH,PRECISION,SCALE,RELATIVE_ORDER,ISUNIQUE,ISPK,COLLATION) VALUES(?,?,?,?,?,?,?,?,?,?,?)";
                    //可以考虑使用batch来提高效率
                    sourceColumn.setId(adapter.insert2(sourceColumn));
                    sourceColumnList.add(sourceColumn);
                   /* param.add(sourceColumn.getSource_table_id());
                    param.add(sourceColumn.getName());
                    param.add(sourceColumn.getData_type());
                    param.add(sourceColumn.isNullable());
                    param.add(sourceColumn.getLength());
                    param.add(sourceColumn.getPrecision());
                    param.add(sourceColumn.getScale());
                    param.add(sourceColumn.getRelative_order());
                    param.add(sourceColumn.isIsunique());
                    param.add(sourceColumn.isIspk());
                    //adapter.executeBatch()
                    params.add(param);
                    //批量处理
                    String sql="insert into DS_SOURCE_COLUMN (SOURCE_TABLE_ID,NAME,DATA_TYPE,NULLABLE,LENGTH,PRECISION,SCALE,RELATIVE_ORDER,ISUNIQUE,ISPK) VALUES(?,?,?,?,?,?,?,?,?,?)";
                    int n = adapter.executeBatch(sql,params);
                    if(n>0){
                        params.clear();
                    }*/
                }

                //置为null，释放内存
                sourceColumns = null;
                //用来去源列的换行
               /* for(int i=0;i<sourceColumnList.size();i++){
                    if(sourceColumnList.get(i).getName().contains("\n")){
                        sourceColumnList.get(i).setName( sourceColumnList.get(i).getName().replaceAll("\n",""));
                    }else{
                        sourceColumnList.get(i).setName( sourceColumnList.get(i).getName().replaceAll("\r\n",""));
                    }
                }*/

            }
            // 如果是WeiBoExtractSquid或者WebExtractSquid
            else if (squidType == SquidTypeEnum.WEIBOEXTRACT.value() || squidType == SquidTypeEnum.WEBEXTRACT.value()) {
                paramMap.clear();
                paramMap.put("source_table_id",Integer.toString(sourceTableId,10));
                sourceColumnList = adapter.query2List(true,paramMap,SourceColumn.class);
                squidLink.setTo_squid_id(extractSquidId);
                // 插入squidLink
                squidLink.setId(adapter.insert2(squidLink));
                squidLinkId = squidLink.getId();

                //修改data store
                String sql = "update ds_source_table set is_extracted = 'Y' where id = " + sourceTableId;
                adapter.execute(sql);
            }

            if (StringUtils.isNull(sourceColumnList) || sourceColumnList.isEmpty()) {
                return null;
            }
            // 创建引用列
            TransformationService transformationService = new TransformationService(token);
            ReferenceColumnGroup columnGroup = new ReferenceGroupService(token).mergeReferenceColumnGroup(adapter,squidLinkFromId,extractSquidId);
            // 创建目标列和引用列
            Column column = null;
            paramMap.clear();
            paramMap.put("id",Integer.toString(squidLinkFromId,10));
            Squid sourceSquid = adapter.query2Object2(true,paramMap,Squid.class);

            ReferenceColumn referenceColumn = null;
            Transformation transformationLeft = null;
            Transformation transformationRight = null;
            TransformationLink transformationLink = null;
            int transformationFromId = 0;
            int transformationToId = 0;
            int relativeOrder = 0;

            Map<String, Object> nameColumns = new HashMap<String, Object>();

            // 源列和目标列初始相同，生成后用户可以对目标列进行调整
            ITransformationInputsDao transInputsDao = new TransformationInputsDaoImpl(adapter);
            Map<String, String> duplicateMap = new HashMap<String, String>();
            int duplicateNameCount = 0;
            String columnName = null;
            //转换成数组
            SourceColumn[] sourceColumns = sourceColumnList.toArray(new SourceColumn[sourceColumnList.size()]);
            for (int i = 0; i < sourceColumns.length; i++) {
                sourceColumn = sourceColumns[i];
                relativeOrder = sourceColumn.getRelative_order();
                // 目标ExtractSquid引用列（变换面板右边，引用db_source_squid）
                referenceColumn = transformationService.initReference_fileFolder(adapter,sourceColumn,sourceColumn.getId(),relativeOrder ,sourceSquid,extractSquidId,columnGroup);
                // 根据添加过的column判断后面的referenceColumn中是否有重复的名字
                if (StringUtils.isNotNull(duplicateMap) && duplicateMap.containsKey(referenceColumn.getName())) {
                    duplicateNameCount++;
                    columnName = referenceColumn.getName() + "_copy_" + duplicateNameCount;
                    referenceColumn.setName(columnName);
                }
                transformationRight = transformationService.initTransformation(
                        adapter,extractSquidId,referenceColumn.getId(),
                        TransformationTypeEnum.VIRTUAL.value(),
                        referenceColumn.getData_type(),1);

                //同步下游
                RepositoryServiceHelper helper = new RepositoryServiceHelper(token,adapter);
                helper.synchronousInsertRefColumn(adapter,referenceColumn,DMLType.INSERT.value());

                referenceColumnList.add(referenceColumn);
                transformationList.add(transformationRight);
                transformationFromId = transformationRight.getId();

                // 目标ExtractSquid真实列（变换面板左边）
                // 替换掉特殊字符     referenceColumn.getName()将更新后名字放入
                sourceColumn.setName(StringUtils.replace(referenceColumn.getName()));
                // fixed bug 980 end by bo.dang
                column = transformationService.initColumn(adapter,sourceColumn,relativeOrder,extractSquidId,nameColumns);
                columnList.add(column);
                columnName = column.getName();
                duplicateMap.put(columnName,columnName);
                //同步下游
                helper.synchronousInsertColumn(adapter,column.getSquid_id(),column,DMLType.INSERT.value(),false);

                if (StringUtils.isHavaChinese(column.getName())) {
                    adapter.execute("delete from DS_COLUMN  where id=" + column.getId() + " and squid_id=" + column.getSquid_id()/* + " and relative_order=" + column.getRelative_order()*/);
                    int colId = column.getSquid_id();

                    column.setName(this.getColumnName(adapter,column.getName(),colId));

                    // column.setName("col_" + no++);
                    column.setId(adapter.insert2(column));
                    transformationLeft = transformationService.initTransformation(
                            adapter,extractSquidId,column.getId(),
                            TransformationTypeEnum.VIRTUAL.value(),
                            column.getData_type(),1);
                } else {
                    transformationLeft = transformationService.initTransformation(
                            adapter,extractSquidId,column.getId(),
                            TransformationTypeEnum.VIRTUAL.value(),
                            column.getData_type(),1);
                }

                transformationList.add(transformationLeft);
                transformationToId = transformationLeft.getId();
                // 创建 transformation link
                transformationLink = transformationService.mergeTransformationLink(adapter,transformationFromId,transformationToId,relativeOrder + 1);
                transformationLinkList.add(transformationLink);
                // 更新TransformationInputs
                transInputsDao.updTransInputs(transformationLink,transformationLeft);
            }
            // 创建transformation目标、引用和tranformationInputs

        } catch (Exception e) {
            logger.error("获取extract squid元数据异常",e);
            out.setMessageCode(MessageCode.ERR_EXTRACT_SQUID_NO_DATA);
            try {
                adapter.rollback();
            } catch (SQLException e1) { // 数据库回滚失败（程序不能处理该异常）！
                logger.fatal("rollback err!",e1);
            }

            throw e;
        } finally {
            adapter.closeSession();
            if ((squidType == SquidTypeEnum.DOC_EXTRACT.value()) && (StringUtils.isNull(docList) || docList.isEmpty()))
                return null;
        }
        if (squidType == SquidTypeEnum.WEIBOEXTRACT.value() || squidType == SquidTypeEnum.WEBEXTRACT.value()) {
            // 设置newSquidId
            resultMap.put("newSquidId",extractSquidId);
            resultMap.put("newSquidLinkId",squidLinkId);
        } else {
            // 设置newSquidId
            resultMap.put("newSquidId",null);
            resultMap.put("newSquidLinkId",null);
        }

        if (squidType == SquidTypeEnum.WEBLOGEXTRACT.value()) {
            CommonConsts.addValidationTask(new SquidValidationTask(token,adapter,MessageBubbleService.setMessageBubble(extractSquidId,0,null,MessageBubbleCode.ERROR_WEBLOG_EXTRACT_SQUID_NO_SOURCE_DATA.value())));
        }

        Collections.sort(columnList,new Comparator<Column>() {
            @Override
            public int compare(Column o1,Column o2) {
                return Integer.compare(o1.getRelative_order(),o2.getRelative_order());
            }
        });
        // 设置目标列集合ColumnList
        resultMap.put("ColumnList",columnList);
        // 设置源列集合referenceColumnList
        resultMap.put("ReferenceColumnList",referenceColumnList);
        // 设置虚拟转换集合transformationList
        resultMap.put("TransformationList",transformationList);
        // 设置虚拟转换Link集合referenceColumnList
        resultMap.put("TransformationLinkList",transformationLinkList);
        return resultMap;
    }

    /**
     * 设置重复名称后自动创建序号
     *
     * @param adapter3
     * @param columnName
     * @param squidFlowId
     * @return
     */
    private synchronized String getColumnName(IRelationalDataManager adapter3,
                                              String columnName,int squidFlowId) {
        Map<String, Object> params = new HashMap<String, Object>();
        int cnt = 1;
        String tempName = columnName;
        while (true) {
            params.put("name",tempName);
            params.put("Squid_id",squidFlowId);
            Column column = adapter3.query2Object(true,params,Column.class);
            if (column == null) {
                return tempName;
            } else {
                int no = Integer.parseInt(columnName.substring(columnName.indexOf("_") + 1));
                int uo = no + 1;
                tempName = columnName.substring(0,columnName.indexOf("_") + uo);
                cnt++;
            }
        }
    }
}