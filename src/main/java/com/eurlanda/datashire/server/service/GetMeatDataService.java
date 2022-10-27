package com.eurlanda.datashire.server.service;

import com.eurlanda.datashire.entity.*;
import com.eurlanda.datashire.entity.TransformationInputs;
import com.eurlanda.datashire.entity.dest.*;
import com.eurlanda.datashire.server.model.DestImpalaColumn;
import com.eurlanda.datashire.server.model.DestHDFSColumn;
import com.eurlanda.datashire.enumeration.SquidTypeEnum;
import com.eurlanda.datashire.enumeration.TransformationTypeEnum;
import com.eurlanda.datashire.enumeration.datatype.SystemDatatype;
import com.eurlanda.datashire.server.dao.*;
import com.eurlanda.datashire.server.exception.ErrorMessageException;
import com.eurlanda.datashire.server.model.Base.SquidModelBase;
import com.eurlanda.datashire.server.model.*;
import com.eurlanda.datashire.server.model.Column;
import com.eurlanda.datashire.server.model.EsColumn;
import com.eurlanda.datashire.server.model.ReferenceColumn;
import com.eurlanda.datashire.server.model.ReferenceColumnGroup;
import com.eurlanda.datashire.server.model.Transformation;
import com.eurlanda.datashire.server.model.TransformationLink;
import com.eurlanda.datashire.server.utils.utility.DataTypeDetecation;
import com.eurlanda.datashire.utility.MessageCode;
import com.eurlanda.datashire.utility.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by eurlanda - new 2 on 2017/8/1.
 */
@Service
public class GetMeatDataService {
    private static final Logger logger = LoggerFactory.getLogger(GetMeatDataService.class);
    @Autowired
    private ReferenceGroupService referenceGroupService;
    @Autowired
    private SquidDao squidDao;
    @Autowired
    private ReferenceColumnDao referenceColumnDao;
    @Autowired
    private TransformationDao transformationDao;
    @Autowired
    private TransformationInputsService transformationInputsService;
    @Autowired
    private TransformationInputsDao transformationInputsDao;
    @Autowired
    private ColumnDao columnDao;
    @Autowired
    private SquidLinkDao squidLinkDao;
    @Autowired
    private ReferenceColumnGroupDao referenceColumnGroupDao;
    @Autowired
    private TransformationLinkDao transformationLinkDao;
    @Autowired
    private SourceColumnDao sourceColumnDao;
    @Autowired
    private ColumnService columnService;
    @Autowired
    private EsColumnDao esColumnDao;
    @Autowired
    private DestHDFSColumnDao destHDFSColumnDao;
    @Autowired
    private DestImpalaColumnDao destImpalaColumnDao;


    /**
     * 根据Column对象生成Trans，TransLink，Column与ReferenceColumn等对象
     *
     * @param
     * @param
     * @return
     */
    @Transactional(rollbackFor = {Exception.class})
    public Map<String, Object> addColumn(List<SourceColumn> sourceColumnList, int squidFromId, int extractSquidId) throws Exception {
        Map<String, Object> resultMap = new HashMap<String, Object>();
        Map<String, Integer> map = new HashMap<>();
        SourceColumn sourceColumn = null;
        boolean falg = true;
        // 源列集合
        List<ReferenceColumn> referenceColumnList = new ArrayList<ReferenceColumn>();
        //下游的referenceColumn
        List<ReferenceColumn> referenceColumns = new ArrayList<ReferenceColumn>();
        List<Column> columnList = new ArrayList<Column>(); //DocExtractSquid的数据源
        //返回的Transformation
        List<Transformation> transformationList = new ArrayList<Transformation>();
        //右边的Transformation
        List<Transformation> transformationRightList = new ArrayList<Transformation>();
        //左边的Transformation
        List<Transformation> transformationLeftList = new ArrayList<Transformation>();
        //下游的Transformation
        List<Transformation> transformations = new ArrayList<Transformation>();
        List<TransformationLink> transformationLinkList = new ArrayList<TransformationLink>();
        try {
            if (StringUtils.isNull(sourceColumnList) || sourceColumnList.isEmpty()) {
                return null;
            } else {

                // 创建目标列和引用列
                Column column = null;
                map.clear();
                map.put("id", squidFromId);
                SquidModelBase sourceSquid = squidDao.selectByPrimaryKey(squidFromId);

                ReferenceColumn referenceColumn = null;
                Transformation transformationLeft = null;
                Transformation transformationRight = null;
                TransformationLink transformationLink = null;
                int relativeOrder = 0;

                Map<String, Object> nameColumns = new HashMap<String, Object>();

                // 源列和目标列初始相同，生成后用户可以对目标列进行调整
                Map<String, String> duplicateMap = new HashMap<String, String>();
                int duplicateNameCount = 0;
                String columnName = null;

                //重新获取元数据要删掉以前的
                List<Integer> ids = columnDao.selectColumnBySquidId(extractSquidId);
                Map<String, List<Integer>> listMap = null;
                if (ids != null && ids.size() > 0) {
                    listMap = columnService.deleteColumn(ids, 1);
                }
                ReferenceColumnGroup columnGroup = referenceGroupService.mergeReferenceColumnGroup(squidFromId, extractSquidId);
                //转换成数组
                SourceColumn[] sourceColumns = sourceColumnList.toArray(new SourceColumn[sourceColumnList.size()]);

                for (int i = 0; i < sourceColumns.length; i++) {
                    sourceColumn = sourceColumns[i];
                    relativeOrder = sourceColumn.getRelative_order();
                    // 目标ExtractSquid引用列（变换面板右边，引用db_source_squid）
                    referenceColumn = this.initReference_fileFolder(sourceColumn, sourceColumn.getId(), relativeOrder, sourceSquid, extractSquidId, columnGroup);
                    referenceColumnList.add(referenceColumn);


                    // 目标ExtractSquid真实列（变换面板左边）
                    // 替换掉特殊字符     referenceColumn.getName()将更新后名字放入
                    sourceColumn.setName(StringUtils.replace(referenceColumn.getName()));
                    // fixed bug 980 end by bo.dang
                    try {
                        column = this.initColumn(sourceColumn, relativeOrder, extractSquidId, nameColumns);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    columnList.add(column);
                    columnName = column.getName();
//                    duplicateMap.put(columnName, columnName);
                }
                //批量添加referenceColumn
                referenceColumnDao.insert(referenceColumnList);
                for (ReferenceColumn referenceColumn1 : referenceColumnList) {
//                    // 根据添加过的column判断后面的referenceColumn中是否有重复的名字
//                    if (StringUtils.isNotNull(duplicateMap) && duplicateMap.containsKey(referenceColumn1.getName())) {
//                        duplicateNameCount++;
//                        columnName = referenceColumn1.getName() + "_copy_" + duplicateNameCount;
//                        referenceColumn1.setName(columnName);
//                    }
                    // 创建Transformation
                    transformationRight = this.initTransformation(extractSquidId, referenceColumn1.getId(), TransformationTypeEnum.VIRTUAL.value(),
                            referenceColumn.getData_type(), referenceColumn1.getRelative_order());
                    transformationRightList.add(transformationRight);
                }
                //批量添加右边的Transformation
                transformationDao.insert(transformationRightList);
                //批量添加Column
                columnDao.insert(columnList);
                for (Column columnn : columnList) {
                    transformationLeft = this.initTransformation(extractSquidId, columnn.getId(), TransformationTypeEnum.VIRTUAL.value(),
                            columnn.getData_type(), columnn.getRelative_order());
                    transformationLeftList.add(transformationLeft);
                }
                //批量添加左边的Transformation
                transformationDao.insert(transformationLeftList);
                int j = 0;
                for (j = 0; j < transformationRightList.size(); j++) {
                    transformationLink = this.mergeTransformationLink(transformationRightList.get(j).getId(), transformationLeftList.get(j).getId(), relativeOrder + 1);
                    transformationLinkList.add(transformationLink);
                }
                //批量添加transformationLink
                transformationLinkDao.insert(transformationLinkList);
                //根据extractSquidId，查询下游的Squid
                //同步下游
                List<Map<String, Object>> squidMaps = squidDao.findNextSquidByFromSquid(extractSquidId);
                if(squidMaps!=null && squidMaps.size()>0){
                    for(Map<String, Object> squidMap : squidMaps){
                        int toSquidId = Integer.parseInt(squidMap.get("id") + "");
                        int squidType = Integer.parseInt(squidMap.get("squid_type_id") + "");
                        synchronousInsertColumn(extractSquidId,toSquidId,squidType,transformationList,columnList,referenceColumnList);
                    }

                }


                //所有的transformation生成 input
                transformationList.addAll(transformationRightList);
                transformationList.addAll(transformationLeftList);
                transformationList.addAll(transformations);
                for (Transformation transformation : transformationList) {
                    try {
                        transformation.setInputs(transformationInputsService.initTransInputs(transformation, transformation.getOutput_data_type(), -1, "", -1, 0));
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                //更新trans_inputs
                j = 0;
                for (TransformationLink transformationLink1 : transformationLinkList) {
                    transformationInputsService.updTransInputs(transformationLink1, transformationLeftList.get(j));
                    j++;
                }
                //remove掉下游的transformations
                transformationList.removeAll(transformations);
            }
        } catch (Exception e) {
            logger.error("获取extract squid元数据异常", e);
            e.printStackTrace();
            throw new ErrorMessageException(MessageCode.DESERIALIZATION_FAILED.value());
        }
        // 设置目标列集合ColumnList
        resultMap.put("ColumnList", columnList);
        // 设置源列集合referenceColumnList
        resultMap.put("ReferenceColumnList", referenceColumnList);
        // 设置虚拟转换集合transformationList
        resultMap.put("TransformationList", transformationList);
        // 设置虚拟转换Link集合referenceColumnList
        resultMap.put("TransformationLinkList", transformationLinkList);
        return resultMap;
    }

    /**
     * 通过column 生成impalaColumn
     *
     * @param column  impala squid上游squid的column
     * @param squidId impala_squid_id
     * @return impalaColumn
     */
    public static DestImpalaColumn getImpalaColumnByColumn(Column column, int squidId) {
        DestImpalaColumn impalaColumn = new DestImpalaColumn();
        impalaColumn.setColumn_id(column.getId());
        impalaColumn.setSquid_id(squidId);
        impalaColumn.setIs_dest_column(1);
        impalaColumn.setField_name(column.getName());
        impalaColumn.setColumn_order(column.getRelative_order());
        return impalaColumn;
    }

    /**
     * 通过column 生成HDFSColumn落地
     *
     * @param column  HDFS squid上游squid的column
     * @param squidId HDFS squid id
     * @return
     */
    public static DestHDFSColumn getHDFSColumnByColumn(Column column, int squidId) {
        DestHDFSColumn HdfsColumn = new DestHDFSColumn();
        HdfsColumn.setColumn_id(column.getId());
        HdfsColumn.setSquid_id(squidId);
        HdfsColumn.setField_name(column.getName());
        HdfsColumn.setIs_dest_column(1);
        HdfsColumn.setColumn_order(column.getRelative_order());
        HdfsColumn.setIs_partition_column(0);
        return HdfsColumn;
    }

    /**
     * 通过column 生成EsColumn
     *
     * @param column  es squid上游squid的column
     * @param squidId es squid id
     * @return
     */
    public static EsColumn genEsColumnByColumn(Column column, int squidId) {
        EsColumn esColumn = new EsColumn();
        esColumn.setColumn_id(column.getId());
        esColumn.setField_name(column.getName());
        esColumn.setIs_persist(1);
        esColumn.setIs_mapping_id(0);
        esColumn.setSquid_id(squidId);
        return esColumn;
    }


    /**
     * 创建ReferenceColumn集合
     *
     * @param referenceColumn 表字段信息
     * @param newColumnId     表字段主键（单独出来，有可能是Column 或者 复制ReferenceColumn）
     * @param order           排序默认值
     * @param formSquid
     * @param toSquidId       squidId（当前为Exception）
     * @param columnGroup
     * @return
     * @throws SQLException
     */
    public ReferenceColumn initReference_fileFolder(SourceColumn referenceColumn,
                                                    int newColumnId, int order, SquidModelBase formSquid,
                                                    int toSquidId, ReferenceColumnGroup columnGroup) {
        // 创建引用列
        ReferenceColumn ref = new ReferenceColumn();
        ref.setColumn_id(newColumnId);
        ref.setId(ref.getColumn_id());
        ref.setCollation(0);
        //全部替换为nvarchar（max)
        ref.setData_type(SystemDatatype.NVARCHAR.value());
        ref.setLength(-1);
        ref.setPrecision(0);
        ref.setScale(0);
        ref.setName(StringUtils.replaceSource(referenceColumn.getName()));
        ref.setNullable(referenceColumn.isNullable());
        ref.setRelative_order(order);
        ref.setSquid_id(formSquid.getId());
        ref.setReference_squid_id(toSquidId);
        ref.setHost_squid_id(formSquid.getId());
        ref.setIsPK(referenceColumn.isIspk());
        ref.setIsUnique(referenceColumn.isIsunique());
        ref.setIs_referenced(true);
        ref.setGroup_id(columnGroup.getId());
        ref.setGroup_name(formSquid.getName());
        // 添加新创建的ReferenceColumn信息
        return ref;
    }

    public static ReferenceColumn initReference(Column referenceColumn, int newColumnId, int formSquid,
                                                int toSquidId, ReferenceColumnGroup columnGroup) throws SQLException {
        // 创建引用列
        ReferenceColumn ref = new ReferenceColumn();
        ref.setColumn_id(newColumnId);
        ref.setId(ref.getColumn_id());
        ref.setCollation(0);
        ref.setData_type(referenceColumn.getData_type());
        ref.setName(referenceColumn.getName());
        ref.setNullable(referenceColumn.isNullable());
        ref.setLength(referenceColumn.getLength());
        ref.setPrecision(referenceColumn.getPrecision());
        ref.setScale(referenceColumn.getScale());
        ref.setRelative_order(referenceColumn.getRelative_order());
        ref.setSquid_id(formSquid);
        ref.setReference_squid_id(toSquidId);
        ref.setHost_squid_id(formSquid);
        ref.setIsPK(referenceColumn.isPK());
        ref.setIsUnique(referenceColumn.isUnique());
        ref.setIs_referenced(true);
        ref.setGroup_id(columnGroup.getId());
        return ref;
    }


    public static ReferenceColumn initReference(ReferenceColumn referenceColumn, int newColumnId, int formSquid,
                                                int toSquidId,ReferenceColumnGroup columnGroup) throws SQLException {
        // 创建引用列
        ReferenceColumn ref = new ReferenceColumn();
        ref.setColumn_id(newColumnId);
        ref.setId(ref.getColumn_id());
        ref.setCollation(0);
        ref.setData_type(referenceColumn.getData_type());
        ref.setName(referenceColumn.getName());
        ref.setNullable(referenceColumn.getNullable());
        ref.setLength(referenceColumn.getLength());
        ref.setPrecision(referenceColumn.getPrecision());
        ref.setScale(referenceColumn.getScale());
        ref.setRelative_order(referenceColumn.getRelative_order());
        ref.setSquid_id(formSquid);
        ref.setReference_squid_id(toSquidId);
        ref.setHost_squid_id(formSquid);
        ref.setIsPK(referenceColumn.getIsPK());
        ref.setIsUnique(referenceColumn.getIsUnique());
        ref.setIs_referenced(true);
        ref.setGroup_id(columnGroup.getId());
        return ref;
    }


    /**
     * 新建Transform（带坐标）
     *
     * @param
     * @param newSquidId       transformation所属squid id
     * @param transTypeId      tran类型
     * @param column_id        如果tran为virt,则需要指明column_id
     * @param column_data_Type transformation输出类型
     * @param output_number    transformation输出数量
     * @return
     * @throws Exception
     */
    public Transformation initTransformation(int newSquidId, int column_id, int transTypeId,
                                             int column_data_Type, int output_number) throws Exception {
        // 创建新增列对应的虚拟变换
        Transformation newTrans = new Transformation();
        if (transTypeId == TransformationTypeEnum.VIRTUAL.value()) {
            newTrans.setColumn_id(column_id);
        }
        newTrans.setSquid_id(newSquidId);
        newTrans.setTranstype(transTypeId);
        newTrans.setOutput_data_type(column_data_Type);
        newTrans.setOutput_number(output_number);

        return newTrans;
    }

    /**
     * 新增Column对象
     *
     * @param order
     * @param toSquidId
     * @return
     * @throws SQLException
     * @author bo.dang
     */
    public Column initColumn(SourceColumn sourceColumn, int order,
                             int toSquidId, Map<String, Object> nameColumn) throws SQLException {
        // 目标ExtractSquid真实列（变换面板左边）
        String newName = null;
        if (StringUtils.isNotNull(nameColumn)) {
            newName = getColumnName(nameColumn, sourceColumn.getName());
        } else {
            newName = sourceColumn.getName();

        }
        //防止替换特殊字符后，出现name为null的情况
        if (StringUtils.isNull(newName)) {
            newName = "defaultName";
            //判断是否有重复名字
            newName = getColumnName(nameColumn, newName);
        }
        Column newColumn = new Column();
        newColumn.setCollation(0);
        //全部转化成NVARCHAR
        newColumn.setData_type(sourceColumn.getData_type());
        newColumn.setLength(sourceColumn.getLength());
        newColumn.setPrecision(sourceColumn.getPrecision());
        newColumn.setScale(sourceColumn.getScale());
        newColumn.setKey(StringUtils.generateGUID());
        newColumn.setName(newName);
        newColumn.setNullable(sourceColumn.isNullable());
        newColumn.setPK(sourceColumn.isIspk());
        if (sourceColumn.isIspk()) {
            newColumn.setNullable(false);
        }
        newColumn.setUnique(sourceColumn.isIsunique());
        newColumn.setRelative_order(order);
        newColumn.setSquid_id(toSquidId);
        // fixed bug 905 start by bo.dang
        newColumn.setIs_groupby(0);
        newColumn.setAggregation_type(-1);

        //目标列去换行
        if (StringUtils.isNotNull(newColumn.getName())) {
            if (newColumn.getName().contains("\n")) {
                newColumn.setName(newColumn.getName().replaceAll("\n", ""));
            } else {
                newColumn.setName(newColumn.getName().replaceAll("\r\n", ""));
            }
            //目标列中文转英文
            if (StringUtils.isHavaChinese(newColumn.getName())) {
                newColumn.setName("col_" + order);
                //判断名字是否重复
                newColumn.setName(getColumnName(nameColumn, newColumn.getName()));
            }
        }
        // fixed bug 905 end by bo.dang
        return newColumn;
    }




    public Column initColumn2(ReferenceColumn columnInfo, int order,
                              int toSquidId, Map<Integer, Object> nameColumn) throws SQLException {
        // 目标ExtractSquid真实列（变换面板左边）
        String newName = null;
        if (StringUtils.isNotNull(nameColumn)) {
            if( nameColumn.get(columnInfo.getHost_squid_id())==null){
                newName="_" + columnInfo.getName();
            }else {
                newName = nameColumn.get(columnInfo.getHost_squid_id()) + "_" + columnInfo.getName();
            }
        } else {
            newName = columnInfo.getName();
        }
        Column newColumn = new Column();
        newColumn.setCollation(0);
        //newColumn.setData_type(DataTypeManager.decode(columnInfo.getData_type()));
        newColumn.setData_type(columnInfo.getData_type());
        newColumn.setLength(columnInfo.getLength());
        newColumn.setPrecision(columnInfo.getPrecision());
        newColumn.setScale(columnInfo.getScale());
        newColumn.setKey(StringUtils.generateGUID());
        if (StringUtils.isHavaChinese(newName)) {
            newName = "col_" + order;
        }
        newColumn.setName(newName);
        newColumn.setNullable(columnInfo.getNullable());
        newColumn.setPK(columnInfo.getIsPK());
        newColumn.setUnique(columnInfo.getIsUnique());
        newColumn.setIs_Business_Key(columnInfo.getIs_Business_Key());
        newColumn.setRelative_order(order);
        newColumn.setAggregation_type(columnInfo.getAggregation_type());
        newColumn.setCdc(columnInfo.getCdc());
        newColumn.setIs_groupby(columnInfo.getIs_groupby());
        newColumn.setDescription(columnInfo.getDescription());
        newColumn.setSquid_id(toSquidId);
        return newColumn;
    }




    /**
     * 设置重复名称后自动创建序号
     *
     * @param nameColumn
     * @param ColumnName
     * @return
     */
    public String getColumnName(Map<String, Object> nameColumn, String ColumnName) {
        String tempName = ColumnName;
        int cnt = 0;
        while (true) {
            if (nameColumn.containsKey(tempName.toLowerCase())) {
                tempName = ColumnName + cnt;
            } else {
                nameColumn.put(tempName.toLowerCase(), tempName);
                return tempName;
            }
            cnt++;
        }
    }

    /**
     * 列的定义
     *
     * @return
     */
    @Transactional(rollbackFor = {Exception.class})
    public List<SourceColumn> conversionColumn(List<List<String>> lists, int sourceTableId) {
        Map<String, Object> paramMap=new HashMap<>();
        DataTypeDetecation dataTypeDetecation = new DataTypeDetecation();
        List<SourceColumn> sourceColumns = new ArrayList<>();
        SystemDatatype type = null;
        int m = 0;
        int nameIndex = 1;
        try {
            if (lists.size() > 0) {
                //拿出lists集合中最长的元素
                if (lists.size() > 1) {
                    //拿出lists集合中最长的元素
                    for (int i = 0; i < lists.size(); i++) {
                        if (lists.get(i).size() > lists.get(nameIndex).size()) {
                            nameIndex = i;
                        }
                    }
                } else {
                    nameIndex = 0;
                }
                //获取列名
                List<String> columnsNames = dataTypeDetecation.getColumnNames(lists.get(0), lists.get(nameIndex).size());
                //去除标题行
                lists.remove(lists.get(0));
                //行转成列
                List<List<String>> stringLists = new ArrayList<>();
                for (int i = 0; i < columnsNames.size(); i++) {
                    List<String> stringList = new ArrayList<>();
                    for (List<String> list : lists) {
                        if (list != null && list.size() > 0 && list.size() >= (i + 1)) {
                            stringList.add(list.get(i));
                        }
                    }
                    stringLists.add(stringList);
                }
                //生成sourceColumn
                for (List<String> list : stringLists) {
                    SourceColumn sourceColumn = new SourceColumn();
                    type = dataTypeDetecation.checkColumnType(list);
                    sourceColumn.setData_type(type.value());
                    sourceColumn.setLength(-1);
                    sourceColumn.setSource_table_id(sourceTableId);
                    sourceColumn.setNullable(true);
                    sourceColumn.setName(columnsNames.get(m));
                    sourceColumn.setRelative_order(++m);
                    sourceColumns.add(sourceColumn);
                }
                //重新获取时删掉以前的sourceColumn
                sourceColumnDao.deleteSourceColumnByTableId(sourceTableId);
                sourceColumnDao.insertSourceColumn(sourceColumns);
            }
        } catch (Exception e) {
            logger.error("--生成sourceColumn异常--", e);
            e.printStackTrace();
        }
        return sourceColumns;
    }


    /**
     * 创建TransformationLink
     *
     * @param transformationFromId
     * @param transformationToId
     * @param order
     * @return
     * @throws Exception
     * @author bo.dang
     */
    public TransformationLink mergeTransformationLink(int transformationFromId, int transformationToId, int order) throws Exception {
        Map<String, String> paramsMap = new HashMap<String, String>();
        paramsMap.put("from_transformation_id", Integer.toString(transformationFromId, 10));
        paramsMap.put("to_transformation_id", Integer.toString(transformationToId, 10));
        TransformationLink transformationLink = transformationLinkDao.selectByTransformationId(paramsMap);
        if (StringUtils.isNotNull(transformationLink)) {
            transformationLinkDao.updateByPrimaryKey(transformationLink);
        } else {
            transformationLink = new TransformationLink();
            transformationLink.setIn_order(order);
            transformationLink.setFrom_transformation_id(transformationFromId);
            transformationLink.setTo_transformation_id(transformationToId);
            transformationLink.setKey(StringUtils.generateGUID());
        }
        return transformationLink;
    }

    public void synchronousInsertColumn(int fromSquid,int toSquidId,int squidType,List<Transformation> transformationList,List<Column> columnList,List<ReferenceColumn> referenceColumnList){
      try{
        //同步下游
                Map<Integer, Object> nameColumns = new HashMap<Integer, Object>();
                List<ReferenceColumn> referenceColumns=new ArrayList<ReferenceColumn>();
                List<Transformation> transformations=new ArrayList<Transformation>();
                List<Column> exceptionColumnList=new ArrayList<Column>();
                List<com.eurlanda.datashire.server.model.TransformationInputs> transformationInputsLists=new ArrayList<>();
                Map<String,Integer> map=new HashMap<String,Integer>();





          if ((!SquidTypeEnum.isDestSquid(squidType) && SquidTypeEnum.EXCEPTION.value() != squidType && SquidTypeEnum.GROUPTAGGING.value() != squidType
                && SquidTypeEnum.SAMPLINGSQUID.value() != squidType && SquidTypeEnum.PIVOTSQUID.value() != squidType)) {
                    referenceColumns.clear();
                    transformations.clear();
                    exceptionColumnList.clear();
                    map.put("fromSquid", fromSquid);
                    map.put("toSquidId", toSquidId);
                    ReferenceColumnGroup group = referenceColumnGroupDao.selectGroupBySquidId(map);
                    for (Column column1 : columnList) {
                        if (group == null) {
                            //添加ReferenceColumnGroup记录
                            int order = 1;
                            List<ReferenceColumnGroup> groupList = referenceColumnGroupDao.getRefColumnGroupListBySquidId(toSquidId);
                            if (groupList != null && groupList.size() > 0) {
                                order = groupList.size() + 1;
                            }
                            group = new ReferenceColumnGroup();
                            group.setReference_squid_id(toSquidId);
                            group.setRelative_order(order);
                            referenceColumnGroupDao.insertReferenceColumnGroup(group);
                        }
                        ReferenceColumn referenceColumn = this.initReference(column1, column1.getId(), fromSquid, toSquidId, group);
                        referenceColumns.add(referenceColumn);
                    }

                    //批量添加下游的referenceColumn
                    referenceColumnDao.insert(referenceColumns);
                    for (ReferenceColumn referenceColumn1 : referenceColumns) {
                        transformations.add(this.initTransformation(toSquidId, referenceColumn1.getColumn_id(), TransformationTypeEnum.VIRTUAL.value(), referenceColumn1.getData_type(), 1));
                    }
                    //批量添加下游虚拟transformation
                    transformationDao.insert(transformations);
                    for(Transformation transformation:transformations){
                        List<Map<String,Object>> mapList=SquidLinkService.getInputscache().get(transformation.getTranstype()+"");
                        if(mapList!=null && mapList.size()>0){
                            List<com.eurlanda.datashire.server.model.TransformationInputs> transformationInputsList=new ArrayList<>();
                            for(Map<String,Object> inputsMap:mapList){
                                com.eurlanda.datashire.server.model.TransformationInputs transformationInputs=new com.eurlanda.datashire.server.model.TransformationInputs();
                                String description=inputsMap.get("description").toString();
                                String relative_order=inputsMap.get("inputOrder").toString();
                                transformationInputs.setRelative_order(Integer.parseInt(relative_order));
                                transformationInputs.setInput_Data_Type(transformation.getOutput_data_type());
                                transformationInputs.setDescription(description);
                                transformationInputs.setSource_transform_id(0);
                                transformationInputs.setTransformationId(transformation.getId());
                                transformationInputsList.add(transformationInputs);
                            }
                            transformation.setInputs(transformationInputsList);
                            transformationInputsLists.addAll(transformationInputsList);
                        }
                    }
                    if(transformationInputsLists!=null && transformationInputsLists.size()>0){
                        transformationInputsDao.insert(transformationInputsLists);
                    }
                }

                //pivotSquid作为下游的时候保留原column只改变referenceColumn
                if(SquidTypeEnum.PIVOTSQUID.value() == squidType){
                    List<Transformation> transformationRightList=new ArrayList<>();
                    List<Transformation> transformationLeftList=new ArrayList<>();
                    List<TransformationLink> transformationLinkList=new ArrayList<>();
                    referenceColumns.clear();
                    exceptionColumnList.clear();
                    transformations.clear();
                    //左侧column集合
                    map.put("fromSquid", fromSquid);
                    map.put("toSquidId", toSquidId);
                    //查询referenceColumnGroup分组
                    ReferenceColumnGroup group = referenceColumnGroupDao.selectGroupBySquidId(map);
                    if (group == null) {
                        int order = 1;
                        List<ReferenceColumnGroup> groupList = referenceColumnGroupDao.getRefColumnGroupListBySquidId(toSquidId);
                        if (groupList != null && groupList.size() > 0) {
                            order = groupList.size() + 1;
                        }
                        group = new ReferenceColumnGroup();
                        group.setReference_squid_id(toSquidId);
                        group.setRelative_order(order);
                        referenceColumnGroupDao.insertReferenceColumnGroup(group);
                    }
                    //循环column，添加referenceColumn
                    for(Column column:columnList){
                        ReferenceColumn referenceColumn = this.initReference(column, column.getId(), fromSquid, toSquidId, group);
                        referenceColumns.add(referenceColumn);
                        //添加column
                        //左侧column
                        Column taggingColumn=initColumn2(referenceColumn,referenceColumn.getRelative_order(),toSquidId,null);
                        exceptionColumnList.add(taggingColumn);
                    }
                    //批量添加referenceCOlumn
                    referenceColumnDao.insert(referenceColumns);

                }


                //
                if (SquidTypeEnum.GROUPTAGGING.value() == squidType || SquidTypeEnum.SAMPLINGSQUID.value() == squidType ) {
                    List<Transformation> transformationRightList=new ArrayList<>();
                    List<Transformation> transformationLeftList=new ArrayList<>();
                    List<TransformationLink> transformationLinkList=new ArrayList<>();
                    referenceColumns.clear();
                    exceptionColumnList.clear();
                    transformations.clear();
                    //左侧column集合
                    map.put("fromSquid", fromSquid);
                    map.put("toSquidId", toSquidId);
                    //查询referenceColumnGroup分组
                    ReferenceColumnGroup group = referenceColumnGroupDao.selectGroupBySquidId(map);
                    if (group == null) {
                        int order = 1;
                        List<ReferenceColumnGroup> groupList = referenceColumnGroupDao.getRefColumnGroupListBySquidId(toSquidId);
                        if (groupList != null && groupList.size() > 0) {
                            order = groupList.size() + 1;
                        }
                        group = new ReferenceColumnGroup();
                        group.setReference_squid_id(toSquidId);
                        group.setRelative_order(order);
                        referenceColumnGroupDao.insertReferenceColumnGroup(group);
                    }
                    //循环column，添加referenceColumn
                    for(Column column:columnList){
                        ReferenceColumn referenceColumn = this.initReference(column, column.getId(), fromSquid, toSquidId, group);
                        referenceColumns.add(referenceColumn);
                        //添加column
                        //左侧column
                        Column taggingColumn=initColumn2(referenceColumn,referenceColumn.getRelative_order(),toSquidId,null);
                        exceptionColumnList.add(taggingColumn);
                    }
                    //批量添加referenceCOlumn
                    referenceColumnDao.insert(referenceColumns);
                    //批量添加Column
                    columnDao.insert(exceptionColumnList);
                    //添加右侧虚拟transformation
                    for(ReferenceColumn referenceColumn:referenceColumns){
                        transformationRightList.add(initTransformation(toSquidId,referenceColumn.getColumn_id(),TransformationTypeEnum.VIRTUAL.value(),referenceColumn.getData_type(),1));
                    }
                    //添加左侧虚拟transformation
                    for(Column column:exceptionColumnList){
                        transformationLeftList.add(initTransformation(toSquidId,column.getId(),TransformationTypeEnum.VIRTUAL.value(),column.getData_type(),1));
                    }
                    transformations.addAll(transformationRightList);
                    transformations.addAll(transformationLeftList);
                    //批量添加transformation
                    transformationDao.insert(transformations);
                    for(Transformation transformation:transformations){
                        List<Map<String,Object>> mapList=SquidLinkService.getInputscache().get(transformation.getTranstype()+"");
                        if(mapList!=null && mapList.size()>0){
                            List<com.eurlanda.datashire.server.model.TransformationInputs> transformationInputsList=new ArrayList<>();
                            for(Map<String,Object> inputsMap:mapList){
                                com.eurlanda.datashire.server.model.TransformationInputs transformationInputs=new com.eurlanda.datashire.server.model.TransformationInputs();
                                String description=inputsMap.get("description").toString();
                                String relative_order=inputsMap.get("inputOrder").toString();
                                transformationInputs.setRelative_order(Integer.parseInt(relative_order));
                                transformationInputs.setInput_Data_Type(transformation.getOutput_data_type());
                                transformationInputs.setDescription(description);
                                transformationInputs.setSource_transform_id(0);
                                transformationInputs.setTransformationId(transformation.getId());
                                transformationInputsList.add(transformationInputs);
                            }
                            transformation.setInputs(transformationInputsList);
                            transformationInputsLists.addAll(transformationInputsList);
                        }
                    }
                    if(transformationInputsLists!=null && transformationInputsLists.size()>0){
                        transformationInputsDao.insert(transformationInputsLists);
                    }
                    //初始化transformationLink
                    for(int i=0 ;i<transformationRightList.size();i++){
                        TransformationLink transformationLink=this.mergeTransformationLink(transformationRightList.get(i).getId(),transformationLeftList.get(i).getId(),i+1);
                        transformationLinkList.add(transformationLink);
                    }
                    //批量添加transformationLink
                    transformationLinkDao.insert(transformationLinkList);

                }
                //todo
                if (SquidTypeEnum.EXCEPTION.value() == squidType) {
                    List<Transformation> transformationRightList=new ArrayList<>();
                    List<Transformation> transformationLeftList=new ArrayList<>();
                    List<TransformationLink> transformationLinkList=new ArrayList<>();
                    //右侧referenceColumn集合
                    referenceColumns.clear();
                    exceptionColumnList.clear();
                    transformations.clear();
                    //左侧column集合
                    map.put("fromSquid", fromSquid);
                    map.put("toSquidId", toSquidId);
                    //查询referenceColumnGroup分组
                    ReferenceColumnGroup group = referenceColumnGroupDao.selectGroupBySquidId(map);
                    if (group == null) {
                        int order = 1;
                        List<ReferenceColumnGroup> groupList = referenceColumnGroupDao.getRefColumnGroupListBySquidId(toSquidId);
                        if (groupList != null && groupList.size() > 0) {
                            order = groupList.size() + 1;
                        }
                        group = new ReferenceColumnGroup();
                        group.setReference_squid_id(toSquidId);
                        group.setRelative_order(order);
                        referenceColumnGroupDao.insertReferenceColumnGroup(group);
                    }
                        //右侧referenceColumn
                        for(ReferenceColumn refeColumn:referenceColumnList){
                        //为了给column 的名字赋值。他拿的是上游的
                            int squid=refeColumn.getHost_squid_id();
                            if(!nameColumns.containsKey(squid)){
                                String squidName = squidDao.getSquidNameByColumnId(refeColumn.getColumn_id());
                                if(squidName!=null && StringUtils.isHavaChinese(squidName)){
                                    SquidTypeEnum types=SquidTypeEnum.valueOf(squidDao.getSquidTypeById(squid));
                                    squidName=types.toString()+squid;
                                }
                                nameColumns.put(squid, squidName);
                            }
                           ReferenceColumn referenceColumn=this.initReference(refeColumn, refeColumn.getId(), fromSquid, toSquidId,group);
                            //左侧column
                            Column exceptioinColumn=initColumn2(refeColumn,refeColumn.getRelative_order(),toSquidId,nameColumns);
                            referenceColumns.add(referenceColumn);
                            exceptionColumnList.add(exceptioinColumn);
                        }
                        //批量添加referenceCOlumn
                        referenceColumnDao.insert(referenceColumns);
                        //批量添加Column
                        columnDao.insert(exceptionColumnList);
                        //右侧虚拟transformation
                        for(ReferenceColumn exceRefeColumn:referenceColumns){
                            transformationRightList.add(initTransformation(toSquidId,exceRefeColumn.getColumn_id(),TransformationTypeEnum.VIRTUAL.value(),exceRefeColumn.getData_type(),1));
                        }
                        //右侧虚拟transformation
                        for(Column column1:exceptionColumnList){
                            transformationLeftList.add(initTransformation(toSquidId,column1.getId(),TransformationTypeEnum.VIRTUAL.value(),column1.getData_type(),1));
                        }
                        transformations.addAll(transformationRightList);
                        transformations.addAll(transformationLeftList);
                        //批量添加transformation
                        transformationDao.insert(transformations);
                    for(Transformation transformation:transformations){
                        List<Map<String,Object>> mapList=SquidLinkService.getInputscache().get(transformation.getTranstype()+"");
                        if(mapList!=null && mapList.size()>0){
                            List<com.eurlanda.datashire.server.model.TransformationInputs> transformationInputsList=new ArrayList<>();
                            for(Map<String,Object> inputsMap:mapList){
                                com.eurlanda.datashire.server.model.TransformationInputs transformationInputs=new com.eurlanda.datashire.server.model.TransformationInputs();
                                String description=inputsMap.get("description").toString();
                                String relative_order=inputsMap.get("inputOrder").toString();
                                transformationInputs.setRelative_order(Integer.parseInt(relative_order));
                                transformationInputs.setInput_Data_Type(transformation.getOutput_data_type());
                                transformationInputs.setDescription(description);
                                transformationInputs.setSource_transform_id(0);
                                transformationInputs.setTransformationId(transformation.getId());
                                transformationInputsList.add(transformationInputs);
                            }
                            transformation.setInputs(transformationInputsList);
                            transformationInputsLists.addAll(transformationInputsList);
                        }
                    }
                    if(transformationInputsLists!=null && transformationInputsLists.size()>0){
                        transformationInputsDao.insert(transformationInputsLists);
                    }
                        //transformationLink
                        for (int j = 0; j < transformationRightList.size(); j++) {
                            TransformationLink transformationLink = this.mergeTransformationLink(transformationRightList.get(j).getId(), transformationLeftList.get(j).getId(), j + 1);
                            transformationLinkList.add(transformationLink);
                        }
                        //批量添加transformationLink
                        transformationLinkDao.insert(transformationLinkList);


                }
                List<EsColumn> esColumnList = new ArrayList<>();
                List<DestHDFSColumn> destHDFSColumnList = new ArrayList<>();
                List<DestImpalaColumn> destImpalaColumnList = new ArrayList<>();
                //DESTES，DEST_HDFS，DEST_IMPALA重新添加
                if (SquidTypeEnum.isDestSquid(squidType)) {
                    if (SquidTypeEnum.DESTES.value() == squidType) {
                        for (Column con : columnList) {
                            esColumnList.add(genEsColumnByColumn(con, toSquidId));
                        }
                        if (esColumnList != null && esColumnList.size() > 0) {
                            //批量添加esColumn
                            esColumnDao.insertBatch(esColumnList);
                        }
                    }
                    if (SquidTypeEnum.DEST_HDFS.value() == squidType || SquidTypeEnum.DESTCLOUDFILE.value() == squidType) {
                        for (Column con : columnList) {
                            destHDFSColumnList.add(getHDFSColumnByColumn(con, toSquidId));
                        }
                        if (destHDFSColumnList != null && destHDFSColumnList.size() > 0) {
                            //批量添加HDFSColumn
                            destHDFSColumnDao.insertBatch(destHDFSColumnList);
                        }
                    }
                    if (SquidTypeEnum.DEST_IMPALA.value() == squidType) {
                        for (Column con : columnList) {
                            destImpalaColumnList.add(getImpalaColumnByColumn(con, toSquidId));
                        }
                        if (destImpalaColumnList != null && destImpalaColumnList.size() > 0) {
                            //批量添加ImpalaColumn
                            destImpalaColumnDao.insertBatch(destImpalaColumnList);
                        }
                    }

                }
                if(transformationList!=null && transformationList.size()>0){
                    transformationList.addAll(transformations);
                }
          //同步下游
          List<Map<String, Object>> squidMaps = squidDao.findNextSquidByFromSquid(toSquidId);
                if(squidMaps!=null && squidMaps.size()>0){
                    for(Map<String, Object> squidMap : squidMaps){
                        int toId = Integer.parseInt(squidMap.get("id") + "");
                        int toSquidType = Integer.parseInt(squidMap.get("squid_type_id") + "");
                        synchronousInsertColumn(toSquidId,toId,toSquidType,transformationList,exceptionColumnList,referenceColumns);
                    }
                }

      }catch (Exception e){
        e.printStackTrace();

      }
    }







}