package com.eurlanda.datashire.sprint7.service.squidflow;

import cn.com.jsoft.jframe.utils.ValidateUtils;
import com.eurlanda.datashire.adapter.DataAdapterFactory;
import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.dao.*;
import com.eurlanda.datashire.dao.dest.IEsColumnDao;
import com.eurlanda.datashire.dao.dest.IHdfsColumnDao;
import com.eurlanda.datashire.dao.dest.ImpalaColumnDao;
import com.eurlanda.datashire.dao.dest.impl.EsColumnDaoImpl;
import com.eurlanda.datashire.dao.dest.impl.HdfsColumnDaoImpl;
import com.eurlanda.datashire.dao.dest.impl.ImpalaColumnDaoImpl;
import com.eurlanda.datashire.dao.impl.*;
import com.eurlanda.datashire.entity.*;
import com.eurlanda.datashire.entity.dest.DestCassandraColumn;
import com.eurlanda.datashire.entity.dest.DestHiveColumn;
import com.eurlanda.datashire.entity.operation.DataSquidCollectionPropertyId;
import com.eurlanda.datashire.entity.operation.StageSquidAndSquidLink;
import com.eurlanda.datashire.enumeration.*;
import com.eurlanda.datashire.enumeration.datatype.SystemDatatype;
import com.eurlanda.datashire.server.utils.TokenUtil;
import com.eurlanda.datashire.sprint7.packet.InfoNewPacket;
import com.eurlanda.datashire.sprint7.packet.InfoPacket;
import com.eurlanda.datashire.sprint7.packet.PushMessagePacket;
import com.eurlanda.datashire.sprint7.plug.*;
import com.eurlanda.datashire.sprint7.service.squidflow.subservice.*;
import com.eurlanda.datashire.sprint7.service.user.subservice.*;
import com.eurlanda.datashire.utility.*;
import com.eurlanda.datashire.validator.SquidValidationTask;
import org.apache.log4j.Logger;

import java.sql.SQLException;
import java.util.*;

/**
 * DataShirService处理 DataShirService业务处理类：处理组合业务 Title : Description: Author
 * :赵春花 2013-9-4 update :赵春花 2013-9-4 Department : JAVA后端研发部 Copyright :
 * ©2012-2013 悦岚（上海）数据服务有限公司
 */
public class DataShirServiceplug extends SupportPlug implements
        IDataShirServiceplug {
    static Logger logger = Logger.getLogger(DataShirServiceplug.class);// 记录日志
    private String token;
    private String key;

    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public DataShirServiceplug(String token) {
        super(token);
        this.token = token;
    }

    public DataShirServiceplug(String token, String key) {
        super(token);
        this.token = token;
        this.key = key;
    }

    /**
     * 删除的通用方法 作用描述： 修改说明：
     *
     * @param infoPackets 对象
     * @param out
     * @return
     */
    public boolean deleteRepositoryObject(List<InfoNewPacket> infoPackets,
                                          ReturnValue out) {
        boolean delete = false;
        IRelationalDataManager adapter3 = null;
        //增加定时器防止删除大数据量的时候，超时
        Timer timer = new Timer();
        final Map<String, Object> returnMap = new HashMap<>();
        returnMap.put("1", 1);
        key = TokenUtil.getKey();
        token = TokenUtil.getToken();
        try {
            timer.schedule(new TimerTask() {
                @Override
                public void run() {
                    PushMessagePacket.pushMap(returnMap, DSObjectType.SQUID_FLOW, "0001", "0025", key, token, MessageCode.BATCH_CODE.value());
                }
            }, 25 * 1000, 25 * 1000);
            adapter3 = DataAdapterFactory.getDefaultDataManager();
            adapter3.openSession();
            // 循环遍历
            for (InfoNewPacket infoPacket : infoPackets) {
                int id = infoPacket.getId();
                if (id == 0) {
                    continue;
                }
                int repositoryId = infoPacket.getRepositoryId();
                int squidFlowId = infoPacket.getSquidFlowId();
                DSObjectType t = DSObjectType.parse(infoPacket.getType());
                switch (t) {
                    case SQUID_FLOW:
                        // 删除SquidFlow
                        delete = this.deleteSquidFlow(adapter3, id, repositoryId, false, out, 1);
                        break;
                    case STREAM_STAGE:
                    case SQUID:
                    case STAGE:
                    case EXTRACT:
                    case DATAMINING:
                        // 删除Squid
                        delete = this.deleteSquid(adapter3, id, repositoryId, out);
                        break;
                    case SOURCETABLE:
                        delete = this.deleteSourceTable(adapter3, id, out);
                        break;
                    case SQUIDLINK:
                        // 删除SquidLink
                        delete = this.deleteSquidLink(adapter3, id, 0, out);
                        break;
                    case COLUMN:
                        // 删除Column
//					delete = this.deleteColumn(adapter3, id, 0);
                        delete = this.delColumn(adapter3, id, 0, out);
                        break;
                    case SQUIDJOIN:
                        // 删除join
                        delete = this.deleteJoin(adapter3, id, 0, out);
                        break;
                    case TRANSFORMATION:
                        // 调用Transformation删除业务类
                        delete = this.deleteTransformation(adapter3, id, 0);
                        break;
                    case TRANSFORMATIONLINK:
                        // 删除TRANSFORMATIONLINK
                        delete = this.deleteTransformationLink(adapter3, id);
                        break;
                    case COLUMNREFERENCE:
                        // 删除引用列
                        ReferenceColumnService referenceColumnService = new ReferenceColumnService(
                                TokenUtil.getToken(), adapter3);
                        delete = referenceColumnService.deleteReferenceColumn(id,
                                out);
                        break;
                    case COLUMNGROUP:
                        // 删除引用列组
                        delete = this.deleteReferenceColumnAndGroup(adapter3, id, 0, out);
                        break;
                    case PROJECT:
                        // 调用Project删除业务类
                        delete = this.deleteProject(adapter3, id, repositoryId, out);
                        break;
                    case TEAM:
                        TeamService teamService = new TeamService();
                        // 调用team删除的接口
                        InfoPacket packetTeam = teamService.remove(id);
                        delete = this.getCommons(packetTeam);
                        out.setMessageCode(MessageCode.parse(packetTeam.getCode()));
                        break;
                    case GROUP:
                        GroupService groupService = new GroupService();
                        // 调用group删除的接口
                        InfoPacket packetGroup = groupService.remove(id);
                        delete = this.getCommons(packetGroup);
                        out.setMessageCode(MessageCode.parse(packetGroup.getCode()));
                        break;
                    case ROLE:
                        RoleService roleService = new RoleService();
                        // 调用role删除的接口
                        InfoPacket packetRole = roleService.remove(id);
                        delete = this.getCommons(packetRole);
                        out.setMessageCode(MessageCode.parse(packetRole.getCode()));
                        break;
                    case USER:
                        UserService userService = new UserService();
                        // 调用user删除的接口
                        InfoPacket packetUser = userService.remove(id);
                        delete = this.getCommons(packetUser);
                        out.setMessageCode(MessageCode.parse(packetUser.getCode()));
                        break;
                    case REPOSITORY:
                        // 调用Repository删除业务类
                        delete = new RepositoryServiceImpl()
                                .deleteRepositoryById(id);
                        if (delete) {
                            out.setMessageCode(MessageCode.SUCCESS);
                        }
                        break;
/*				// added by bo.dang
                case REPORT:
					// 调用ReportSquid删除业务类
					delete = new ReportSquidServicesub(token)
							.deleteReportSquid(id);
					if (delete) {
						out.setMessageCode(MessageCode.SUCCESS);
					}
					break;*/
                    case SQUIDINDEXS:
                        // 调用删除index
                        delete = new SquidIndexsServiceSub(TokenUtil.getToken()).deleteSquidIndexs(id, out);
                        if (delete) {
                            out.setMessageCode(MessageCode.SUCCESS);
                        }
                        break;
                    case JOBSCHEDULE:
                        // 删除调度任务业务类
                        delete = new JobScheduleService().deleteJobSchedule(id,
                                squidFlowId, repositoryId, out);
                        break;
                    case WEBURL:
                        delete = this.deleteWebUrl(adapter3, id);
                    default:
                        break;
                }
            }
        } catch (Exception e) {
            timer.cancel();
            timer.purge();
            logger.error(
                    "[删除deleteRepositoryObject=========================================exception]",
                    e);
            try {
                adapter3.rollback();
            } catch (SQLException e1) {
                logger.error("rollback err!", e1);
            }
            out.setMessageCode(MessageCode.SQL_ERROR);
            return false;
        } finally {
            timer.cancel();
            timer.purge();
            adapter3.closeSession();
        }
        return delete;
    }

    /**
     * 查询squidFlow下的Squid和SquidLink执行推送机制
     * <p/>
     * 作用描述： 根据SquidFlowID 查询Squid信息和SquidLink新 业务描述： 1、DBSourceSquid集合数据包含：
     * 该SquidFlow下的所有DBSourceSquid对象； 相应DBSourceSquid相关联数据库的所有表信息；
     * 相应表相关联的所有列的信息； 2、ExtractSquid集合数据包含： 该SquidFlow下的所有ExtractSquid对象；
     * 相应ExtractSquid下的SquidLink信息； 相应ExtractSquid的来源Column信息；
     * 相应ExtractSquid下的来源Transformation信息； 相应ExtractSquid下的Transformation信息；
     * 相应ExtractSquid下的TransformationLink信息； 3、StageSquid集合数据包含：
     * 该SquidFlow下的所有StageSquid对象； 相应StageSquid下的SquidLink信息；
     * 相应StageSquid的来源Column信息； 相应StageSquid下的来源Transformation信息；
     * 相应StageSquid下的Transformation信息； 相应StageSquid下的TransformationLink信息；
     * //TODO 相应StageSquid下的join信息； 4、DBDestinationSquid集合数据包含：
     * 该SquidFlow下的所有DBDestinationSquid对象； 5、SquidLink
     * 该SquidFlow下的所有SquidLink对象； 6、推送数据的总是（前台要求） 使用数据推送机制 推送顺序
     * 1、DBSourceSquid集合对象推送 2、ExtractSquid集合对象推送 3、StageSquid 集合对象推送
     * 4、DBDestinationSquid集合对象推送 5、SquidLink对象推送 验证：
     * 调用方法前验证Infopacket对象里的Id不能为空，key不能为空 修改说明：
     */
    public int queryAllSquidAndSquidLink(InfoPacket infoPacket, ReturnValue out) {
        int count = 0; // all squid + all squid link
        int id = infoPacket.getId(); // squid flow id
        SquidPlug squidPlug = new SquidPlug(adapter);
        try {
            // 1. dbSourceSquid
            IRelationalDataManager adapter2 = new RepositoryServiceHelper(TokenUtil.getToken())
                    .getAdapter();
            adapter2.openSession();
            DBSourceSquid[] dbSourceSquids = squidPlug
                    .getSquidFlowEDBsouresSquid(adapter2, id, "",
                            SquidTypeEnum.DBSOURCE.toString(), out);
            if (dbSourceSquids != null) {
                DataPlug dataPlug = new DataPlug(TokenUtil.getToken(), adapter);
                for (DBSourceSquid dbSourceSquid : dbSourceSquids) {
                    dbSourceSquid.setSourceTableList(dataPlug.getSourceTable(
                            dbSourceSquid.getId(), out, adapter2));
                    dbSourceSquid.setType(DSObjectType.DBSOURCE.value());
                }
                PushMessagePacket.push(StringUtils.asList(dbSourceSquids),
                        DSObjectType.DBSOURCE, "0001", "0029", TokenUtil.getKey(), TokenUtil.getToken());
            }

            // 2. SquidLink数据
            List<SquidLink> squidLinks = adapter2.query2List2(true, null,
                    SquidLink.class);
            PushMessagePacket.push(squidLinks, DSObjectType.SQUIDLINK, "0001",
                    "0029", TokenUtil.getKey(), TokenUtil.getToken());

            // 3. ExtractSquid
            TableExtractSquid[] extractSquids = squidPlug
                    .getSquidFlowExtractSquid(id,
                            SquidTypeEnum.EXTRACT.toString(), out);
            if (extractSquids != null && extractSquids.length >= 1) {
                for (TableExtractSquid extractSquid : extractSquids) {
                    ExtractService.setExtractSquidData(TokenUtil.getToken(), extractSquid,
                            true, adapter2);
                }
                PushMessagePacket.push(StringUtils.asList(extractSquids),
                        DSObjectType.EXTRACT, "0001", "0029", TokenUtil.getKey(), TokenUtil.getToken());
            }

            // 4. StageSquid
            StageSquid[] squids = squidPlug.getSquidFlowStagSquid(adapter2, id,
                    "", SquidTypeEnum.STAGE.toString(), out);
            if (squids != null && squids.length >= 1) {
                for (StageSquid stageSquid : squids) { // 获得StagSquid
                    StageSquidService.setStageSquidData(adapter2, stageSquid);
                }
                PushMessagePacket.push(StringUtils.asList(squids),
                        DSObjectType.STAGE, "0001", "0029", TokenUtil.getKey(), TokenUtil.getToken());
            }
            adapter2.closeSession();

            // 5. DBDestinationSquid
            PushMessagePacket.push(squidPlug.getDestSquid(adapter2, id, "",
                    SquidTypeEnum.DBDESTINATION.value(), out),
                    DSObjectType.DBSOURCE, "0001", "0029", TokenUtil.getKey(), TokenUtil.getToken());

            count += squidPlug.getSquidCount(id, out);
            count += squidLinks == null ? 0 : squidLinks.size();
        } catch (Exception e) {
            logger.error("dbSourceSquid-datas", e);
        } finally {
            adapter.commitAdapter();
        }
        return count;
    }

    /**
     * 获得StageSquidData 作用描述： 根据StageSquid获得该StageSquid相关联的数据 业务描述：
     * 1、获得该squid的SourceColumns来源列信息 2、获得该squid的Columns本身的列信息
     * 3、获得该Squid所关联的Transformation信息 4、获得该Squid所关联的TransformationLink信息 修改说明：
     *
     * @param out
     * @param stageSquid stageSquid 对象
     */
    private StageSquid getStageSquidData_old(StageSquid stageSquid,
                                             ReturnValue out) {
        ColumnPlug columnPlug = new ColumnPlug(adapter);
        SquidLinkPlug squidLinkPlug = new SquidLinkPlug(adapter);
        TransformationPlug transformationPlug = new TransformationPlug(adapter);
        TransformationLinkPlug transformationLinkPlug = new TransformationLinkPlug(
                adapter);
        RepositoryServiceHelper serviceHelper = new RepositoryServiceHelper(
                TokenUtil.getToken());

        try {
            // 当前squid的所有join集合
            stageSquid.setJoins(new JoinServicesub(TokenUtil.getToken())
                    .getJoinBySquidId(stageSquid.getId()));

            stageSquid
                    .setColumns(columnPlug.getColumns(stageSquid.getId(), out));

            // TODO 如果有多个源？
            int sourceSquidId = squidLinkPlug.getSourceSquidId(
                    stageSquid.getId(), out);
            // DBSourceTable dbSourceTable = null;
            // if (sourceSquidId > 0) {
            // ExtractSquid extractSquid = (ExtractSquid) squidPlug.getSquid(
            // sourceSquidId, out);
            // dbSourceTable = dbSourceTablePlug.getSourceTable(sourceSquidId,
            // extractSquid.getTable_name(), out);
            // }

            List<ReferenceColumn> sourceColumnlist = new ArrayList<ReferenceColumn>();
            // if (dbSourceTable != null) {
            // sourceColumnlist = columnPlug.getColumnByDBSourceTableId(
            // dbSourceTable.getId(), out);
            // }
            List<ReferenceColumnGroup> list = serviceHelper
                    .getReferenceColumnGroupList(stageSquid.getId());
            if (list != null && !list.isEmpty()) {
                for (int i = 0; i < list.size(); i++) {
                    list.get(i).setReferenceColumnList(
                            serviceHelper.getReferenceColumnList(
                                    stageSquid.getId(), list.get(i).getId()));
                    sourceColumnlist.addAll(list.get(i)
                            .getReferenceColumnList());
                }
            }
            stageSquid.setSourceColumns(sourceColumnlist);

            List<Transformation> transformations = transformationPlug
                    .getTransformations(stageSquid.getId(), out);

            List<Transformation> sourceTransformations = transformationPlug
                    .getTransformations(sourceSquidId, out);
            List<Transformation> sourceTransformationList = new ArrayList<Transformation>();
            if (sourceTransformations != null && sourceColumnlist != null) {
                for (Transformation transformation : sourceTransformations) {
                    for (Column sourceColumn : sourceColumnlist) {
                        if (transformation.getColumn_id() == sourceColumn
                                .getId()) {
                            sourceTransformationList.add(transformation);
                        }
                    }
                }
            }
            List<TransformationLink> transformationLinks = new ArrayList<TransformationLink>();
            if (transformations != null && sourceTransformations != null) {
                for (Transformation transformation : transformations) {
                    sourceTransformationList.add(transformation);
                    TransformationLink[] transformationLinkArrayList = transformationLinkPlug
                            .getTransformationLinksTo(transformation.getId(),
                                    out);
                    if (transformationLinkArrayList != null) {
                        for (TransformationLink transformationLink : transformationLinkArrayList) {
                            transformationLinks.add(transformationLink);
                        }
                    }
                }
            }
            stageSquid.setTransformations(sourceTransformationList);
            if (transformationLinks != null) {
                stageSquid.setTransformationLinks(transformationLinks);
            }
        } catch (Exception e) {
            logger.error("getStageSquidData-datas", e);
        }
        return stageSquid;
    }

    /**
     * 根据ExtractSquid获得ExtractSquid 作用描述： 根据ExtractSquid获得该ExtractSquid相关联的数据
     * 业务描述： 1、获得该Squid所相关联的Column信息 2、获得该Squid的来源所关联的Column信息
     * 3、获得该Squid的来源关联的sourceTransformation信息 4、获得该Squid的相关联的Transformation信息
     * 5、获得该Squid的相关联的TransformationLink信息 修改说明：
     *
     * @param extractSquid extractSquid对象
     * @param out
     * @return
     */
    private TableExtractSquid getExtractSquidData_old(
            TableExtractSquid extractSquid, ReturnValue out) {
        ColumnPlug columnPlug = new ColumnPlug(adapter);
        SquidLinkPlug squidLinkPlug = new SquidLinkPlug(adapter);
        TransformationPlug transformationPlug = new TransformationPlug(adapter);
        TransformationLinkPlug transformationLinkPlug = new TransformationLinkPlug(
                adapter);
        // DBSourceTablePlug dbSourceTablePlug = new DBSourceTablePlug(adapter);
        RepositoryServiceHelper serviceHelper = new RepositoryServiceHelper(
                TokenUtil.getToken());

        try {
            // 查询源的连接信息
            // 获得该squid的表名及列信息
            extractSquid.setColumns(columnPlug.getColumns(extractSquid.getId(),
                    out));

            int sourceSquidId = squidLinkPlug.getSourceSquidId(
                    extractSquid.getId(), out);

            List<ReferenceColumn> sourceColumnlist = new ArrayList<ReferenceColumn>();
            // BOHelper.convert(serviceHelper.getSourceColumnList(1,
            // extractSquid.getTable_name()));

            // 获得DBsourceID
			/*
			 * DBSourceTable dbSourceTable = dbSourceTablePlug.getSourceTable(
			 * sourceSquidId, extractSquid.getTable_name(), out);
			 * 
			 * if (dbSourceTable != null) { sourceColumnlist =
			 * columnPlug.getColumnByDBSourceTableId( dbSourceTable.getId(),
			 * out); //extractSquid.setSourceColumns(sourceColumnlist); }
			 */
            List<ReferenceColumnGroup> list = serviceHelper
                    .getReferenceColumnGroupList(extractSquid.getId());
            if (list != null && !list.isEmpty()) {
                for (int i = 0; i < list.size(); i++) {
                    list.get(i).setReferenceColumnList(
                            serviceHelper.getReferenceColumnList(
                                    extractSquid.getId(), list.get(i).getId()));
                    sourceColumnlist.addAll(list.get(i)
                            .getReferenceColumnList());
                }
            }

            extractSquid.setSourceColumns(sourceColumnlist);

            List<Transformation> transformations = transformationPlug
                    .getTransformations(extractSquid.getId(), out);

            List<Transformation> sourceTransformations = transformationPlug
                    .getTransformations(sourceSquidId, out);
            List<Transformation> sourceTransformationList = new ArrayList<Transformation>();
            if (sourceTransformations != null && sourceColumnlist != null) {
                for (Transformation transformation : sourceTransformations) {
                    for (Column sourceColumn : sourceColumnlist) {
                        if (transformation.getColumn_id() == sourceColumn
                                .getId()) {
                            sourceTransformationList.add(transformation);
                        }
                    }
                }
            }
            List<TransformationLink> transformationLinks = new ArrayList<TransformationLink>();
            if (transformations != null && sourceTransformations != null) {
                for (Transformation transformation : transformations) {
                    sourceTransformationList.add(transformation);
                    TransformationLink[] transformationLinkArrayList = transformationLinkPlug
                            .getTransformationLinksTo(transformation.getId(),
                                    out);
                    if (transformationLinkArrayList != null) {
                        for (TransformationLink transformationLink : transformationLinkArrayList) {
                            transformationLinks.add(transformationLink);
                        }
                    }
                }
            }
            extractSquid.setTransformations(sourceTransformationList);
            extractSquid.setTransformationLinks(transformationLinks);
            extractSquid.setType(DSObjectType.EXTRACT.value());
        } catch (Exception e) {
            logger.error("getExtractSquidData-datas", e);
        } finally {
        }
        return extractSquid;
    }

    /**
     * 创建StageSquid和SquidLink 作用描述：
     * 前端传入StageSquidAndSquidLink对象包含StageSquid和SquidLink两个对象
     * 1、根据SquidLink创建连接信息 2、根据SquidLink对象的来源SquidId查询出来源的Column信息(stageSquidID)
     * 3、将来源Column信息新增到Column表 4、新增Transformation信息
     * 5、查询sourceColumn信息和目标Column信息
     * 6、查询SourceTransformation信息和目标Transformation信息
     * <p/>
     * 验证： 调用方法前： StageSquidAndSquidLink对象不能为空 StageSquid id 不能为空 SquidLink
     * 对象不为空且sourceSquidID和目标SquidID不能为空 修改说明：
     *
     * @param squidAndSquidLink squidAndSquidLink对象
     * @param out               异常返回
     * @return
     */
    public StageSquidAndSquidLink createMoreStageSquidAndSquidLink_old(
            StageSquidAndSquidLink squidAndSquidLink, ReturnValue out) {
        boolean create = false;
        // 获得StageSquidID
        StageSquid stageSquid = squidAndSquidLink.getStageSquid();
        SquidLink squidLink = squidAndSquidLink.getSquidLink();
        squidLink.setType(DSObjectType.SQUIDLINK.value());
        // int id = stageSquid.getId();
        // 获得SourceSquidId查询SourceColumn信息
        int sourceSquidId = squidLink.getFrom_squid_id();
        int toSquidId = squidLink.getTo_squid_id();
        // 新增squidlink
        // SquidLinkPlug squidLinkPlug = new SquidLinkPlug(adapter);
        RepositoryServiceHelper serviceHelper = new RepositoryServiceHelper(
                TokenUtil.getToken()); // 原子业务帮助类
        try {
            // create = squidLinkPlug.createSquidLink(squidLink, out);
            // if (create) {
            // squidLink.setId(squidLinkPlug.getSquidLinkId(squidLink.getKey(),
            // out));
            // } else {
            // return squidAndSquidLink;
            // }
            squidLink.setId(serviceHelper.add2(squidLink));
            // ColumnPlug columnPlug = new ColumnPlug(adapter);
            // DBSourceTablePlug dbSourceTablePlug = new
            // DBSourceTablePlug(adapter);
            // String tableName = null;
            // SquidPlug squidPlug = new SquidPlug(adapter);
            // ExtractSquid extractSquid = (ExtractSquid) squidPlug.getSquid(
            // sourceSquidId, out);
            // tableName = extractSquid.getTable_name();
            // int toTableID = getDbsourceTable(id, tableName, out);
            // DBSourceTable dbSourceTable = dbSourceTablePlug.getSourceTable(
            // sourceSquidId, tableName, out);

            List<Column> sourceColumns = serviceHelper
                    .getColumnList(sourceSquidId);

            if (sourceColumns == null) {
                // 源squid没查询到Column信息
                out.setMessageCode(MessageCode.ERR_COLUMN_NULL);
                return squidAndSquidLink;
            }

            List<Column> columns = serviceHelper.getColumnList(toSquidId);
            int columnSize = columns == null ? 0 : columns.size(); // 已存在目标列的个数

            List<ReferenceColumnGroup> rg = serviceHelper
                    .getReferenceColumnGroupList(toSquidId);
            ReferenceColumnGroup columnGroup = new ReferenceColumnGroup();
            columnGroup.setKey(StringUtils.generateGUID());
            columnGroup.setReference_squid_id(toSquidId);
            columnGroup.setRelative_order(rg == null || rg.isEmpty() ? 1 : rg
                    .size() + 1);
            columnGroup.setId(serviceHelper.add2(columnGroup));
            // List<ReferenceColumnGroup> columnGroupList = new
            // ArrayList<ReferenceColumnGroup>();
            // columnGroupList.add(columnGroup);

            List<Column> toColumns = new ArrayList<Column>();
            Column newColumn = null;
            for (int i = 0; i < sourceColumns.size(); i++) {
                Column column = sourceColumns.get(i);
                newColumn = new Column();
                newColumn.setCollation(column.getCollation());
                newColumn.setData_type(column.getData_type());
                newColumn.setLength(column.getLength());
                newColumn.setName(column.getName());
                newColumn.setNullable(column.isNullable());
                newColumn.setPrecision(column.getPrecision());
                newColumn.setRelative_order(columnSize + i + 1);
                newColumn.setSquid_id(toSquidId);
                newColumn.setKey(StringUtils.generateGUID());
                newColumn.setCollation(0);
                newColumn.setType(DSObjectType.COLUMN.value());
                newColumn.setId(serviceHelper.add2(newColumn));
                toColumns.add(newColumn);

                ReferenceColumn ref = new ReferenceColumn();
                ref.setColumn_id(column.getId());
                ref.setId(ref.getColumn_id());
                ref.setCollation(newColumn.getCollation());
                ref.setData_type(newColumn.getData_type());
                ref.setKey(StringUtils.generateGUID());
                ref.setName(newColumn.getName());
                ref.setNullable(newColumn.isNullable());
                ref.setRelative_order(i + 1);
                ref.setSquid_id(sourceSquidId);
                ref.setType(DSObjectType.COLUMN.value());
                ref.setReference_squid_id(toSquidId);
                ref.setHost_squid_id(sourceSquidId);
                ref.setIs_referenced(true);
                ref.setGroup_id(columnGroup.getId());
                serviceHelper.add2(ref);
            }

            // create = columnPlug.createColumns(toColumns, out);
            // if (create) {
            TransformationPlug transformationPlug = new TransformationPlug(
                    adapter);
            int j = 1;
            for (Column column : toColumns) {
                // column.setId(columnPlug.getColumnId(column.getKey(), out));
                // 组装Transformation
                Transformation eTransformation = new Transformation();
                eTransformation.setColumn_id(column.getId());
                eTransformation.setKey(StringUtils.generateGUID());
                eTransformation.setLocation_x(0);
                eTransformation.setLocation_y(j * 25 + 25 / 2);
                eTransformation.setSquid_id(toSquidId);
                eTransformation.setTranstype(TransformationTypeEnum.VIRTUAL
                        .value());
                create = transformationPlug.createTransformation(
                        eTransformation, out);
                if (create == false) {
                    out.setMessageCode(MessageCode.INSERT_ERROR);
                    return squidAndSquidLink;
                }
                j++;
            }

            if (columnSize <= 0) {
                // 自动增加以下 column id 类型为int 型，从0 每次加 1 自增
                newColumn = new Column();
                newColumn.setCollation(0);
                newColumn.setData_type(SystemDatatype.BIGINT.value());
                newColumn.setLength(0);
                newColumn.setName("id");
                newColumn.setNullable(true);
                newColumn.setPrecision(0);
                newColumn.setRelative_order(1);
                newColumn.setSquid_id(toSquidId);
                newColumn.setKey(StringUtils.generateGUID());
                serviceHelper.add(newColumn);
            }
            // squidAndSquidLink.setStageSquid(getStageSquidData(stageSquid,out));
            // }
        } catch (Exception e) {
            logger.error("createMoreStageSquidAndSquidLink-return", e);
        } finally {
            adapter.commitAdapter();
        }
        return squidAndSquidLink;
    }

    /**
     * 创建COlumnm列信息和Transformation信息 作用描述：
     * 根据拖入的列创建相应的列和Transformation信息并返回StageSquidAndSquidLink对象
     * StageSquidAndSquidLink对象包含相关信息 业务描述： 1、根据来源SquidId获得SquidLink
     * 判断SquidLink是否存在不存在的情况下创建新的数据存在则无需创建 2、获得sourceColumns来源列的信息
     * 根据SquidLink得到sourceId来源ID根据来源ＩＤ查询ｓｑｕｉｄ获得来源表名根据ｓｑｕｉｄＩＤ和表名获得代表SourceTableID
     * 根据SourceTableId获得来源列信息 3、根据来源列的信息新增当前Squid的列信息
     * 根据SquidＩＤ和Ｔａｂｌｅｎａｍｅ获得SourceTableID组装列信息进行批量新增
     * 4、根据新增列的信息创建Transformation信息 列信息创建成功后根据前台算法创建Transformation信息
     * 5、组装StageSquidAndSquidLink完整信息返回 stageSquid对象及相关联的信息 SquidLink信息 修改说明：
     *
     * @param squidAndSquidLink squidAndSquidLink对象
     * @param out               异常返回
     * @return
     */
    public void drag2StageSquid(StageSquidAndSquidLink squidAndSquidLink,
                                ReturnValue out) {
        StageSquid stageSquid = squidAndSquidLink.getStageSquid();
        List<Column> columns = stageSquid.getColumns();
        SquidLink squidLink = squidAndSquidLink.getSquidLink();
        RepositoryServiceHelper serviceHelper = new RepositoryServiceHelper(
                TokenUtil.getToken());
        try {
            // 没相同LINK存在的情况下创建
            if (squidLink.getId() <= 0) {
                squidLink.setId(serviceHelper.add2(squidLink));
            }
            squidLink.setType(DSObjectType.SQUIDLINK.value());

            TableExtractSquid squid = serviceHelper.getOne(
                    TableExtractSquid.class, squidLink.getFrom_squid_id());
            if (squid == null) { // 来源squid为空 异常处理
                out.setMessageCode(MessageCode.ERR_SOURCESQUID_NULL);
                return;
            }

            // 创建引用列组
            ReferenceColumnGroup columnGroup = new ReferenceColumnGroup();
            columnGroup.setKey(StringUtils.generateGUID());
            columnGroup.setReference_squid_id(stageSquid.getId());
            columnGroup.setRelative_order(1);
            columnGroup.setId(serviceHelper.add2(columnGroup));
            // List<ReferenceColumnGroup> columnGroupList = new
            // ArrayList<ReferenceColumnGroup>();
            // columnGroupList.add(columnGroup);

            for (int i = 0; i < columns.size(); i++) {
                Column column = columns.get(i);
                column.setRelative_order(i + 1);

                ReferenceColumn ref = new ReferenceColumn();
                ref.setColumn_id(column.getId());
                ref.setId(ref.getColumn_id());
                ref.setCollation(0);
                ref.setData_type(column.getData_type());
                ref.setKey(StringUtils.generateGUID());
                ref.setName(column.getName());
                ref.setNullable(column.isNullable());
                ref.setRelative_order(i + 1);
                ref.setSquid_id(squid.getId());
                ref.setReference_squid_id(stageSquid.getId());
                ref.setHost_squid_id(squid.getId());
                ref.setIs_referenced(true);
                ref.setGroup_id(columnGroup.getId());
                ref.setId(serviceHelper.add2(ref)); // 创建引用列
                column.setId(serviceHelper.add2(column)); // 创建目标列

                // 创建目标列的虚拟变换
                Transformation trans = new Transformation();
                trans.setColumn_id(column.getId());
                trans.setKey(StringUtils.generateGUID());
                trans.setLocation_x(0);
                trans.setLocation_y(i * 25 + 25 / 2);
                trans.setSquid_id(stageSquid.getId());
                trans.setTranstype(TransformationTypeEnum.VIRTUAL.value());
                trans.setId(serviceHelper.add2(trans));
            }
            StageSquidService.setStageSquidData(serviceHelper.getAdapter(), stageSquid);
        } catch (Exception e) {
            logger.error("createMoreColumnAndTransformation-return", e);
        }
        return;
    }

    /**
     * 作用描述：根据suqidFlowId 执行一个suqidFlow
     *
     * @param id  squidFlow的ID
     * @param out
     */
    public void executeFlow(int id, boolean isPush, ReturnValue out) {
        logger.debug(String.format("executeFlow-squidFlowCond:%s....", id));
        // 获取Transformation集合
        try {
            TransformationPlug plugTransformation = new TransformationPlug(
                    adapter);
            Transformation[] transformationArray = plugTransformation
                    .getSquidFlowTransformation(id, out);
            // 获取TransformationLink集合
            TransformationLinkPlug plugTransformationLink = new TransformationLinkPlug(
                    adapter);
            TransformationLink[] transformationLinkArray = plugTransformationLink
                    .getSquidFlowTransformationLink(id, out);
            // 获取ESquid集合
            SquidPlug squidPlug = new SquidPlug(adapter);
            Squid[] squidArray = squidPlug.getSquidFlowESquid(id, out);
            // 获取ESquidLink集合
            SquidLinkPlug squidLinkPlug = new SquidLinkPlug(adapter);
            SquidLink[] squidLinkArray = squidLinkPlug.getSquidFlowSquidLink(
                    id, out);

            // 获取EColumn列集合
            ColumnPlug columnPlug = new ColumnPlug(adapter);
            Map<Integer, List<Column>> squidColumnMap = columnPlug
                    .getSquidFlowColumn(id, out);
            // 获取EDBSourceTable集合
            DBSourceTablePlug sourceTablePlug = new DBSourceTablePlug(adapter);
            Map<Integer, DBSourceTable> squidEDBTableNameMap = sourceTablePlug
                    .getSquidFlowDBSourceTable(id, out);
            // SquidTransformationFactory squidFactory =
            // SquidTransformationFactory.newInstance();
            // squidFactory.setTransformationArray(transformationArray);
            // squidFactory.setTransformationLinkArray(transformationLinkArray);
            // squidFactory.setSquidArray(squidArray);
            // squidFactory.setSquidLinkArray(squidLinkArray);
            // squidFactory.setSquidDatabaseMap(new
            // DBSquidService(token).getAllDBSquids(id, out));
            // squidFactory.setSquidEDBSourceTableMap(squidEDBTableNameMap);
            // squidFactory.setSquidColumnMap(squidColumnMap);
            // // 运行一个SquidFlow
            // squidFactory.runSquidFlow(id, isPush, out);
        } catch (Exception e) {
            logger.error("executeFlow-squidFlowCond:%s....", e);
        } finally {
            adapter.commitAdapter();
        }
    }

    /**
     * 处理外部提供的删除方法的返回值转换
     *
     * @return
     */
    public boolean getCommons(InfoPacket packet) {
        boolean commonsValue = false;
        if (null != packet) {
            if (1 == packet.getCode()) {
                commonsValue = true;
            }
        }
        return commonsValue;
    }


    /**
     * 在删除Squid的时候删除trans, trans inputs, trans link
     *
     * @param adapter
     * @param transId
     * @param squidId
     * @return
     * @throws DatabaseException
     * @throws SQLException
     */
    public boolean deleteTransformationsWhenDeleteSquids(IRelationalDataManager adapter, int transId, int squidId) throws DatabaseException, SQLException {
        if (squidId <= 0)
            return false;
		/* 删除所有transformation的inputs */
        String deleteInputsSql = "delete i from ds_tran_inputs i inner join ds_transformation t on t.ID = i.TRANSFORMATION_ID where t.SQUID_ID = " + squidId;
        adapter.execute(deleteInputsSql);
		/* 删除所有的trans link */
        String deleteTransLinkSql = "delete l from ds_transformation_link l inner join ds_transformation t on t.id = l.FROM_TRANSFORMATION_ID where t.SQUID_ID = " + squidId;
        adapter.execute(deleteTransLinkSql);
		/* 删除所有的transformation */
        String deleteTransSql = "delete from ds_transformation where SQUID_ID = " + squidId;
        adapter.execute(deleteTransSql);
        return true; /* 返回值好像没什么意义 */
    }


    /**
     * 删除Transformation及相关的 TransformationLink、TransInputs等
     *
     * @param adapter3
     * @param transId
     * @param squidId
     * @return
     * @throws DatabaseException
     * @throws SQLException
     */
    public boolean deleteTransformation(IRelationalDataManager adapter3,
                                        int transId, int squidId) throws DatabaseException, SQLException {
        return deleteTransformation(adapter3, transId, squidId, null, null, null);
    }

    public boolean deleteTransformation(IRelationalDataManager adapter3,
                                        int transId, int squidId, List<Integer> transLinkList,
                                        List<TransformationInputs> updateInputs,
                                        List<TransformationInputs> deleteInputs) throws DatabaseException, SQLException {
        boolean flag = true;
        Map<String, String> paramsMap = new HashMap<String, String>();
        paramsMap.clear();
        if (transId > 0) {
            paramsMap.put("id", String.valueOf(transId));
        } else if (squidId > 0) {
            paramsMap.put("squid_id", String.valueOf(squidId));
        } else {
            return flag;
        }
        List<Transformation> transformations = adapter3.query2List(true, paramsMap, Transformation.class);
        if (transformations != null && transformations.size() > 0) {
            String selectSql = "select * ";
            String deleteSql = "delete ";
            String sql = "";
            // 一次删除所有的squid的input
            if (squidId > 0) {
                //先把source的清空了
                sql = " from ds_tran_inputs where source_transform_id in (" +
                        "select id from ds_transformation where 1=1 ";
                sql += " and squid_id=" + squidId;
                sql += ")";
                List<TransformationInputs> transInputs = adapter3.query2List(true, selectSql + sql, null, TransformationInputs.class);
                if (transInputs != null && transInputs.size() > 0) {
                    for (TransformationInputs transformationInput : transInputs) {
                        transformationInput.setIn_condition("");
                        transformationInput.setSource_tran_output_index(0);
                        transformationInput.setSource_transform_id(0);
                        adapter3.update2(transformationInput);
                    }
                }

                sql = " from ds_tran_inputs where transformation_id in (" +
                        "select id from ds_transformation where 1=1 ";
                sql += " and squid_id=" + squidId;
                sql += ")";
                transInputs = adapter3.query2List(true, selectSql + sql, null, TransformationInputs.class);
                if (transInputs != null && transInputs.size() > 0) {
                    flag = adapter3.execute(deleteSql + sql) >= 0 ? true : false;
                }
            }
            if (transId > 0) {
                sql = " from DS_TRANSFORMATION_LINK where from_transformation_id =" + transId +
                        " or to_transformation_id=" + transId;
            } else if (squidId > 0) {
                sql = " from DS_TRANSFORMATION_LINK where " +
                        "from_transformation_id in (select id from ds_transformation where squid_id=" + squidId + ")" +
                        " or " +
                        "to_transformation_id in (select id from ds_transformation where squid_id=" + squidId + ")";
            }
            List<TransformationLink> transLinks = adapter3.query2List(true, selectSql + sql, null, TransformationLink.class);
            if (transLinks != null && transLinks.size() > 0) {
                if (transLinkList != null) {
                    for (TransformationLink transformationLink : transLinks) {
                        transLinkList.add(transformationLink.getId());
                    }
                }
                flag = adapter3.execute(deleteSql + sql) >= 0 ? true : false;
                if (transId > 0) {
                    for (TransformationLink transformationLink : transLinks) {
                        int from_transformation_id = transformationLink.getFrom_transformation_id();
                        int to_transformation_id = transformationLink.getTo_transformation_id();
                        this.resetTransformationInput(adapter3, from_transformation_id, to_transformation_id,
                                null, updateInputs, deleteInputs);
                    }
                }
            }

            //单次删除每个input
            if (transId > 0) {
                paramsMap.clear();
                paramsMap.put("transformation_id", String.valueOf(transId));
                flag = adapter3.delete(paramsMap, TransformationInputs.class) >= 0 ? true : false;

                paramsMap.clear();
                paramsMap.put("source_transform_id", String.valueOf(transId));
                TransformationInputs inputs = adapter3.query2Object2(true, paramsMap, TransformationInputs.class);
                if (inputs != null) {
                    inputs.setIn_condition("");
                    inputs.setSource_tran_output_index(0);
                    inputs.setSource_transform_id(0);
                    flag = adapter3.update2(inputs);
                }
            }
            //删除transformation
            paramsMap.clear();
            if (transId > 0) {
                paramsMap.put("id", String.valueOf(transId));
                // 推送消息泡用
                Transformation transformation = adapter3.query2Object2(true, paramsMap, Transformation.class);
                if (transformation == null) {
                } else {
                    squidId = transformation.getSquid_id();
                }
            } else if (squidId > 0) {
                paramsMap.put("squid_id", String.valueOf(squidId));
            }
            //System.out.println(paramsMap);
            flag = adapter3.delete(paramsMap, Transformation.class) >= 0 ? true : false;
        }
        return flag;
    }

    public boolean deleteTransformationLink(IRelationalDataManager adapter3, int linkId) throws DatabaseException, SQLException {
        boolean flag = true;
        String selectSql = "select * ";
        String deleteSql = "delete ";
        String sql = "";
        if (linkId > 0) {
            sql = " from DS_TRANSFORMATION_LINK where id=" + linkId;
        }
        List<TransformationLink> transLinks = adapter3.query2List(true, selectSql + sql, null, TransformationLink.class);
        if (transLinks != null && transLinks.size() > 0) {
            flag = adapter3.execute(deleteSql + sql) >= 0 ? true : false;
            if (linkId > 0) {
                int from_transformation_id = transLinks.get(0).getFrom_transformation_id();
                int to_transformation_id = transLinks.get(0).getTo_transformation_id();
                this.resetTransformationInput(adapter3, from_transformation_id, to_transformation_id);
                CommonConsts.addValidationTask(new SquidValidationTask(TokenUtil.getToken(), null, MessageBubbleService.setMessageBubble(-1, from_transformation_id, null, MessageBubbleCode.WARN_TRANSFORMATION_NO_LINK.value())));
                CommonConsts.addValidationTask(new SquidValidationTask(TokenUtil.getToken(), null, MessageBubbleService.setMessageBubble(-1, to_transformation_id, null, MessageBubbleCode.WARN_TRANSFORMATION_NO_LINK.value())));
                CommonConsts.addValidationTask(new SquidValidationTask(TokenUtil.getToken(), null, MessageBubbleService.setMessageBubble(-1, to_transformation_id, null, MessageBubbleCode.ERROR_TRAN_PARAMETERS.value())));
            }
        }
        return flag;
    }
	
/*	*//**
     * 删除Column
     * @param adapter3
     * @param squidId
     * @param columnId
     * @return
     * @throws DatabaseException
     *//*
	public boolean deleteColumn(IRelationalDataManager adapter3, int columnId, int squidId) throws DatabaseException{
		boolean flag = true;
		String selectSql = "select *";
		String deleteSql = "delete ";
		String sql = " from ds_column where 1=1";
		if (columnId>0){
			sql = sql + " and id="+columnId;
		} else if (squidId>0){
			sql = sql + " and squid_id="+squidId;
		}
		List<Column> columnLists = adapter3.query2List(true, selectSql+sql, null, Column.class);
		if (columnLists!=null&&columnLists.size()>0){
			flag = adapter3.execute(deleteSql+sql)>=0?true:false;
		}
		return flag;
	}*/


    /**
     * 根据id删除column
     *
     * @param out
     * @return
     * @throws Exception
     */
    public boolean delColumn(IRelationalDataManager adapter3, int columnId, int squidId, ReturnValue out) throws Exception {
        List<DataSquidCollectionPropertyId> dataList = new ArrayList<>();
        return delColumn(adapter3, columnId, squidId, out, dataList);
    }


    /**
     * 根据id删除column信息，记录同步下游squid操作
     *
     * @param adapter3 数据库链接对象
     * @param columnId column信息
     * @param squidId  squid信息
     * @param out      输出对象
     * @return
     * @throws Exception
     */
    public boolean delColumn(IRelationalDataManager adapter3, int columnId, int squidId, ReturnValue out,
                             List<DataSquidCollectionPropertyId> dataList) throws Exception {
        boolean flag = true;
		/*删除整个Squid的逻辑*/
        IColumnDao columnDao = new ColumnDaoImpl(adapter3);
        List<Integer> columnList = new ArrayList<Integer>();

        int tempSquidId = 0;
		/* 在删除整个Squid的时候，ColumnId传参为0 */
        if (squidId > 0 && columnId == 0) {
            List<Column> columnLists = columnDao.getColumnListBySquidId(squidId);
            if (columnLists != null && columnLists.size() > 0) {
                flag = columnDao.delColumnListBySquidId(squidId) >= 0 ? true : false;
            }
            return flag;
        }

		/*删除单个Column的逻辑*/
        ISquidDao squidDao = new SquidDaoImpl(adapter3);
        ISquidLinkDao squidLinkDao = new SquidLinkDaoImpl(adapter3);
        IReferenceColumnDao refColumnDao = new ReferenceColumnDaoImpl(adapter3);
        ITransformationDao transDao = new TransformationDaoImpl(adapter3);
        ITransformationService tranService = new TransformationService(TokenUtil.getToken());
        List<Integer> transList = new ArrayList<Integer>();
        List<Integer> transLinkList = new ArrayList<Integer>();
        List<TransformationInputs> updateInputs = new ArrayList<TransformationInputs>();
        List<TransformationInputs> deleteInputs = new ArrayList<TransformationInputs>();

        //单个ColumnId的处理，并获取column信息
        Column column = columnDao.getObjectById(columnId, Column.class);
        if (StringUtils.isNull(column)) {
            out.setMessageCode(MessageCode.NODATA);
            return false;
        }
        String name = column.getName();
        tempSquidId = column.getSquid_id();
        DataSquidCollectionPropertyId dataPropertId = new DataSquidCollectionPropertyId();
        dataPropertId.setSquidId(tempSquidId);
        //删除Column的Transformation
        Transformation transformation = transDao.getTransformationById(tempSquidId, columnId);
        if (StringUtils.isNotNull(transformation)) {
            flag = deleteTransformation(adapter3, transformation.getId(), 0, transLinkList, updateInputs, deleteInputs);
            if (flag && dataList != null) {
                transList.add(transformation.getId());
                dataPropertId.setTransformationIds(transList);
                dataPropertId.setTransformLinkIds(transLinkList);
                dataPropertId.setUpdateInputs(updateInputs);
                dataPropertId.setDeleteInputs(deleteInputs);
            }
        }
        List<SquidLink> squidLinkList = squidLinkDao.getSquidLinkListByFromSquid(tempSquidId);
        synDelColumn(adapter3, squidLinkList, dataList, columnId, flag);
        //最后删除column，根据id
        flag = columnDao.delete(columnId, Column.class) >= 0 ? true : false;
        if (flag) {
            columnList.add(columnId);
            dataPropertId.setColumnIds(columnList);
        }
        if (dataList != null) {
            dataPropertId.setReferenceColumnIds(new ArrayList<Integer>());
            dataList.add(dataPropertId);
        }
        return flag;
    }

    public void synDelColumn(IRelationalDataManager adapter3, List<SquidLink> squidLinkList, List<DataSquidCollectionPropertyId> dataList, int columnId, boolean flag) throws Exception {
        SquidLink squidLink = null;
        int toSquidId = 0;
        Squid squid = null;
        int squidType = 0;
        if (StringUtils.isNotNull(squidLinkList) && !squidLinkList.isEmpty()) {
            ISquidDao squidDao = new SquidDaoImpl(adapter3);
            IReferenceColumnDao refColumnDao = new ReferenceColumnDaoImpl(adapter3);
            ITransformationDao transDao = new TransformationDaoImpl(adapter3);
            IColumnDao columnDao = new ColumnDaoImpl(adapter3);
            ISquidLinkDao squidLinkDao = new SquidLinkDaoImpl(adapter3);

            for (int j = 0; j < squidLinkList.size(); j++) {
                List<Integer> refColumnList = new ArrayList<>();
                List<Integer> delcolumnList = new ArrayList<>();
                List<Integer> tempTransList = new ArrayList<>();
                List<Integer> tempTransLinkList = new ArrayList<>();
                List<TransformationInputs> tempUpdateInputs = new ArrayList<>();
                List<TransformationInputs> tempDeleteInputs = new ArrayList<>();
                squidLink = squidLinkList.get(j);
                toSquidId = squidLink.getTo_squid_id();
                squid = squidDao.getObjectById(toSquidId, Squid.class);
                squidType = squid.getSquid_type();
                if (StringUtils.isNull(squid)) {
                    continue;
                }
                // 如果是ReportSquid的话，需要生成错误的消息泡
                if (SquidTypeEnum.REPORT.value() == squidType
                        || SquidTypeEnum.GISMAP.value() == squidType
                        || SquidTypeEnum.DESTWS.value() == squidType) {
                    // 49.报表squid的源数据被修改，可能导致报表运行不正常
                    CommonConsts.addValidationTask(new SquidValidationTask(TokenUtil.getToken(), adapter3, MessageBubbleService.setMessageBubble(
                            toSquidId, toSquidId, squid.getName(),
                            MessageBubbleCode.WARN_REPORT_SQUID_COLUMNS_BY_DELETED.value())));
                    continue;
                }
                // 如果是 dest es squid,需要删除下游EsColumn
                if (SquidTypeEnum.DESTES.value() == squidType) {
                    // 需要删除下游EsColumn
                    IEsColumnDao esColumnDao = new EsColumnDaoImpl(adapter3);
                    // 删除 esColumn
                    esColumnDao.deleteEsColumn(squid.getId(), columnId);
                    // 如果是 dest hdfs squid,需要删除下游HdfsColumn
                } else if (SquidTypeEnum.DEST_HDFS.value() == squidType || squidType == SquidTypeEnum.DESTCLOUDFILE.value()) {
                    // 需要删除下游HDFSColumn
                    IHdfsColumnDao hdfsColumnDao = new HdfsColumnDaoImpl(adapter3);
                    // 删除 HDFSColumn
                    hdfsColumnDao.deleteHdfsColumn(squid.getId(), columnId);
                } else if (SquidTypeEnum.DEST_IMPALA.value() == squidType) {
                    // 需要删除下游IMPALAColumn
                    ImpalaColumnDao impalaColumnDao = new ImpalaColumnDaoImpl(adapter3);
                    // 删除 IMPALAColumn
                    impalaColumnDao.deleteImpalaColumn(squid.getId(), columnId);
                } else if (SquidTypeEnum.DEST_HIVE.value() == squidType) {
                    //将columnId置为空
                    String sql = "update ds_dest_hive_column set column_id=0 where squid_id=" + squid.getId() + "  and column_id=" + columnId;
                    adapter3.execute(sql);
                }else if (SquidTypeEnum.DEST_CASSANDRA.value() == squidType) {
                    //将columnId置为空
                    String sql = "update ds_dest_cassandra_column set column_id=0 where squid_id=" + squid.getId() + "  and column_id=" + columnId;
                    adapter3.execute(sql);
                } else {
                    if (SquidTypeEnum.USERDEFINED.value() == squidType) {
						String sql = "update ds_userdefined_datamap_column set column_id=0 where squid_id="+squid.getId()+" and column_id="+columnId;
                        adapter3.execute(sql);
                    } else if(SquidTypeEnum.STATISTICS.value() == squidType){
                        String sql = "update ds_statistics_datamap_column set column_id=0 where squid_id="+squid.getId()+" and column_id="+columnId;
                        adapter3.execute(sql);
                    }
                    // 删除ReferenceColumn
                    ReferenceColumn
                            refColumn =
                            refColumnDao.getReferenceColumnById(toSquidId, columnId);
                    flag = refColumnDao.delReferenceColumnByColumnId(columnId, toSquidId);
                    //查找出Transformation-》transformationLink-》column
                    Transformation trans= transDao.getTransformationById(toSquidId, columnId);
                    if (trans != null) {
                        Map<String, Object> paramMap = new HashMap<>();
                        paramMap.put("FROM_TRANSFORMATION_ID", trans.getId());
                        List<TransformationLink> links = adapter3.query2List2(true, paramMap, TransformationLink.class);
                        if (links != null && links.size() > 0) {
                            paramMap.clear();
                            paramMap.put("id", links.get(0).getTo_transformation_id());
                            paramMap.put("squid_id", toSquidId);
                            List<Transformation> transformationLeft = adapter3.query2List2(true, paramMap, Transformation.class);
                            if (transformationLeft != null && transformationLeft.size() > 0) {
                                int leftColumnId = transformationLeft.get(0).getColumn_id();
                                //删除column
                                if (SquidTypeEnum.GROUPTAGGING.value() == squidType) {
                                    columnDao.delete(leftColumnId, Column.class);
                                    delcolumnList.add(leftColumnId);
                                }
                                List<SquidLink> squidLinks = squidLinkDao.getSquidLinkListByFromSquid(toSquidId);
                                //递归删除
                                synDelColumn(adapter3, squidLinks, dataList, leftColumnId, flag);
                            }
                        }
                    }
                    //删除ReferenceColumnGroup
					/*Map<String,String> param = new HashMap<String,String>();
					param.put("id",refColumn.getGroup_id()+"");
					param.put("reference_squid_id",refColumn.getReference_squid_id()+"");
					adapter3.delete(param,ReferenceColumnGroup.class);*/
                    if (flag && dataList != null) {
                        refColumnList.add(columnId);
                    }
                    Transformation transformation=null;
                    if(refColumn!=null){
                       transformation = transDao.getTransformationById(toSquidId, columnId);
                    }
                    if(transformation!=null&&transformation.getTranstype()==TransformationTypeEnum.VIRTUAL.value()){
                        if (StringUtils.isNotNull(transformation)) {
                            flag = deleteTransformation(adapter3, transformation.getId(), 0,
                                    tempTransLinkList,
                                    tempUpdateInputs, tempDeleteInputs);
                            if (flag && dataList != null) {
                                tempTransList.add(transformation.getId());
                            }
                        }
                    }

                    if (dataList != null) {
                        DataSquidCollectionPropertyId tempData = new DataSquidCollectionPropertyId(
                                toSquidId, delcolumnList, refColumnList, tempTransList,
                                tempTransLinkList,
                                tempUpdateInputs, tempDeleteInputs
                        );
                        dataList.add(tempData);
                    }
                    RepositoryServiceHelper helper = new RepositoryServiceHelper(TokenUtil.getToken(), adapter3);
                    if (refColumn != null) {
                        helper.synchronousDeleteRefColumn(adapter3, refColumn,
                                DMLType.DELETE.value(), dataList);
                    }
                }
            }

        }
    }

    /**
     * 删除SourceTable By Id
     * 2014-12-1
     *
     * @param irdm
     * @param id
     * @param out
     * @return
     * @throws SQLException
     * @author Akachi
     * @E-Mail zsts@hotmail.com
     */
    public boolean deleteSourceTable(IRelationalDataManager irdm, int id, ReturnValue out) throws SQLException {
        String deleteSql = "delete from DS_SOURCE_TABLE where id=?";
        List paramList = new ArrayList();
        paramList.add(id);
        boolean flag = irdm.execute(deleteSql, paramList) >= 0 ? true : false;
        return flag;
    }

    /**
     * 为重复获取元数据的时候，删除Squid的Column， 以及同步下游的referenceColumn提供的方法
     * 根据id删除column
     *
     * @param out
     * @return
     * @throws SQLException
     * @throws DatabaseException
     * @author bo.dang
     */
    public boolean delColumn(IRelationalDataManager adapter3, int squidId, ReturnValue out) throws DatabaseException, Exception {
        boolean flag = true;
        String sql = "";
        Map<String, String> paramMap = new HashMap<String, String>();
        sql = "select * from ds_column where squid_id=" + squidId;
        List<Column> columnLists = adapter3.query2List(true, sql, null, Column.class);
        if (columnLists != null && columnLists.size() > 0) {
	        		/*sql = "select ds.* from ds_squid ds inner join " +
	                		"ds_squid_link dsl on ds.id=dsl.to_squid_id and ds.squid_type_id=20 " +
	                		"where dsl.from_squid_id in " +
	                		"(select to_squid_id from ds_squid_link where from_squid_id="+squidId+")";
	        		SquidModelBase expSquid = adapter3.query2Object(true, sql, null, SquidModelBase.class);
	                if(StringUtils.isNotNull(expSquid)){
	                	for (Column column : columnLists) {
	                		this.delColumn(adapter3, column.getId(), 0, out);
						}
	                }else{
			    		sql = "delete from ds_column where squid_id="+squidId;
			            flag = adapter3.execute(sql)>=0?true:false;
	                }*/
	        		/*sql = "delete from ds_column where squid_id="+squidId;
		            flag = adapter3.execute(sql)>=0?true:false;*/
            for (Column column : columnLists) {
                //同步下游
                DataShirServiceplug plug = new DataShirServiceplug(TokenUtil.getToken());
                plug.delColumn(adapter3, column.getId(), 0, new ReturnValue());
            }
        }
        //删除Column的Transformation
            /*flag = deleteColumnForTrans(adapter3, squidId, flag, paramMap);
            paramMap.clear();
            paramMap.put("from_squid_id", Integer.toString(squidId, 10));
            List<SquidLink> squidLinkList = adapter3.query2List(true, paramMap, SquidLink.class);
            SquidLink squidLink = null;
            int toSquidId = 0;
            SquidModelBase squid = null;
            int squidType = 0;
            Transformation transformation = null;
            // 检索当前Squid是否有下游Squid,如果有下游Squid，那么需要更新下游所有的column, transformation
            if(StringUtils.isNotNull(squidLinkList) && !squidLinkList.isEmpty()){
                //TransformationServicesub transformationService = new TransformationServicesub(token, adapter3);
                for(int j=0; j<squidLinkList.size(); j++){
                    squidLink = squidLinkList.get(j);
                    toSquidId = squidLink.getTo_squid_id();
                    paramMap.clear();
                    paramMap.put("id", Integer.toString(toSquidId, 10));
                    squid = adapter3.query2Object2(true, paramMap, SquidModelBase.class);
                    squidType = squid.getSquid_type();
                    if(StringUtils.isNull(squid)){
                        continue;
                    }
                    // 如果是ReportSquid的话，需要生成错误的消息泡
                    if(SquidTypeEnum.REPORT.value() == squidType){
                        // 49.报表squid的源数据被修改，可能导致报表运行不正常
                        CommonConsts.addValidationTask(new SquidValidationTask(token, adapter3, MessageBubbleService.setMessageBubble(toSquidId, toSquidId, squid.getName(), MessageBubbleCode.WARN_REPORT_SQUID_COLUMNS_BY_DELETED.value())));
                        continue;
                    }
                    
                    // 获取ReferenceColumn
                    paramMap.clear();
                    paramMap.put("host_squid_id", Integer.toString(squidId, 10));
                    paramMap.put("reference_squid_id", Integer.toString(toSquidId, 10));
                    List<ReferenceColumn> referenceColumnList = adapter3.query2List(true, paramMap, ReferenceColumn.class);
                    
                    if(StringUtils.isNull(referenceColumnList) || referenceColumnList.isEmpty()){
                    	continue;
                    }
                    for (int i = 0; i < referenceColumnList.size(); i++) {
                    	paramMap.clear();
                    	paramMap.put("column_id", String.valueOf(referenceColumnList.get(i).getColumn_id()));
                    	paramMap.put("squid_id", Integer.toString(toSquidId, 10));
                    	transformation = adapter3.query2Object2(true, paramMap, Transformation.class);
                    	if(StringUtils.isNotNull(transformation)){
                    		flag = deleteTransformation(adapter3, transformation.getId(), 0);
                    	}
						
					}
                    // 删除ReferenceColumn
                    paramMap.clear();
                    paramMap.put("host_squid_id", Integer.toString(squidId, 10));
                    paramMap.put("reference_squid_id", Integer.toString(toSquidId, 10));
                    flag = adapter3.delete(paramMap, ReferenceColumn.class)>0?true:false;
                    
                    
                    // 同时删除ReferenceColumn对应的虚拟的Transformation、TransformationLink、TransformationInputs
                    
                        Map<String, Object> resultMap = transformationService.delTransformation(transformation.getId(), true, out);
                        
                        flag = (Boolean) resultMap.get("delFlag");
                        List<Integer> toTransformationIdList = (List<Integer>) resultMap.get("toTransformationIdList");
                    
                    // 如果下游是Exception SquidModelBase，那么还需要删除对应的Squid Column（转换器面板上左边Column）
                    
                    sql = "select ds.* from ds_squid ds inner join " +
                		"ds_squid_link dsl on ds.id=dsl.to_squid_id and ds.squid_type_id=20 " +
                		"where dsl.from_squid_id="+toSquidId;
                    SquidModelBase expSquid = adapter3.query2Object(true, sql, null, SquidModelBase.class);
                    if(StringUtils.isNotNull(expSquid)){
                    	paramMap.clear();
            	        paramMap.put("id", Integer.toString(tempSquidId, 10));
            	        SquidModelBase fromSquid = adapter3.query2Object2(true, paramMap, SquidModelBase.class);
                    	
                        paramMap.clear();
                        paramMap.put("name", fromSquid.getName()+"_"+name);
                        paramMap.put("squid_id", String.valueOf(expSquid.getId()));
                        column = adapter3.query2Object2(true, paramMap, Column.class);
                        // 删除ExceptionSquid的Column
                        if (!StringUtils.isNull(column)){
                        	flag = delColumn(adapter3, column.getId(), toSquidId, out);
                        }
                        
                        //删除ExceptionSquid的ReferenceColumn的Transformation
                        this.deleteColumnForTrans(adapter3, columnId, expSquid.getId(), flag, paramMap);
                        
                        //删除ExceptionSquid的ReferenceColumn
                        paramMap.clear();
                        paramMap.put("column_id", Integer.toString(columnId, 10));
                        paramMap.put("reference_squid_id", Integer.toString(expSquid.getId(), 10));
                        ReferenceColumn refColumn = adapter3.query2Object2(true, paramMap, ReferenceColumn.class);
                        if (!StringUtils.isNull(refColumn)){
                        	adapter3.delete(paramMap, ReferenceColumn.class);
                        }
                    }
                    
                }
            }*/
            
/*            //最后删除column，根据id
            paramMap.clear();
            paramMap.put("id", Integer.toString(columnId, 10));
            flag = adapter3.delete(paramMap, Column.class)>=0?true:false;*/
/*            CommonConsts.addValidationTask(new SquidValidationTask(token, adapter3,  MessageBubbleService.setMessageBubble(tempSquidId, columnId, name, MessageBubbleCode.ERROR_COLUMN_DATA_TYPE.value())));
            CommonConsts.addValidationTask(new SquidValidationTask(token, adapter3,  MessageBubbleService.setMessageBubble(tempSquidId, columnId, name, MessageBubbleCode.ERROR_AGGREGATION_OR_GROUP.value())));
            */
        return flag;
    }

    private boolean deleteColumnForTrans(IRelationalDataManager adapter3,
                                         int squidId, boolean flag, Map<String, String> paramMap)
            throws DatabaseException, SQLException {
        Transformation transformation;
        // 删除当前Column的tranformation
        paramMap.clear();
        paramMap.put("squid_id", Integer.toString(squidId, 10));
        transformation = adapter3.query2Object2(true, paramMap, Transformation.class);
        if (StringUtils.isNotNull(transformation)) {
            // 同时删除ReferenceColumn对应的虚拟的Transformation、TransformationLink、TransformationInputs
            flag = deleteTransformation(adapter3, transformation.getId(), 0);
        }
        return flag;
    }

    private boolean deleteColumnForTrans(IRelationalDataManager adapter3,
                                         int columnId, int squidId, List<Integer> transList)
            throws DatabaseException, SQLException {
        Map<String, String> paramMap = new HashMap<String, String>();
        boolean flag = false;
        Transformation transformation;
        // 删除当前Column的tranformation
        paramMap.clear();
        paramMap.put("column_id", String.valueOf(columnId));
        paramMap.put("squid_id", Integer.toString(squidId, 10));
        transformation = adapter3.query2Object2(true, paramMap, Transformation.class);
        if (StringUtils.isNotNull(transformation)) {
            // 同时删除ReferenceColumn对应的虚拟的Transformation、TransformationLink、TransformationInputs
            flag = deleteTransformation(adapter3, transformation.getId(), 0);
            if (flag && transList != null) {
                transList.add(transformation.getId());
            }
        }
        return flag;
    }


    /**
     * 删除Squid时，删除ReferenceColumn，不对Transformation做处理
     *
     * @param adapter
     * @param groupId
     * @param squidId
     * @param out
     * @return
     * @throws Exception
     */
    public boolean deleteReferenceColumnAndGroupInaCurrentySquid(IRelationalDataManager adapter, int groupId, int squidId, ReturnValue out) throws Exception {
        Map<String, String> paramsMap = new HashMap<>(); /* 参数 */
        if (groupId > 0 && squidId == 0) {
            paramsMap.put("group_id", String.valueOf(groupId));
        } else if (squidId > 0) {
            paramsMap.put("reference_squid_id", String.valueOf(squidId));
        }
		/* 先做同步 */
        List<ReferenceColumn> referenceColumnList = adapter.query2List(true, paramsMap, ReferenceColumn.class);
        RepositoryServiceHelper helper = new RepositoryServiceHelper(TokenUtil.getToken(), adapter);
        for (ReferenceColumn item : referenceColumnList) {
            helper.synchronousDeleteRefColumn(adapter, item, DMLType.DELETE.value(), null);
            if (groupId == 0)
                groupId = item.getGroup_id();
        }
		/* 开始删除 */
        String deleteRCSql = "delete from DS_REFERENCE_COLUMN where REFERENCE_SQUID_ID = " + squidId;
        String deleteRGSql = "delete from DS_REFERENCE_COLUMN_GROUP where id = " + groupId;

        int i = adapter.execute(deleteRCSql); /* 删除ReferenceColumn */
        int j = adapter.execute(deleteRGSql); /* 删除ReferenceColumn Group */

        return i > 0 && j > 0;
    }

    /**
     * 删除ReferenceColumn和ReferenceColumnGroup
     *
     * @param adapter3
     * @param squidId
     * @param groupId
     * @return
     * @throws Exception
     */
    public boolean deleteReferenceColumnAndGroup(IRelationalDataManager adapter3, int groupId, int squidId, ReturnValue out) throws Exception {
        boolean flag = true;
        Map<String, String> paramsMap = new HashMap<String, String>();
        if (groupId > 0 && squidId == 0) {
            paramsMap.put("group_id", String.valueOf(groupId));
        } else if (squidId > 0) {
            paramsMap.put("reference_squid_id", String.valueOf(squidId));
        }
        List<ReferenceColumn> refColumns = adapter3.query2List(true, paramsMap, ReferenceColumn.class);
        if (refColumns != null && refColumns.size() > 0) {
            if (groupId > 0) {
                for (ReferenceColumn referenceColumn : refColumns) {
                    String sql = "select dt.id from ds_transformation dt " +
                            "inner join ds_reference_column drc " +
                            "on dt.column_id=drc.column_id and drc.reference_squid_id=dt.squid_id and drc.group_id=" + referenceColumn.getGroup_id() +
                            " where drc.reference_squid_id=" + referenceColumn.getReference_squid_id() + " and drc.column_id=" + referenceColumn.getColumn_id();
                    Map<String, Object> map = adapter3.query2Object(true, sql, null);
                    if (map != null && map.containsKey("ID")) {
                        String id = map.get("ID") + "";
                        if (ValidateUtils.isNumeric(id)) {
                            int transId = Integer.parseInt(map.get("ID") + "");
                            flag = this.deleteTransformation(adapter3, transId, 0);
                        }
                    }
                    //同步下游
                    RepositoryServiceHelper helper = new RepositoryServiceHelper(TokenUtil.getToken(), adapter3);
                    helper.synchronousDeleteRefColumn(adapter3, referenceColumn, DMLType.DELETE.value(), null);
                }
            }
            flag = adapter3.delete(paramsMap, ReferenceColumn.class) >= 0 ? true : false;
            if (flag) {
                if (groupId > 0) {
                    paramsMap.clear();
                    paramsMap.put("id", String.valueOf(groupId));
                }
                flag = adapter3.delete(paramsMap, ReferenceColumnGroup.class) >= 0 ? true : false;
            }
            // 删除Squid同步到下游的Squid
            if (groupId > 0 && squidId > 0) {
                paramsMap.clear();
                paramsMap.put("id", Integer.toString(squidId, 10));
                Squid squid = adapter3.query2Object2(true, paramsMap, Squid.class);
                int squidType = squid.getSquid_type();
                // 如果下游是Exception SquidModelBase, 对应 squid column也要删除
                if (SquidTypeEnum.EXCEPTION.value() == squidType) {
                    flag = delColumn(adapter3, 0, squidId, out);
                    flag = deleteTransformation(adapter3, 0, squidId);
                }
            }
        }
        return flag;
    }

    /**
     * 删除join
     *
     * @param adapter
     * @param joinId
     * @param squidId
     * @return
     * @throws Exception
     */
    public boolean deleteJoin(IRelationalDataManager adapter, int joinId,
                              int squidId, ReturnValue out) throws Exception {
        boolean flag = true;
        String sql = "select * from DS_JOIN where 1=1";
        if (joinId > 0) {
            sql += " and id=" + joinId;
        } else if (squidId > 0) {
            sql += " and target_squid_id=" + squidId;
        }
        List<SquidJoin> joins = adapter.query2List(true, sql, null, SquidJoin.class);
        if (joins != null && joins.size() > 0) {
            for (SquidJoin squidJoin : joins) {
                Map<String, Object> map = adapter.query2Object(true, "SELECT distinct group_id FROM DS_REFERENCE_COLUMN WHERE "
                        + "host_squid_id=" + squidJoin.getTarget_squid_id() + " AND reference_squid_id="
                        + squidJoin.getJoined_squid_id(), null);
                if (map != null && map.containsKey("GROUP_ID")) {
                    String id = map.get("GROUP_ID") + "";
                    if (ValidateUtils.isNumeric(id)) {
                        int group_id = Integer.parseInt(map.get("GROUP_ID") + "");
                        flag = this.deleteReferenceColumnAndGroup(adapter, group_id, 0, out);
                    }
                }
        }
        }
        if (0 != squidId && 0 == joinId) {
            // 根据squidId删除join
            sql = "delete from DS_JOIN where target_squid_id=" + squidId
                    + " or joined_squid_id=" + squidId;
        } else if (0 == squidId && 0 != joinId) {
            sql = "delete from DS_JOIN where id=" + joinId;
        }
        flag = adapter.execute(sql) >= 0 ? true : false;
        return flag;
    }

    /**
     * 删除squidlink
     *
     * @param adapter
     * @param linkId
     * @param squidId
     * @return
     * @throws Exception
     */
    public boolean deleteSquidLink(IRelationalDataManager adapter, int linkId, int squidId, ReturnValue out) throws Exception {
        boolean flag = true;
        String sql = "select * from DS_SQUID_LINK where 1=1";
        if (linkId > 0) {
            sql += " and id=" + linkId;
        } else if (squidId > 0) {
            sql += " and from_squid_id=" + squidId;
        }
        List<SquidLink> links = adapter.query2List(true, sql, null, SquidLink.class);
        if (links != null && links.size() > 0) {
            for (SquidLink squidLink : links) {
                // 如果下游为destEsSquid，需要删除squid中的escolumn
                int toSquidId = squidLink.getTo_squid_id();
                Map<String, Object> params = new HashMap<>();
                params.put("id", toSquidId);
                Squid squid = adapter.query2Object(true, null, Squid.class);
                if (squid.getSquid_type() == SquidTypeEnum.DESTES.value()) {
                    IEsColumnDao esColumnDao = new EsColumnDaoImpl(adapter);
                    esColumnDao.deleteEsColumnBySquidId(toSquidId);
                    continue;
                }
                // 如果下游为destHdfsSquid，需要删除squid中的hdfscolumn
                if (squid.getSquid_type() == SquidTypeEnum.DEST_HDFS.value() || squid.getSquid_type() == SquidTypeEnum.DESTCLOUDFILE.value()) {
                    IHdfsColumnDao hdfsColumnDao = new HdfsColumnDaoImpl(adapter);
                    hdfsColumnDao.deleteHdfsColumnBySquidId(toSquidId);
                    continue;
                }
                // 如果下游为destImpalaSquid，需要删除squid中的impalacolumn
                if (squid.getSquid_type() == SquidTypeEnum.DEST_IMPALA.value()) {
                    ImpalaColumnDao impalaColumnDao = new ImpalaColumnDaoImpl(adapter);
                    impalaColumnDao.deleteImpalaColumnBySquidId(toSquidId);
                    continue;
                }

                // 删除link 下游squid的 referencecolumn
                Map<String, Object> map = adapter.query2Object(true, "SELECT distinct group_id FROM DS_REFERENCE_COLUMN WHERE "
                        + "host_squid_id=" + squidLink.getFrom_squid_id() + " AND reference_squid_id="
                        + squidLink.getTo_squid_id(), null);
                if (map != null && map.containsKey("GROUP_ID")) {
                    String id = map.get("GROUP_ID") + "";
                    if (ValidateUtils.isNumeric(id)) {
                        int group_id = Integer.parseInt(map.get("GROUP_ID") + "");
                        flag = this.deleteReferenceColumnAndGroup(adapter, group_id, squidLink.getTo_squid_id(), out);
                    }
                }

            }
        }
        int execut = 0;
        if (0 != squidId && 0 == linkId) {
            //删除link
            sql = "delete from DS_SQUID_LINK where from_squid_id=" + squidId
                    + " or to_squid_id=" + squidId;
            //删除referenceGroup
        } else {
            sql = "delete from DS_SQUID_LINK where id=" + linkId;
        }
        execut = adapter.execute(sql);
        if (execut < 0) {
            flag = false;
        }
        return flag;
    }


    /**
     * 删除squid
     *
     * @param adapter
     * @param squidId
     * @return
     * @throws Exception
     */
    public boolean deleteSquid(IRelationalDataManager adapter, int squidId, int repositoryId, ReturnValue out) throws Exception {
        boolean flag = true;
        //从子表到主表倒序删除
        Map<String, String> paramMap = new HashMap<String, String>();
        paramMap.clear();
        paramMap.put("id", String.valueOf(squidId));
        Squid squid = adapter.query2Object2(true, paramMap, Squid.class);
        long start = System.currentTimeMillis();
        if (squid != null) {
            int fromSquidId = 0;
            int sourceTableId = 0;
            SquidLink squidLink = null;
            DocExtractSquid docExtractSquid = null;
            DataSquid dataSquid = null;
            Boolean updateFlag = false;
            Boolean breakFlag = false;
            if (SquidTypeEnum.DOC_EXTRACT.value() == squid.getSquid_type()
                    || SquidTypeEnum.EXTRACT.value() == squid.getSquid_type()
                    || SquidTypeEnum.XML_EXTRACT.value() == squid.getSquid_type()
                    || SquidTypeEnum.WEIBOEXTRACT.value() == squid.getSquid_type()
                    || SquidTypeEnum.WEBEXTRACT.value() == squid.getSquid_type()
                    || SquidTypeEnum.WEBLOGEXTRACT.value() == squid.getSquid_type()
                    || SquidTypeEnum.WEBSERVICEEXTRACT.value() == squid.getSquid_type()
                    || SquidTypeEnum.HTTPEXTRACT.value() == squid.getSquid_type()
                    || SquidTypeEnum.KAFKAEXTRACT.value() == squid.getSquid_type()
                    || SquidTypeEnum.HBASEEXTRACT.value() == squid.getSquid_type()
                    || SquidTypeEnum.MONGODBEXTRACT.value() == squid.getSquid_type()
                    || SquidTypeEnum.HIVEEXTRACT.value() == squid.getSquid_type()
                    || SquidTypeEnum.CASSANDRA_EXTRACT.value() == squid.getSquid_type()) {
                paramMap.clear();
                paramMap.put("to_squid_id", Integer.toString(squidId, 10));
                squidLink = adapter.query2Object2(true, paramMap, SquidLink.class);
                if (StringUtils.isNotNull(squidLink)) {
                    fromSquidId = squidLink.getFrom_squid_id();
                    paramMap.clear();
                    paramMap.put("id", Integer.toString(squidId, 10));

                    if (SquidTypeEnum.DOC_EXTRACT.value() == squid.getSquid_type()) {
                        docExtractSquid = adapter.query2Object2(true, paramMap, DocExtractSquid.class);
                        sourceTableId = docExtractSquid.getSource_table_id();
                    } else if (SquidTypeEnum.KAFKAEXTRACT.value() == squid.getSquid_type()) {
                        dataSquid = adapter.query2Object2(true, paramMap, KafkaExtractSquid.class);
                        sourceTableId = dataSquid.getSource_table_id();
                    } else if (SquidTypeEnum.HBASEEXTRACT.value() == squid.getSquid_type()) {
                        dataSquid = adapter.query2Object2(true, paramMap, HBaseExtractSquid.class);
                        sourceTableId = dataSquid.getSource_table_id();
                    } else if (SquidTypeEnum.EXTRACT.value() == squid.getSquid_type()
                            || SquidTypeEnum.XML_EXTRACT.value() == squid.getSquid_type()
                            || SquidTypeEnum.WEIBOEXTRACT.value() == squid.getSquid_type()
                            || SquidTypeEnum.WEBEXTRACT.value() == squid.getSquid_type()
                            || SquidTypeEnum.WEBLOGEXTRACT.value() == squid.getSquid_type()
                            || SquidTypeEnum.WEBSERVICEEXTRACT.value() == squid.getSquid_type()
                            || SquidTypeEnum.HTTPEXTRACT.value() == squid.getSquid_type()
                            || SquidTypeEnum.MONGODBEXTRACT.value() == squid.getSquid_type()
                            || SquidTypeEnum.HIVEEXTRACT.value() == squid.getSquid_type()
                            || SquidTypeEnum.CASSANDRA_EXTRACT.value() == squid.getSquid_type()) {
                        dataSquid = adapter.query2Object2(true, paramMap, DataSquid.class);
                        sourceTableId = dataSquid.getSource_table_id();
                    }
                }
            } else {
                breakFlag = true;
            }
            //删除 webserviceSquid httpConnectionSquid
            Date now = new Date();
            long nowNumber = now.getTime();
            int squit_type = squid.getSquid_type();


            flag = this.deleteReferenceColumnAndGroupInaCurrentySquid(adapter, 0, squidId, out);  /* 已经重写 */
            flag = this.delColumn(adapter, 0, squidId, out);		/* 已经是批量删除后再同步(需要重写，将Trans删除拿掉) */
            flag = this.deleteTransformationsWhenDeleteSquids(adapter, 0, squidId); 	/* 已经重写 */
            flag = this.deleteJoin(adapter, 0, squidId, out); 		/* 量不大 */
            flag = this.deleteSquidLink(adapter, 0, squidId, out); 	/* 涉及到连带删除，所以需要同步下游Squid，重写意义不大 */
            flag = this.deleteThirdPartyParams(adapter, squidId, out); /* 量不大 */


//			flag = this.deleteReferenceColumnAndGroup(adapter, 0, squidId, out);
//			flag = this.delColumn(adapter, 0, squidId, out);
//			flag = this.deleteTransformation(adapter, 0, squidId);
//			flag = this.deleteJoin(adapter, 0, squidId, out);
//			flag = this.deleteSquidLink(adapter, 0, squidId, out);
//			flag = this.deleteThirdPartyParams(adapter, squidId, out);

            logger.info("///////////////" + (nowNumber - new Date().getTime()));

            //删除squid类型所对应的数据表
            paramMap.clear();

            paramMap.put("id", Integer.toString(squidId, 10)); // fixed bug 663 by bo.dang

            Class c = SquidTypeEnum.classOfValue(squid.getSquid_type());
            flag = adapter.delete(paramMap, c) >= 0 ? true : false;

            // 如果删除的是ReportSquid,需要同时删除ReportFolderMapping
            if (SquidTypeEnum.REPORT.value() == squid.getSquid_type()) {
                ReportFolderServicesub reportFolderServicesub = new ReportFolderServicesub(adapter);
                reportFolderServicesub.deleteReportFolderMappingBySquidId(repositoryId, squidId);
            }

            paramMap.clear();
            paramMap.put("id", String.valueOf(squidId));
            adapter.delete(paramMap, DocExtractSquid.class);

            paramMap.clear();
            paramMap.put("host_squid_id", String.valueOf(squid.getId()));
            adapter.delete(paramMap, ReferenceColumn.class);

            paramMap.clear();
            paramMap.put("reference_squid_id", String.valueOf(squidId));
            adapter.delete(paramMap, ReferenceColumn.class);

            paramMap.clear();
            paramMap.put("reference_squid_id", String.valueOf(squidId));
            adapter.delete(paramMap, ReferenceColumnGroup.class);

            paramMap.clear();
            paramMap.put("squid_id", String.valueOf(squidId));
            adapter.delete(paramMap, DSVariable.class);

            paramMap.clear();
            paramMap.put("squid_id", String.valueOf(squidId));
            adapter.delete(paramMap, SquidIndexs.class);

            paramMap.clear();
            paramMap.put("source_squid_id", String.valueOf(squid.getId()));
            adapter.delete(paramMap, DBSourceTable.class);

            //删除UserDefined dataMap表
            paramMap.clear();
            paramMap.put("squid_id", String.valueOf(squidId));
            adapter.delete(paramMap, UserDefinedMappingColumn.class);

            //删除UserDefined parameter表
            paramMap.clear();
            paramMap.put("squid_id", String.valueOf(squidId));
            adapter.delete(paramMap, UserDefinedParameterColumn.class);

            //删除dest_hiveColumn表
            paramMap.clear();
            paramMap.put("squid_id", String.valueOf(squidId));
            adapter.delete(paramMap, DestHiveColumn.class);

            //删除dest_cassandraColumn表
            paramMap.clear();
            paramMap.put("squid_id", String.valueOf(squidId));
            adapter.delete(paramMap, DestCassandraColumn.class);

            //删除statistic dataMap
            paramMap.clear();
            paramMap.put("squid_id", String.valueOf(squidId));
            adapter.delete(paramMap, StatisticsDataMapColumn.class);

            //删除statistic parameterColumn
            paramMap.clear();
            paramMap.put("squid_id", String.valueOf(squidId));
            adapter.delete(paramMap, StatisticsParameterColumn.class);

            //删除squid表
            paramMap.clear();
            paramMap.put("id", String.valueOf(squidId));
            flag = adapter.delete(paramMap, Squid.class) >= 0 ? true : false;
            logger.info("squid 删除时间:" + (System.currentTimeMillis() - start));
            // 更新SourceTabel中的is_extracted
            if (!breakFlag && (SquidTypeEnum.DOC_EXTRACT.value() == squid.getSquid_type()
                    || SquidTypeEnum.EXTRACT.value() == squid.getSquid_type()
                    || SquidTypeEnum.XML_EXTRACT.value() == squid.getSquid_type()
                    || SquidTypeEnum.WEIBOEXTRACT.value() == squid.getSquid_type()
                    || SquidTypeEnum.WEBEXTRACT.value() == squid.getSquid_type()
                    || SquidTypeEnum.WEBLOGEXTRACT.value() == squid.getSquid_type()
                    || SquidTypeEnum.WEBSERVICEEXTRACT.value() == squid.getSquid_type()
                    || SquidTypeEnum.HTTPEXTRACT.value() == squid.getSquid_type()
                    || SquidTypeEnum.KAFKAEXTRACT.value() == squid.getSquid_type()
                    || SquidTypeEnum.HBASEEXTRACT.value() == squid.getSquid_type()
                    || SquidTypeEnum.MONGODBEXTRACT.value() == squid.getSquid_type())
                    || SquidTypeEnum.CASSANDRA_EXTRACT.value() == squid.getSquid_type()
                    || SquidTypeEnum.HIVEEXTRACT.value() == squid.getSquid_type()) {
                String sql = "update ds_source_table set is_extracted = 'N' where id = " + sourceTableId + " and source_squid_id = " + fromSquidId;
                adapter.execute(sql);
                updateFlag = true;
            }
        }
        return flag;
    }

    /**
     * 删除ThirdPartyParams
     * 2014-12-11
     *
     * @param adapter
     * @param squidFlowId
     * @param out
     * @return
     * @throws SQLException
     * @author Akachi
     * @E-Mail zsts@hotmail.com
     */
    private boolean deleteThirdPartyParams(IRelationalDataManager adapter, int squidFlowId, ReturnValue out) throws SQLException {
        Map<String, String> params = new HashMap<String, String>();
        params.put("squid_id", squidFlowId + "");
        adapter.delete(params, ThirdPartyParams.class);
        return true;
    }

    /**
     * 删除Squid flow
     *
     * @param adapter
     * @param squidFlowId
     * @param repositoryId
     * @param isDeleteAllSquidInSquidflow 是否为删除Squidflow内的所有Squid
     * @param out
     * @param tag(0:表示删除一个Squidflow内所有的Squid时不用删squidflow状态表,1:表示要删)
     * @return
     * @throws Exception
     */
    public boolean deleteSquidFlow(IRelationalDataManager adapter, int squidFlowId, int repositoryId, boolean isDeleteAllSquidInSquidflow, ReturnValue out, int tag) throws Exception {
        // 如果是删除SquidFlow的话，那么不需要执行消息泡校验逻辑
        boolean flag = true;
        Map<String, String> paramMap = new HashMap<String, String>();
        List<SquidFlowStatus> statusList = null;
        if (squidFlowId == 0) {
            out.setMessageCode(MessageCode.NODATA);
            return false;
        }
        if (0 == repositoryId)
            return false;
        // 根据repositoryId和squidFlowId去系统表查询squidflow的状态
        if (!isDeleteAllSquidInSquidflow) {
            LockSquidFlowProcess lockSquidFlowProcess = new LockSquidFlowProcess();
            statusList = lockSquidFlowProcess.getSquidFlowStatus2(
                    repositoryId, squidFlowId, out, adapter);
            Map<String, Object> map = CalcSquidFlowStatus
                    .calcStatus(statusList);
            if (Integer.parseInt(map.get("schedule").toString()) > 0
                    || Integer.parseInt(map.get("checkout").toString()) > 0) {// 调度的个数大于0或者已经加锁
                out.setMessageCode(MessageCode.WARN_DELETESQUIDFLOW);
                return true;
            }
        }
        //TODO： 现在没有删除从功能中拿掉但是数据库表还在的那些数据
		/* 直接删除所有表中数据，不需要同步 */
        logger.info("开始删除Squidflow");
        String deleteVarSql = "delete from ds_variable where SQUID_FLOW_ID = " + squidFlowId; /* 删除变量 */
		/* 如果只删除Squidflow内的所有Squid，只需要删除Squid内的变量即可 */
        if (isDeleteAllSquidInSquidflow)
            deleteVarSql = "delete v from ds_variable v inner join ds_squid s on s.ID = v.SQUID_ID where s.SQUID_FLOW_ID = " + squidFlowId; /*删除当前Squid内的变量*/
        adapter.execute(deleteVarSql);

        String deleteTransformationInputsSql = "delete i from ds_tran_inputs i inner join ds_transformation t on t.ID = i.SOURCE_TRANSFORM_ID inner join ds_squid s on s.ID = t.SQUID_ID where s.SQUID_FLOW_ID = " + squidFlowId; /* 删除 inputs */
        String deleteTransformationLinkSql = "delete l from ds_transformation_link l inner join ds_transformation t on l.FROM_TRANSFORMATION_ID = t.ID inner join ds_squid s on s.ID = t.SQUID_ID where s.SQUID_FLOW_ID = " + squidFlowId; /* 删除trans link */
        String deleteTransformationSql = "delete t from ds_transformation t inner join ds_squid s on s.ID = t.SQUID_ID where s.SQUID_FLOW_ID = " + squidFlowId; /* 删除trans */
        adapter.execute(deleteTransformationInputsSql);
        adapter.execute(deleteTransformationLinkSql);
        adapter.execute(deleteTransformationSql);

        //删除Column ReferenceColumn 与 ReferenceColumn Gourp
        String deleteColumnSql = "delete c from ds_column c inner join ds_squid s on c.SQUID_ID = s.ID where s.SQUID_FLOW_ID = " + squidFlowId; /* 删除Column */
        String deleteRefernceColumnSql = "delete r from ds_reference_column r inner join ds_squid s on r.REFERENCE_SQUID_ID = s.ID where s.SQUID_FLOW_ID = " + squidFlowId; /* 删除RefColumn */
        String deleteRefernceColumnGroupSql = "delete g from ds_reference_column_group g inner join ds_squid s on g.REFERENCE_SQUID_ID = s.ID where s.SQUID_FLOW_ID = " + squidFlowId; /* 删除RefColumn Group */
        adapter.execute(deleteColumnSql);
        adapter.execute(deleteRefernceColumnSql);
        adapter.execute(deleteRefernceColumnGroupSql);

        //删除SourceTable与SourceTable column
        String deleteSourceTableColumnSql = "delete c from ds_source_column c inner join ds_source_table t on c.SOURCE_TABLE_ID = t.ID inner join ds_squid s on t.SOURCE_SQUID_ID = s.ID where s.SQUID_FLOW_ID = " + squidFlowId;
        String deleteSourceTableSql = "delete t from ds_source_table t inner join ds_squid s on t.SOURCE_SQUID_ID = s.ID where s.SQUID_FLOW_ID = " + squidFlowId;
        adapter.execute(deleteSourceTableColumnSql);
        adapter.execute(deleteSourceTableSql);


        //删除ES SquidModelBase 与 column
//        String deleteESSquidColumnSql = "delete c from ds_es_column c inner join ds_dest_es_squid e on c.SQUID_ID = e.ID inner join ds_squid s on s.ID = e.ID where s.SQUID_FLOW_ID = " + squidFlowId;
//        String deleteESSquidSql = "delete e from ds_dest_es_squid e inner join ds_squid s on s.ID = e.ID where s.SQUID_FLOW_ID = " + squidFlowId;
        String deleteESSquidColumnSql = "delete c from ds_es_column c inner join ds_squid s on c.SQUID_ID = s.ID where s.SQUID_FLOW_ID = " + squidFlowId;
//        String deleteESSquidSql = "delete from ds_squid where SQUID_FLOW_ID = " + squidFlowId;
        adapter.execute(deleteESSquidColumnSql);
//        adapter.execute(deleteESSquidSql);

        //删除注释Squid
//        String deleteAnnotationSquidSql = "delete a from ds_annotation_squid a inner join ds_squid s on s.ID = a.ID where s.SQUID_FLOW_ID = " + squidFlowId;
//        String deleteAnnotationSquidSql = "delete from ds_squid where SQUID_FLOW_ID = " + squidFlowId;
//        adapter.execute(deleteAnnotationSquidSql);

        //删除HDFS落地Squid与Column
        String deleteHDFSSquidColumnSql = "delete c from ds_dest_hdfs_column c inner join ds_squid s on s.ID = c.SQUID_ID where s.SQUID_FLOW_ID = " + squidFlowId;
        adapter.execute(deleteHDFSSquidColumnSql);

        //删除Impala落地Squid与Column
        String deleteImpalaSquidColumnSql = "delete c from ds_dest_impala_column c inner join ds_squid s on s.ID = c.SQUID_ID where s.SQUID_FLOW_ID = " + squidFlowId;
        adapter.execute(deleteImpalaSquidColumnSql);


        //删除Join
        String deleteJoinSql = "delete j from ds_join j inner join ds_squid s on j.JOINED_SQUID_ID =  s.ID where s.SQUID_FLOW_ID = " + squidFlowId;
        adapter.execute(deleteJoinSql);

        //删除SQL链接
//        String deleteSQLConnectionSquidSql = "delete c from ds_sql_connection c inner join ds_squid s on s.ID = c.ID where s.SQUID_FLOW_ID = " + squidFlowId;
//        adapter.execute(deleteSQLConnectionSquidSql);

        //删除userdefined
        String deleteUserDefinedSquidSql = "delete c from ds_userdefined_datamap_column c inner join ds_squid s on s.ID = c.squid_id where s.SQUID_FLOW_ID = " + squidFlowId;
        adapter.execute(deleteUserDefinedSquidSql);
        deleteUserDefinedSquidSql = "delete c from ds_userdefined_parameters_column c inner join ds_squid s on s.ID = c.squid_id where s.SQUID_FLOW_ID = " + squidFlowId;
        adapter.execute(deleteUserDefinedSquidSql);

        //删除dest_hive
        String deleteHiveSquidSql = "delete c from ds_dest_hive_column c inner join ds_squid s on s.ID = c.squid_id where s.SQUID_FLOW_ID = " + squidFlowId;
        adapter.execute(deleteHiveSquidSql);


        //删除dest_Cassandra

        String deleteCassandraSquidSql = "delete c from ds_dest_Cassandra_column c inner join ds_squid s on s.ID = c.squid_id where s.SQUID_FLOW_ID = " + squidFlowId;
        adapter.execute(deleteCassandraSquidSql);

        //删除statistic

        String deletStatisticSql = "delete c from ds_statistics_datamap_column c inner join ds_squid s on s.ID = c.squid_id where s.SQUID_FLOW_ID = " + squidFlowId;
        adapter.execute(deletStatisticSql);
        deletStatisticSql = "delete c from ds_statistics_parameters_column c inner join ds_squid s on s.ID = c.squid_id where s.SQUID_FLOW_ID = " + squidFlowId;
        adapter.execute(deletStatisticSql);

        //删除Squid link
        String deleteSquidLinkSql = "delete l from ds_squid_link l inner join ds_squid s on s.ID = l.FROM_SQUID_ID where s.SQUID_FLOW_ID = " + squidFlowId;
        adapter.execute(deleteSquidLinkSql);


        //删除Squid父类表
        String deleteSquidSql = "delete from ds_squid where SQUID_FLOW_ID = " + squidFlowId;
        adapter.execute(deleteSquidSql);
        if (tag == 1) {
            //删除ds_sys_squid_flow_status
            String deleteFlowStatusSql = "delete from ds_sys_squid_flow_status where SQUID_FLOW_ID = " + squidFlowId;
            adapter.execute(deleteFlowStatusSql);
        }
        // 删除squidflow
        if (!isDeleteAllSquidInSquidflow) {
            paramMap.clear();
            paramMap.put("id", String.valueOf(squidFlowId));
            flag = adapter.delete(paramMap, SquidFlow.class) > 0 ? true : false;
        } else {
            flag = true;
        }
        logger.info("删除Squidflow完毕");
        return flag;
    }

    /**
     * 删除project
     *
     * @param adapter
     * @param projectId
     * @return
     * @throws Exception
     */
    public boolean deleteProject(IRelationalDataManager adapter, int projectId, int repositoryId, ReturnValue out) throws Exception {
        boolean delete = true;
        Map<String, String> paramMap = new HashMap<String, String>();
        boolean refreshFlag = false;//squidflow状态表是否有非normal数据
        paramMap.put("project_id", String.valueOf(projectId));
        List<SquidFlow> squidFlows = adapter.query2List(true, paramMap, SquidFlow.class);
        if (null != squidFlows && squidFlows.size() > 0) {
            for (SquidFlow squidFlow : squidFlows) {
                delete = this.deleteSquidFlow(adapter, squidFlow.getId(), repositoryId, false, out, 1);
                if (60002 == out.getMessageCode().value()) {
                    refreshFlag = true;
                }
                if (!delete) {
                    break;
                }
            }
        }
        if (delete && refreshFlag) {
            out.setMessageCode(MessageCode.WARN_DELETEPROJECT);
            return true;
        } else if (delete && !refreshFlag) {
            // 删除project
            paramMap.clear();
            paramMap.put("id", String.valueOf(projectId));
            boolean flag = adapter.delete(paramMap, Project.class) >= 0 ? true : false;
            if (flag) {
                paramMap.clear();
                paramMap.put("project_id", String.valueOf(projectId));
                adapter.delete(paramMap, DSVariable.class);
            }
            return flag;
        }
        return delete;
    }

    /**
     * 删除url列表
     *
     * @param adapter
     * @param id
     * @return
     * @throws SQLException
     */
    private boolean deleteWebUrl(IRelationalDataManager adapter, int id) throws SQLException {
        Map<String, String> paramMap = new HashMap<String, String>();
        paramMap.put("id", String.valueOf(id));
        Url url = adapter.query2Object2(true, paramMap, Url.class);
        CommonConsts.addValidationTask(new SquidValidationTask(TokenUtil.getToken(), adapter, MessageBubbleService.setMessageBubble(url.getSquid_id(), url.getSquid_id(), null, MessageBubbleCode.WARN_WEB_CONNECTION_NO_URL_LIST.value())));
        return adapter.delete(paramMap, Url.class) > 0 ? true : false;
    }

    //根据link中的transid 进行重置和删除功能。
    public void resetTransformationInput(IRelationalDataManager adapter3, int from_transformation_id, int to_transformation_id) throws SQLException {
        resetTransformationInput(adapter3, from_transformation_id, to_transformation_id, null);
    }

    public void resetTransformationInput(IRelationalDataManager adapter3, int from_transformation_id, int to_transformation_id, Map<String, Integer> indexMap) throws SQLException {
        resetTransformationInput(adapter3, from_transformation_id, to_transformation_id, null, null, null);
    }

    public void resetTransformationInput(IRelationalDataManager adapter3, int from_transformation_id,
                                         int to_transformation_id, Map<String, Integer> indexMap,
                                         List<TransformationInputs> updateInputs,
                                         List<TransformationInputs> deleteInputs) throws SQLException {
        Map<String, String> paramMap = new HashMap<String, String>();
        paramMap.put("transformation_id", String.valueOf(to_transformation_id));
        paramMap.put("source_transform_id", String.valueOf(from_transformation_id));
        List<TransformationInputs> transInputs = adapter3.query2List(true, paramMap, TransformationInputs.class);
        if (null != transInputs && transInputs.size() > 0) {
            boolean isUpdate = false;
            StringBuffer sql = new StringBuffer();
            sql.append("select max(dtid.input_order) as CNT,dtt.id as transType from ds_tran_input_definition dtid ");
            sql.append("inner join ds_transformation_type dtt on dtid.code=dtt.code ");
            sql.append("inner join ds_transformation dt on dtt.id=dt.transformation_type_id ");
            sql.append(" where dtid.input_order not in (-1) and dt.id=" + to_transformation_id);
            Map<String, Object> map = adapter3.query2Object(true,
                    sql.toString(), null);
            //验证，是否还有空余的链接数
            int cnt = 0;
            if (map != null && map.containsKey("CNT") &&
                    ValidateUtils.isNumeric(map.get("CNT") + "")) {
                cnt = Integer.parseInt(map.get("CNT") + "") + 1;
                if (cnt < 9999) {
                    isUpdate = true;
                }
            }
            for (TransformationInputs transformationInput : transInputs) {
                if (indexMap != null) {
                    indexMap.put("index", transformationInput.getSource_tran_output_index());
                }
                if (isUpdate) {
                    transformationInput.setIn_condition("");
                    transformationInput.setSource_tran_output_index(0);
                    transformationInput.setSource_transform_id(0);
                    String getSquidTypeId = "select squid_type_id as squidType from ds_squid ds inner join ds_transformation dtf on dtf.squid_id = ds.id inner join ds_tran_inputs dti  on dti.transformation_id = dtf.id where dti.transformation_id=" + transformationInput.getTransformationId();
                    Map<String, Object> squidTypeMap = adapter3.query2Object(true, getSquidTypeId, null);
                    if (map.containsKey("TRANSTYPE") && (Integer)map.get("TRANSTYPE") == TransformationTypeEnum.TRAIN.value()
                            && squidTypeMap != null && squidTypeMap.containsKey("SQUIDTYPE") && (Integer)squidTypeMap.get("SQUIDTYPE") == SquidTypeEnum.QUANTIFY.value()) {
                        transformationInput.setInput_Data_Type(SystemDatatype.OBJECT.value());
                    }
                    adapter3.update2(transformationInput);
                    if (updateInputs != null) {
                        updateInputs.add(transformationInput);
                    }
                } else {
                    paramMap.clear();
                    paramMap.put("id", String.valueOf(transformationInput.getId()));
                    adapter3.delete(paramMap, TransformationInputs.class);
                    if (deleteInputs != null) {
                        deleteInputs.add(transformationInput);
                    }
                    String sqlStr = "select * from ds_tran_inputs where transformation_id=" + transformationInput.getTransformationId() + " order by relative_order";
                    List<TransformationInputs> inputs = adapter3.query2List(true, sqlStr, null, TransformationInputs.class);
                    if (inputs != null && inputs.size() > 0) {
                        int i = 0;
                        for (TransformationInputs transformationInputs : inputs) {
                            transformationInputs.setRelative_order(i);
                            adapter3.update2(transformationInputs);
                            i++;
                        }
                    }
                }

                paramMap.clear();
                paramMap.put("id", String.valueOf(to_transformation_id));
                Transformation toTrans = adapter3.query2Object2(true, paramMap, Transformation.class);
                if (toTrans != null) {
                    int typeid = toTrans.getTranstype();
                    if (typeid == TransformationTypeEnum.CHOICE.value()) {
                        String sqlstr = "select * from ds_transformation_link " +
                                "where to_transformation_id=" + transformationInput.getTransformationId();
                        List<TransformationLink> links = adapter3.query2List(true, sqlstr, null, TransformationLink.class);
                        if (links == null || links.size() == 0) {
                            paramMap.clear();
                            paramMap.put("id", String.valueOf(transformationInput.getTransformationId()));
                            Transformation trans = adapter3.query2Object2(true, paramMap, Transformation.class);
                            trans.setOutput_data_type(SystemDatatype.OBJECT.value());
                            adapter3.update2(trans);
                            String toLink = "select * from ds_transformation_link " +
                                    "where from_transformation_id=" + transformationInput.getTransformationId();
                            List<TransformationLink> ToLinks = adapter3.query2List(true, toLink, null, TransformationLink.class);
                            if(ToLinks!=null && ToLinks.size()>0){
                                for(TransformationLink link:ToLinks){
                                    try{
                                        this.deleteTransformationLink(adapter3, link.getId());
                                    }catch (Exception e){
                                       e.printStackTrace();
                                    }
                                }
                            }

                        }
                    }
                }
            }
        }
    }

    /**
     * deleteSourceColumn(这里用一句话描述这个方法的作用)
     * TODO(这里描述这个方法适用条件 – 可选)
     *
     * @param adapter3
     * @param out
     * @return Boolean 返回类型
     * @throws Exception
     * @throws
     * @Title: deleteSourceColumn
     * @Description: TODO
     * @author bo.dang
     */
    public Boolean deleteSourceColumnList(IRelationalDataManager adapter3, int hostSquidId, int referenceSquidId, int sourceTableId, ReturnValue out) throws Exception {
/*		// 删除SourceColumn的信息
        paramMap.clear();
        paramMap.put("id", Integer.toString(columnId, 10));
        paramMap.put("source_table_id",Integer.toString(sourceTableId, 10));
        adapter3.delete(paramMap, SourceColumn.class);
		// 删除ReferenceColumn的信息
        paramMap.clear();
        paramMap.put("column_id", Integer.toString(columnId, 10));
        paramMap.put("reference_squid_id", Integer.toString(squidId, 10));
        adapter3.delete(paramMap, ReferenceColumn.class);
        paramMap.clear();
        paramMap.put("squid_id", Integer.toString(squidId, 10));
        paramMap.put("column_id", Integer.toString(columnId, 10));
		Transformation transformation = adapter3.query2Object(paramMap, Transformation.class);
		// 删除Transformation
		deleteTransformation(adapter3, transformation.getId(), 0);
		// 删除Column
		delColumn(adapter3, 0, squidId, out);*/
        Boolean flag = false;
        // 删除SourceColumn的信息
        Map<String, String> paramMap = new HashMap<String, String>();
        paramMap.clear();
        paramMap.put("source_table_id", Integer.toString(sourceTableId, 10));
        flag = adapter3.delete(paramMap, SourceColumn.class) > 0 ? true : false;
        Map<String, Object> groupIdMap = adapter3.query2Object(true, "SELECT distinct group_id FROM DS_REFERENCE_COLUMN WHERE "
                + "host_squid_id=" + hostSquidId + " AND reference_squid_id=" + referenceSquidId, null);
        int groupId = 0;
        if (StringUtils.isNotNull(groupIdMap) && !groupIdMap.isEmpty()) {
            groupId = Integer.parseInt(groupIdMap.get("GROUP_ID").toString(), 10);
        }

        // 删除ReferenceColumn的信息
        flag = deleteReferenceColumnAndGroup(adapter3, groupId, referenceSquidId, out);
        //flag = this.deleteColumn(adapter, 0, squidId);
        // 删除Column
        flag = delColumn(adapter3, referenceSquidId, out);
        // 删除Transformation
        flag = deleteTransformation(adapter3, 0, referenceSquidId);
        return flag;
    }


















    public Boolean hBasedeleteSourceColumnList(IRelationalDataManager adapter3, int hostSquidId, int referenceSquidId, int sourceTableId, ReturnValue out) throws Exception {
/*		// 删除SourceColumn的信息
        paramMap.clear();
        paramMap.put("id", Integer.toString(columnId, 10));
        paramMap.put("source_table_id",Integer.toString(sourceTableId, 10));
        adapter3.delete(paramMap, SourceColumn.class);
		// 删除ReferenceColumn的信息
        paramMap.clear();
        paramMap.put("column_id", Integer.toString(columnId, 10));
        paramMap.put("reference_squid_id", Integer.toString(squidId, 10));
        adapter3.delete(paramMap, ReferenceColumn.class);
        paramMap.clear();
        paramMap.put("squid_id", Integer.toString(squidId, 10));
        paramMap.put("column_id", Integer.toString(columnId, 10));
		Transformation transformation = adapter3.query2Object(paramMap, Transformation.class);
		// 删除Transformation
		deleteTransformation(adapter3, transformation.getId(), 0);
		// 删除Column
		delColumn(adapter3, 0, squidId, out);*/
        Boolean flag = false;
        /*// 不删除SourceColumn的信息，就用以前的信息创建referenceColumn
        Map<String, String> paramMap = new HashMap<String, String>();
        paramMap.clear();
        paramMap.put("source_table_id", Integer.toString(sourceTableId, 10));
        flag = adapter3.delete(paramMap, SourceColumn.class) > 0 ? true : false;*/
        Map<String, Object> groupIdMap = adapter3.query2Object(true, "SELECT distinct group_id FROM DS_REFERENCE_COLUMN WHERE "
                + "host_squid_id=" + hostSquidId + " AND reference_squid_id=" + referenceSquidId, null);
        int groupId = 0;
        if (StringUtils.isNotNull(groupIdMap) && !groupIdMap.isEmpty()) {
            groupId = Integer.parseInt(groupIdMap.get("GROUP_ID").toString(), 10);
        }

        // 删除ReferenceColumn的信息
        flag = deleteReferenceColumnAndGroup(adapter3, groupId, referenceSquidId, out);
        //flag = this.deleteColumn(adapter, 0, squidId);
        // 删除Column
        flag = delColumn(adapter3, referenceSquidId, out);
        // 删除Transformation
        flag = deleteTransformation(adapter3, 0, referenceSquidId);
        return flag;
    }
}
