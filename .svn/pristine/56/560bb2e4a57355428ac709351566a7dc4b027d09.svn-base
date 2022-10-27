package com.eurlanda.datashire.sprint7.service.squidflow;

import com.eurlanda.datashire.adapter.DataAdapterFactory;
import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.entity.HdfsSquid;
import com.eurlanda.datashire.entity.SourceTable;
import com.eurlanda.datashire.entity.SquidLink;
import com.eurlanda.datashire.enumeration.DSObjectType;
import com.eurlanda.datashire.enumeration.SquidTypeEnum;
import com.eurlanda.datashire.server.dao.RepositoryDao;
import com.eurlanda.datashire.server.model.Repository;
import com.eurlanda.datashire.server.utils.TokenUtil;
import com.eurlanda.datashire.sprint7.packet.InfoMessagePacket;
import com.eurlanda.datashire.sprint7.packet.InfoNewMessagePacket;
import com.eurlanda.datashire.sprint7.service.squidflow.subservice.ExtractServicesub;
import com.eurlanda.datashire.sprint7.service.workspace.RepositoryProcess;
import com.eurlanda.datashire.utility.*;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Service
public class RepositoryService implements IRepositoryService {
    @Autowired
    private RepositoryDao repositoryDao;
    private static Logger logger = Logger.getLogger(RepositoryService.class);// 记录日志
    private String token;//令牌根据令牌得到相应的连接信息
    private String key;//key值

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

    //异常处理机制
    ReturnValue out = null;
    /**
     * 根据squidflowId获取repositoryId
     * @param squidFlowId
     * @return
     */
    public Integer getRepositoryIdBySquidFlowId(int squidFlowId){
        Repository repository = repositoryDao.getRepositoryBySquidFlowId(squidFlowId);
        int repositoryId = 0;
        if(repository!=null){
            repositoryId = repository.getId();
        }
        return repositoryId;
    }

    public Integer getRepositoryIdBySquidFlowIdWithAutoware(int squidFlowId){
        IRelationalDataManager adapter3 = null;
        int repositoryId = 0;
        try{
            adapter3 = DataAdapterFactory.getDefaultDataManager();
            adapter3.openSession();
            String sql = "select dsr.* from ds_sys_repository dsr,ds_project dp,ds_squid_flow dsf where dsr.id = dp.REPOSITORY_ID and dp.id = dsf.project_id  and dsf.id=?";
            List<String> params = new ArrayList<>();
            params.add(squidFlowId+"");
            List<Repository> repositorys = adapter3.query2List(true,sql,params,Repository.class);
            if(repositorys!=null && repositorys.size()>0){
                repositoryId = repositorys.get(0).getId();
            }
        }catch(Exception e){
            e.printStackTrace();
        }finally {
            adapter3.closeSession();
        }
        return repositoryId;
    }

    /**
     * 根据repositoryId获取到数据库连接信息(确定某个repository使用的是哪一台机器)
     * @param repositoryId
     * @return
     */
    public String getDbUrlByRepositoryId(int repositoryId){
        String dbUrl = "";
        int p_num = repositoryId % Integer.parseInt(SysConf.getValue("cloud_db_num"));
        dbUrl = SysConf.getValue("cloud_db_ip_port"+p_num);
        return dbUrl;
    }
    /**
     * 根据squidFlowId获取到cloudDb的数据库连接信息
     * @param squidFlowId
     * @return
     */
    public String getDBUrlBySquidFlowId(int squidFlowId){
        int repositoryId = getRepositoryIdBySquidFlowId(squidFlowId);
        return getDbUrlByRepositoryId(repositoryId);
    }
    public String getDBUrlBySquidFlowIdWithAutoware(int squidFlowId){
        int repositoryId = getRepositoryIdBySquidFlowIdWithAutoware(squidFlowId);
        if(repositoryId>0){
            return getDbUrlByRepositoryId(repositoryId);
        }
       return "";
    }

    @Override
    public String deleteSquidLink(String info) {
        LinkProcess linkProcess = new LinkProcess(TokenUtil.getToken());
        out = new ReturnValue();
        Map<String, Object> output = linkProcess.deleteSquidLink(info, out);
        return infoNewMessagePacket(output,
                DSObjectType.SQUIDLINK, out);
    }

    @Override
    public String createJoin(String info) {
        SquidJoinProcess joinProcess = new SquidJoinProcess(TokenUtil.getToken());
        out = new ReturnValue();
        //调用SquidJoin新增处理类
        Map<String, Object> outputMap = joinProcess.createJoin(info,
                out);
        return infoNewMessagePacket(outputMap, DSObjectType.SQUIDJOIN, out);
    }

    @Override
    public String deleteTransGroup(String info) {
        TransformationGroupProcess groupProcess = new TransformationGroupProcess(TokenUtil.getToken());
        //调用TransGroup删除处理类
        out = new ReturnValue();
        Map<String, Object> outputMap = groupProcess.deleteTransGroup(info, out);
        return infoNewMessagePacket(outputMap, DSObjectType.GROUP, out);
    }

    @Override
    public String updateTransGroupOrder(String info) {
        TransformationGroupProcess groupProcess = new TransformationGroupProcess(TokenUtil.getToken());
        //调用TransGroup更新group排序
        out = new ReturnValue();
        Map<String, Object> outputMap = groupProcess.updateTransGroupOrder(info, out);
        return infoNewMessagePacket(outputMap, DSObjectType.GROUP, out);
    }

    @Override
    public String createExtract2StageLink(String info) {
        SquidLink squidLink = JsonUtil.object2HashMap(info, SquidLink.class);
        LinkProcess link = new LinkProcess(TokenUtil.getToken());
        out = new ReturnValue();
        Map<String, Object> squidLinkMap = link.link2ExtractStage(squidLink, out);
        return infoNewMessagePacket(squidLinkMap, DSObjectType.SQUIDLINK, out);
    }

    @Override
    public String deleteSquidJoin(String info) {
        SquidJoinProcess joinProcess = new SquidJoinProcess(TokenUtil.getToken());
        out = new ReturnValue();
        Map<String, Object> map = joinProcess.deleteSquidJoin(info);
        return infoNewMessagePacket(map, DSObjectType.SQUIDJOIN, out);
    }

    @Override
    public String updateSquidJoin(String info) {
        SquidJoinProcess process = new SquidJoinProcess(TokenUtil.getToken());
        out = new ReturnValue();
        Map<String, Object> map = process.updateSquidJoin(info, out);
        return infoNewMessagePacket(map, DSObjectType.SQUIDJOIN, out);
    }

    @Override
    public String updateJoinOrder(String info) {
        SquidJoinProcess process = new SquidJoinProcess(TokenUtil.getToken());
        out = new ReturnValue();
        Map<String, Object> map = process.updateJoinOrder(info, out);
        return infoNewMessagePacket(map, DSObjectType.SQUIDJOIN, out);
    }

    @Override
    public String getTemplateDataByTypes(String info) {
        TemplateDataProcess template = new TemplateDataProcess();
        out = new ReturnValue();
        Map<String, Object> map = template.getTemplateDataByTypes(info, out);
        return infoNewMessagePacket(map, DSObjectType.TEMPLATE, out);
    }

    @Override
    public String getAllTemplateDataTypes(String info) {
        TemplateDataProcess template = new TemplateDataProcess();
        out = new ReturnValue();
        Map<String, Object> map = template.getAllTemplateDataTypes(out);
        return infoNewMessagePacket(map, DSObjectType.TEMPLATE, out);
    }

    @Override
    public String deleteJoins(String info) {
        out = new ReturnValue();
        SquidJoinProcess process = new SquidJoinProcess(TokenUtil.getToken());
        Map<String, Object> map = process.deleteJoins(info, out);
        return infoNewMessagePacket(map, DSObjectType.SQUIDJOIN, out);
    }

    @Override
    public String upDateJoinForTypeCond(String info) {
        out = new ReturnValue();
        SquidJoinProcess process = new SquidJoinProcess(TokenUtil.getToken());
        Map<String, Object> map = process.updateJoinForTypeCond(info, out);
        return infoNewMessagePacket(map, DSObjectType.SQUIDJOIN, out);
    }

    /**
     * 获取某trans的所有inputs集合
     *
     * @param info
     * @return
     */
    public String getTransformationInputById(String info) {
        out = new ReturnValue();
        TransformationService service = new TransformationService(TokenUtil.getToken());
        Map<String, Object> map = service.getTransformationInputById(info, out);
        return infoNewMessagePacket(map, DSObjectType.TRANSFORMATIONINPUTS, out);
    }

    /**
     * 更新trans及transformation
     *
     * @param info
     * @return
     */
    public String updTransformation(String info) {
        out = new ReturnValue();
        TransformationService service = new TransformationService(TokenUtil.getToken());
        Map<String, Object> map = service.updTransformation(info, out);
        return infoNewMessagePacket(map, DSObjectType.TRANSFORMATIONINPUTS, out);
    }

    /**
     * 更新trans_inputs集合
     *
     * @param info
     * @return
     */
    public String updTransformInputs(String info) {
        out = new ReturnValue();
        TransformationService service = new TransformationService(TokenUtil.getToken());
        Map<String, Object> map = service.updTransformInputs(info, out);
        return infoNewMessagePacket(map, DSObjectType.TRANSFORMATIONINPUTS, out);
    }

    /**
     * 创建新的trans对象，根据类型不同 初始化的Inputs集合也不同
     *
     * @param info
     * @return
     */
    public String createTransAndInputs(String info) {
        out = new ReturnValue();
        TransformationService service = new TransformationService(TokenUtil.getToken());
        Map<String, Object> map = service.createTransAndInputs(info, out);
        return infoNewMessagePacket(map, DSObjectType.TRANSFORMATIONINPUTS, out);
    }

    /**
     * 拖动transformation打断transformationlink业务处理类  新接口（需要判断是否初始化input记录）
     *
     * @param info
     * @return
     */
    public String drapTrans2InputsAndLink(String info) {
        out = new ReturnValue();
        TransformationService service = new TransformationService(TokenUtil.getToken());
        Map<String, Object> map = service.drapTrans2InputsAndLink(info, out);
        return infoNewMessagePacket(map, DSObjectType.TRANSFORMATIONINPUTS, out);
    }

    /**
     * 更改SquidFlow的名称
     *
     * @param info
     * @return
     */
    public String updSquidFlow(String info) {
        out = new ReturnValue();
        SquidFlowService service = new SquidFlowService(TokenUtil.getToken());
        Map<String, Object> map = service.updSquidFlow(info, out);
        return infoNewMessagePacket(map, DSObjectType.SQUID_FLOW, out);
    }

    /**
     * 更改Project的名称
     *
     * @param info
     * @return
     */
    public String updProject(String info) {
        out = new ReturnValue();
        ProjectService service = new ProjectService(TokenUtil.getToken());
        Map<String, Object> map = service.updProject(info, out);
        return infoNewMessagePacket(map, DSObjectType.PROJECT, out);
    }

    /**
     * 创建ExceptionSquid
     *
     * @param info
     * @return
     */
    public String createExceptionSquid(String info) {
        out = new ReturnValue();
        ExceptionSquidProcess service = new ExceptionSquidProcess(TokenUtil.getToken());
        Map<String, Object> map = service.createExceptionSquid(info, out);
        return infoNewMessagePacket(map, DSObjectType.EXCEPTION, out);
    }

    /**
     * 更新ExceptionSquid的基本属性
     *
     * @param info
     * @return
     */
    public String updExceptionSquid(String info) {
        out = new ReturnValue();
        ExceptionSquidProcess service = new ExceptionSquidProcess(TokenUtil.getToken());
        Map<String, Object> map = service.updExceptionSquid(info, out);
        return infoNewMessagePacket(map, DSObjectType.EXCEPTION, out);
    }

    /**
     * 获取某一个ProjectId下的所有的DataMining（下游除外）
     *
     * @param info
     * @return
     */
    public String getAllDataMiningSquidInRepository(String info) {
        out = new ReturnValue();
        SquidProcess process = new SquidProcess(TokenUtil.getToken());
        Map<String, Object> map = process.getAllDataMiningSquidInRepository(info, out);
        return infoNewMessagePacket(map, DSObjectType.DATAMINING, out);
    }

    /**
     * 云端接口,获取project下面的模型(公有数猎场获取当前project下面，私有数猎场获取自己拥有的project下面)
     */
    public String getAllDataMiningSquidInCloud(String info) {
        out = new ReturnValue();
        SquidProcess process = new SquidProcess(TokenUtil.getToken());
        Map<String, Object> map = process.getAllDataMiningSquidInCloud(info, out);
        return infoNewMessagePacket(map, DSObjectType.DATAMINING, out);
    }

    /**
     * 获取某一个Repository下的所有的Quantify （下游除外）
     *
     * @param info
     * @return
     */
    public String getAllQuantifySquidInRepository(String info) {
        out = new ReturnValue();
        SquidProcess process = new SquidProcess(TokenUtil.getToken());
        Map<String, Object> map = process.getAllQuantifySquidInRepository(info, out);
        return infoNewMessagePacket(map, DSObjectType.DATAMINING, out);
    }

    /**
     * 获取dataming版本信息
     *
     * @param info
     * @return
     */
    public String getDataMiningVersions(String info) {
        out = new ReturnValue();
        SquidProcess process = new SquidProcess(TokenUtil.getToken());
        Map<String, Object> map = process.getDataMiningVersions(info, out);
        return infoNewMessagePacket(map, DSObjectType.DATAMINING, out);
    }

    /**
     * 校验SquidJoin的 condition
     *
     * @param info
     * @return
     */
    public String getSquidJoinValidator(String info) {
        out = new ReturnValue();
        JoinProcess process = new JoinProcess(TokenUtil.getToken(), TokenUtil.getKey());
        Map<String, Object> map = process.getSquidJoinValidator(info, out);
        return null;
    }

    /**
     * 更新由cdc属性的设置（独立）
     *
     * @param info
     * @return
     */
    public String manageDataSquidByCDC(String info) {
        out = new ReturnValue();
        SquidProcess process = new SquidProcess(TokenUtil.getToken());
        Map<String, Object> map = process.manageDataSquidByCDC(info, out);
        return infoNewMessagePacket(map, DSObjectType.DATAMINING, out);
    }

    /**
     * 搜索Repository下的指定关键字
     *
     * @param info
     * @return
     */
    public String findObjectsForParams(String info) {
        out = new ReturnValue();
        RepositoryProcess process = new RepositoryProcess(TokenUtil.getToken(), TokenUtil.getKey());
        Map<String, Object> map = process.findObjectsForParams(info, out);
        return infoNewMessagePacket(map, DSObjectType.DATAMINING, out);
    }

    /**
     * 打开搜索结果集中的某条记录，同时刷新project信息，当搜索为squidflow时，squidflow打开参数传空集合
     *
     * @param info
     * @return
     */
    public String openSubitemsOfRepository(String info) {
        out = new ReturnValue();
        RepositoryProcess process = new RepositoryProcess(TokenUtil.getToken(), TokenUtil.getKey());
        Map<String, Object> map = process.openSubitemsOfRepository(info, out);
        return infoNewMessagePacket(map, DSObjectType.DATAMINING, out);
    }

    /**
     * 打开搜索结果集中的某条记录，同时刷新squidflow信息
     *
     * @param info
     * @return
     */
    public String openSubitemsOfProject(String info) {
        out = new ReturnValue();
        RepositoryProcess process = new RepositoryProcess(TokenUtil.getToken(), TokenUtil.getKey());
        Map<String, Object> map = process.openSubitemsOfProject(info, out);
        return infoNewMessagePacket(map, DSObjectType.DATAMINING, out);
    }

    /**
     * 批量设置dataSquid落地属性
     *
     * @param info
     * @return
     */
    public String setSquidsPersistence(String info) {
        out = new ReturnValue();
        SquidProcess process = new SquidProcess(TokenUtil.getToken());
        Map<String, Object> map = process.setSquidsPersistence(info, out);
        return infoNewMessagePacket(map, DSObjectType.SQUID, out);
    }

    /**
     * 批量设置dataSquid落地目标
     */
    public String setSquidsDest(String info) {
        out = new ReturnValue();
        SquidProcess process = new SquidProcess(TokenUtil.getToken());
        Map<String, Object> map = process.setSquidsDest(info, out);
        return infoNewMessagePacket(map, DSObjectType.SQUID, out);
    }

    /**
     * 批量取消dataSquid落地属性（需要考虑cdc的处理）
     *
     * @param info
     * @return
     */
    public String cancelSquidsPersistence(String info) {
        out = new ReturnValue();
        SquidProcess process = new SquidProcess(TokenUtil.getToken());
        Map<String, Object> map = process.cancelSquidsPersistence(info, out);
        return infoNewMessagePacket(map, DSObjectType.SQUID, out);
    }

    /**
     * DocExtract 创建新的ReferenceColumn(String info)
     *
     * @param info
     * @return
     */
    public String createReferenceColumn(String info) {
        out = new ReturnValue();
        RefereceColumnProcess process = new RefereceColumnProcess(TokenUtil.getToken());
        Map<String, Object> map = process.createReferenceColumn(info, out);
        return infoNewMessagePacket(map, DSObjectType.COLUMN, out);
    }

    /**
     * DocExtract 修改ReferenceColumn
     *
     * @param info
     * @return
     */
    public String updateReferenceColumn(String info) {
        out = new ReturnValue();
        RefereceColumnProcess process = new RefereceColumnProcess(TokenUtil.getToken());
        Map<String, Object> map = process.updateReferenceColumn(info, out);
        return infoNewMessagePacket(map, DSObjectType.COLUMN, out);
    }

    /**
     * DocExtract 删除ReferenceColumn
     *
     * @param info
     * @return
     */
    public String deleteReferenceColumn(String info) {
        out = new ReturnValue();
        RefereceColumnProcess process = new RefereceColumnProcess(TokenUtil.getToken());
        Map<String, Object> map = process.deleteReferenceColumn(info, out);
        return infoNewMessagePacket(map, DSObjectType.COLUMN, out);
    }

    /**
     * DocExtract 修改ReferenceColumn的排序
     *
     * @param info
     * @return
     */
    public String updateRefColumnForOrder(String info) {
        out = new ReturnValue();
        RefereceColumnProcess process = new RefereceColumnProcess(TokenUtil.getToken());
        Map<String, Object> map = process.updateRefColumnForOrder(info, out);
        return infoNewMessagePacket(map, DSObjectType.COLUMN, out);
    }

    /**
     * 单对象转换成Json格式
     * 作用描述：
     * 修改说明：
     *
     * @param <T>
     * @return
     */
    private <T> String infoNewMessagePacket(T object, DSObjectType type, ReturnValue out) {
        return JsonUtil.toJsonString(object, type, out.getMessageCode());
    }

    /**
     * 创建FileFloderSquid
     *
     * @param info
     * @return
     * @author lei.bin
     */
    public String createFileFolderSquid(String info) {
        out = new ReturnValue();
        SquidProcess squidProcess = new SquidProcess(TokenUtil.getToken());
        Map<String, Object> map = squidProcess.createFileFloderSquid(info, out);
        return infoNewMessagePacket(map, DSObjectType.DBSOURCE, out);
    }

    /**
     * 创建ftpsuqid
     *
     * @param info
     * @return
     */
    public String createFtpSquid(String info) {
        out = new ReturnValue();
        SquidProcess squidProcess = new SquidProcess(TokenUtil.getToken());
        Map<String, Object> map = squidProcess.createFtpSquid(info, out);
        return infoNewMessagePacket(map, DSObjectType.DBSOURCE, out);
    }

    /**
     * 创建hdfssquid
     *
     * @param info
     * @return
     */
    public String createHdfsSquid(String info) {
        out = new ReturnValue();
        SquidProcess squidProcess = new SquidProcess(TokenUtil.getToken());
        Map<String, Object> map = squidProcess.createHdfsSquid(info, out);
        return infoNewMessagePacket(map, DSObjectType.DBSOURCE, out);
    }

    /**
     * 创建weiboSquid
     *
     * @param info
     * @return
     */
    public String createWeiBoSquid(String info) {
        out = new ReturnValue();
        SquidProcess squidProcess = new SquidProcess(TokenUtil.getToken());
        Map<String, Object> map = squidProcess.createWeiboSquid(info, out);
        return infoNewMessagePacket(map, DSObjectType.DBSOURCE, out);
    }

    /**
     * 创建WebSquid
     *
     * @param info
     * @return
     */
    public String createWebSquid(String info) {
        out = new ReturnValue();
        SquidProcess squidProcess = new SquidProcess(TokenUtil.getToken());
        Map<String, Object> map = squidProcess.createWebSquid(info, out);
        return infoNewMessagePacket(map, DSObjectType.DBSOURCE, out);
    }

    /**
     * 更新FileSquid
     *
     * @param info
     * @return
     */
    public String updateFileFolderSquid(String info) {
        out = new ReturnValue();
        SquidProcess squidProcess = new SquidProcess(TokenUtil.getToken());
        Map<String, Object> map = squidProcess.updateFileSquid(info, out);
        return infoNewMessagePacket(map, DSObjectType.DBSOURCE, out);
    }

    /**
     * 更新FtpSquid
     *
     * @param info
     * @return
     */
    public String updateFtpSquid(String info) {
        out = new ReturnValue();
        SquidProcess squidProcess = new SquidProcess(TokenUtil.getToken());
        Map<String, Object> map = squidProcess.updateFtpSquid(info, out);
        return infoNewMessagePacket(map, DSObjectType.DBSOURCE, out);
    }

    /**
     * 更新HdfsSquid
     *
     * @param info
     * @return
     */
    public String updateHdfsSquid(String info) {
        out = new ReturnValue();
        SquidProcess squidProcess = new SquidProcess(TokenUtil.getToken());
        Map<String, Object> map = squidProcess.updateHdfsSquid(info, out);
        return infoNewMessagePacket(map, DSObjectType.DBSOURCE, out);
    }

    /**
     * 更新WeiboSquid
     *
     * @param info
     * @return
     */
    public String updateWeiBoSquid(String info) {
        out = new ReturnValue();
        SquidProcess squidProcess = new SquidProcess(TokenUtil.getToken());
        Map<String, Object> map = squidProcess.updateWeiboSquid(info, out);
        return infoNewMessagePacket(map, DSObjectType.DBSOURCE, out);
    }

    /**
     * 更新WebSquid
     *
     * @param info
     * @return
     */
    public String updateWebSquid(String info) {
        out = new ReturnValue();
        SquidProcess squidProcess = new SquidProcess(TokenUtil.getToken());
        Map<String, Object> map = squidProcess.updateWebSquid(info, out);
        return infoNewMessagePacket(map, DSObjectType.DBSOURCE, out);
    }

    /**
     * 根据连接信息获取file内容
     *
     * @param info
     * @return
     */
    public String connectFileFolderSquid(String info) {
        out = new ReturnValue();
        SquidProcess squidProcess = new SquidProcess(TokenUtil.getToken());
        Map<String, Object> map = squidProcess.connectFileFloderSquid(info, out);
        return infoNewMessagePacket(map, DSObjectType.LOCALFILE, out);
    }

    /**
     * 根据连接信息获取FTP内容
     *
     * @param info
     * @return
     */
    public String connectFtpSquid(String info) {
        out = new ReturnValue();
        SquidProcess squidProcess = new SquidProcess(TokenUtil.getToken());
        Map<String, Object> map = squidProcess.connectFtpSquid(info, out);
        List<SourceTable> list = (List<SourceTable>) map.get("SourceTables");
        return infoNewMessagePacket(map, DSObjectType.FTP, out);
    }

    /**
     * 根据连接信息获取hdfs内容
     *
     * @param info
     * @return
     */
    public String connectHdfsSquid(String info) {
        out = new ReturnValue();
        SquidProcess squidProcess = new SquidProcess(TokenUtil.getToken());
        Map<String, Object> map = squidProcess.connectHdfsSquid(info, out);
        return infoNewMessagePacket(map, DSObjectType.HDFS, out);
    }

    /**
     * 根据连接信息获取weibo内容
     *
     * @param info
     * @return
     */
    public String connectWeiBoSquid(String info) {
        out = new ReturnValue();
        SquidProcess squidProcess = new SquidProcess(TokenUtil.getToken());
        Map<String, Object> map = squidProcess.connectWeiboSquid(info, out);
        return infoNewMessagePacket(map, DSObjectType.WEIBO, out);
    }

    /**
     * 根据连接信息获取web内容
     *
     * @param info
     * @return
     */
    public String connectWebSquid(String info) {
        out = new ReturnValue();
        SquidProcess squidProcess = new SquidProcess(TokenUtil.getToken());
        Map<String, Object> map = squidProcess.connectWebSquid(info, out);
        return infoNewMessagePacket(map, DSObjectType.WEB, out);
    }

    /**
     * 查看File文件数据
     *
     * @param info
     * @return
     */
    public String viewFileData(String info) {

        out = new ReturnValue();
        SquidProcess squidProcess = new SquidProcess(TokenUtil.getToken());
        Map<String, Object> map = squidProcess.viewFileData(info, out);
        return infoNewMessagePacket(map, DSObjectType.FILEFOLDER, out);
    }

    /**
     * 查看Ftp文件数据
     *
     * @param info
     * @return
     */
    public String viewFtpData(String info) {
        out = new ReturnValue();
        SquidProcess squidProcess = new SquidProcess(TokenUtil.getToken());
        Map<String, Object> map = squidProcess.viewFtpData(info, out);
        return infoNewMessagePacket(map, DSObjectType.FTP, out);
    }

    /**
     * 查看HDFS文件数据
     *
     * @param info
     * @return
     */
    public String viewHdfsData(String info) {
        Map<String, Object> resultMap = JsonUtil.toHashMap(info);//"FileSquid""FileName"
        HdfsSquid hdfsSquid = JsonUtil.toGsonBean(resultMap.get("HdfsSquid").toString(), HdfsSquid.class);
        DSObjectType type = null;
        if (hdfsSquid.getSquid_type() == SquidTypeEnum.SOURCECLOUDFILE.value()) {
            type = DSObjectType.SOURCECLOUDFILE;
        } else if(hdfsSquid.getSquid_type() == SquidTypeEnum.TRAINNINGFILESQUID.value()){
            type = DSObjectType.TRAINNINGFILESQUID;
        } else {
            type = DSObjectType.HDFS;
        }
        out = new ReturnValue();
        SquidProcess squidProcess = new SquidProcess(TokenUtil.getToken());
        InfoMessagePacket<String> listMessage = new InfoMessagePacket<String>();
        Map<String, Object> map = squidProcess.viewHdfsData(info, out);
        return infoNewMessagePacket(map, type, out);
    }

    /**
     * @param docExtractSquidID
     * @param name
     * @return
     * @author lei.bin
     * @date 2014年5月7日
     */
    public List<String> getDocExtractSourceList(IRelationalDataManager adapter, int sourceId, int docExtractSquidID) throws Exception {
        out = new ReturnValue();
        SquidProcess squidProcess = new SquidProcess(TokenUtil.getToken());
        try {
            return squidProcess.getDocExtractSourceList(adapter, sourceId, docExtractSquidID, out);
        } catch (Exception e) {
            e.printStackTrace();
            logger.info(e);
            out.setMessageCode(MessageCode.ERR_EXTRACT_SQUID_NO_DATA);
            throw e;
        }
    }

    /**
     * @param xmlExtractSquidID
     * @param name
     * @return
     * @author lei.bin
     * @date 2014年5月7日
     */
    public Map<String, Object> getXmlExtractSourcePath(IRelationalDataManager adapter, int squidLinkFromId, String tableName) {
        out = new ReturnValue();
        SquidProcess squidProcess = new SquidProcess(TokenUtil.getToken());
        return squidProcess.getXmlExtractSourcePath(adapter, squidLinkFromId, tableName, out);
    }

    /**
     * 创建url列表
     *
     * @param info
     * @return
     */
    public String createWebUrls(String info) {
        out = new ReturnValue();
        SquidProcess squidProcess = new SquidProcess(TokenUtil.getToken());
        Map<String, Object> map = squidProcess.createWebUrls(info, out);
        return infoNewMessagePacket(map, DSObjectType.WEBURL, out);
    }

    /**
     * 更新url列表
     *
     * @param info
     * @return
     */
    public String updateWebUrls(String info) {
        out = new ReturnValue();
        SquidProcess squidProcess = new SquidProcess(TokenUtil.getToken());
        squidProcess.updateWebUrls(info, out);
        return infoNewMessagePacket(null, DSObjectType.WEBURL, out);
    }

    /**
     * 创建第三方httpsquid
     *
     * @param info
     * @return
     */
    public String createHttpSquid(String info) {
        out = new ReturnValue();
        SquidProcess squidProcess = new SquidProcess(TokenUtil.getToken());
        Map<String, Object> map = squidProcess.createHttpSquid(info, out);
        return infoNewMessagePacket(map, DSObjectType.HTTP, out);
    }

    /**
     * 更新第三方httpsquid
     *
     * @param info
     * @return
     */
    public String updateHttpSquid(String info) {
        out = new ReturnValue();
        SquidProcess squidProcess = new SquidProcess(TokenUtil.getToken());
        Map<String, Object> map = squidProcess.updateHttpSquid(info, out);
        return infoNewMessagePacket(map, DSObjectType.HTTP, out);
    }

    /**
     * 创建第三方Webservicesquid
     *
     * @param info
     * @return
     */
    public String createWebserviceSquid(String info) {
        out = new ReturnValue();
        SquidProcess squidProcess = new SquidProcess(TokenUtil.getToken());
        Map<String, Object> map = squidProcess.createWebserviceSquid(info, out);
        return infoNewMessagePacket(map, DSObjectType.WEBSERVICE, out);
    }

    /**
     * 更新第三方Webservicesquid
     *
     * @param info
     * @return
     */
    public String updateWebserviceSquid(String info) {
        out = new ReturnValue();
        SquidProcess squidProcess = new SquidProcess(TokenUtil.getToken());
        Map<String, Object> map = squidProcess.updateWebserviceSquid(info, out);
        return infoNewMessagePacket(map, DSObjectType.WEBSERVICE, out);
    }

    /**
     * 更新第三方接口参数
     * 2014-12-4
     *
     * @param info
     * @return
     * @author Akachi
     * @E-Mail zsts@hotmail.com
     */
    public String updateThirdPartyParams(String info) {
        out = new ReturnValue();
        SquidProcess squidProcess = new SquidProcess(TokenUtil.getToken());
        Map<String, Object> map = squidProcess.updateThirdPartyParams(info, out);
        return infoNewMessagePacket(map, DSObjectType.WEBSERVICE, out);
    }

    /**
     * 连接Webservicesquid
     *
     * @param info
     * @return
     */
    public String connectWebserviceSquid(String info) {
        out = new ReturnValue();
        SquidProcess squidProcess = new SquidProcess(TokenUtil.getToken());
        Map<String, Object> map = squidProcess.connectWebserviceSquid(info, out);
        return infoNewMessagePacket(map, DSObjectType.WEBSERVICE, out);
    }

    /**
     * 创建SourceTable
     *
     * @param info
     * @return
     */
    public String createSourceTable(String info) {
        out = new ReturnValue();
        SquidProcess squidProcess = new SquidProcess(TokenUtil.getToken());
        Map<String, Object> map = squidProcess.createSourceTable(info, out);
        return infoNewMessagePacket(map, DSObjectType.SOURCETABLE, out);
    }

    /**
     * 更新SourceTable
     *
     * @param info
     * @return
     */
    public String updateSourceTable(String info) {
        out = new ReturnValue();
        SquidProcess squidProcess = new SquidProcess(TokenUtil.getToken());
        Map<String, Object> map = squidProcess.updateSourceTable(info, out);
        return infoNewMessagePacket(map, DSObjectType.SOURCETABLE, out);
    }

    //added by yi.zhou 2015-01-27

    /**
     * 创建DestWsSquid
     *
     * @param info
     * @return
     */
    public String createDestWsSquid(String info) {
        out = new ReturnValue();
        SquidProcess squidProcess = new SquidProcess(TokenUtil.getToken());
        Map<String, Object> map = squidProcess.createDestWsSquid(info, out);
        return infoNewMessagePacket(map, DSObjectType.DESTWS, out);
    }

    /**
     * 修改DestWsSquid
     *
     * @param info
     * @return
     */
    public String updateDestWsSquid(String info) {
        out = new ReturnValue();
        SquidProcess squidProcess = new SquidProcess(TokenUtil.getToken());
        Map<String, Object> map = squidProcess.updateDestWsSquid(info, out);
        return infoNewMessagePacket(map, DSObjectType.DESTWS, out);
    }

    //added by yi.zhou 2015-03-30

    /**
     * 创建AnnotationSquid
     *
     * @param info
     * @return
     */
    public String createAnnotationSquid(String info) {
        out = new ReturnValue();
        SquidProcess squidProcess = new SquidProcess(TokenUtil.getToken());
        Map<String, Object> map = squidProcess.createAnnotationSquid(info, out);
        return infoNewMessagePacket(map, DSObjectType.ANNOTATION, out);
    }

    /**
     * 修改AnnotationSquid
     *
     * @param info
     * @return
     */
    public String updateAnnotationSquid(String info) {
        out = new ReturnValue();
        SquidProcess squidProcess = new SquidProcess(TokenUtil.getToken());
        Map<String, Object> map = squidProcess.updateAnnotationSquid(info, out);
        return infoNewMessagePacket(map, DSObjectType.ANNOTATION, out);
    }

    /**
     * 创建createNOSQLConnection
     *
     * @param info
     * @return
     */
    public String createNOSQLConnectionSquid(String info) {
        out = new ReturnValue();
        SquidProcess squidProcess = new SquidProcess(TokenUtil.getToken());
        Map<String, Object> map = squidProcess.createNOSQLConnection(info, out);
        return infoNewMessagePacket(map, DSObjectType.MONGODB, out);
    }

    /**
     * 修改updateNOSQLConnection
     *
     * @param info
     * @return
     */
    public String updateNOSQLConnection(String info) {
        out = new ReturnValue();
        SquidProcess squidProcess = new SquidProcess(TokenUtil.getToken());
        Map<String, Object> map = squidProcess.updateNOSQLConnection(info, out);
        return infoNewMessagePacket(map, DSObjectType.MONGODB, out);
    }

    /**
     * 连接connectNOSQLConnetion
     *
     * @param info
     * @return
     */
    public String connectNOSQLConnection(String info) {
        out = new ReturnValue();
        SquidProcess squidProcess = new SquidProcess(TokenUtil.getToken());
        Map<String, Object> map = squidProcess.connectNOSQLConnection(info, out);
        return infoNewMessagePacket(map, DSObjectType.MONGODB, out);
    }

    /**
     * 预览mongodb里面的数据
     *
     * @param info
     * @return
     */
    public String getNoSQLPreviewData(String info) {
        out = new ReturnValue();
        SquidProcess squidProcess = new SquidProcess(TokenUtil.getToken());
        Map<String, Object> map = squidProcess.getNoSQLPreviewData(info, out);
        //return infoNewMessagePacket(map, DSObjectType.MONGODB, out);
        return JsonUtil.toJsonString(map, DSObjectType.MONGODB, out.getMessageCode(), 1);
    }

    /**
     * 批量创建MongoExtract
     *
     * @param info
     * @return
     */
    public String createMongodbExtractsSquid(String info) {
        out = new ReturnValue();
        SquidProcess squidProcess = new SquidProcess(TokenUtil.getToken(), TokenUtil.getKey());
        squidProcess.createMongodbExtracts(info, out);
        return null;
    }

    /**
     * 获得MongoDBExtract Column Name
     *
     * @param info
     * @return
     */
    public String getMongodbExtract(String info) {
        out = new ReturnValue();
        ExtractServicesub squidProcess = new ExtractServicesub(TokenUtil.getToken());
        Map<String, Object> infoMap = JsonUtil.toHashMap(info);
        Map<String, Object> resultMap = squidProcess.getMongodbExtracts(infoMap, out);
        InfoNewMessagePacket<Map<String, Object>> infoMessage = new InfoNewMessagePacket<Map<String, Object>>();
        // 有源数据或者解析成功
        if (StringUtils.isNotNull(resultMap)) {
            infoMessage.setCode(out.getMessageCode().value());
        }
        // 没有有源数据或者解析失败
        else {
            infoMessage.setCode(MessageCode.ERR_EXTRACT_SQUID_NO_DATA.value());
        }
        infoMessage.setInfo(resultMap);
        infoMessage.setDesc("获取DocExractSquid的元数据");
        return JsonUtil.object2Json(infoMessage);
    }

    /**
     * 修改MongodbExtract
     *
     * @param info
     * @return
     */
    public String updateMongodbExtract(String info) {
        out = new ReturnValue();
        SquidProcess squidProcess = new SquidProcess(TokenUtil.getToken());
        Map<String, Object> map = squidProcess.updateMongodbExtract(info, out);
        return infoNewMessagePacket(map, DSObjectType.MONGODBEXTRACT, out);
    }
}
