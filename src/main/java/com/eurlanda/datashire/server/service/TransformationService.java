package com.eurlanda.datashire.server.service;

import com.eurlanda.datashire.server.dao.TransformationDao;
import com.eurlanda.datashire.server.dao.TransformationInputsDao;
import com.eurlanda.datashire.server.exception.ErrorMessageException;
import com.eurlanda.datashire.server.model.Transformation;
import com.eurlanda.datashire.utility.MessageCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

/**
 * Created by eurlanda - new 2 on 2017/6/20.
 */
@Service
@Transactional
public class TransformationService {
    private static final Logger LOGGER = LoggerFactory.getLogger(TransformationService.class);
    @Autowired
    private TransformationDao transformationDao;
    @Autowired
    private TransformationInputsDao transformationInputsDao;
    @Autowired
    private TransformationLinkService transformationLinkService;

    /**
     * 删除Transformation要把对应的inputs删掉
     * @param
     * @return
     */
    @Transactional(rollbackFor = {Exception.class})
    public int deleteTransformation(List<Integer> ids) throws Exception{
        int count = 0;
        try {
            if (ids != null & ids.size() > 0) {
                //根据TransformationId删除inputs
                count = transformationDao.deleteTransformations(ids);
                transformationInputsDao.deleteByTransId(ids);
            }else{
                throw new ErrorMessageException(MessageCode.DESERIALIZATION_FAILED.value());
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
        return count;
    }

    /**
     *新增Transformation
     * @param
     * @throws Exception
     */
    @Transactional(rollbackFor = {Exception.class})
    public int insertTransformation(List<Transformation> transformationList,Integer squid_id)throws Exception{
        try {
            if(squid_id==0||transformationList.size()==0){
                throw new Exception("数据不完整");
            }
            if(null!=transformationList&&transformationList.size()>0){
                for (Transformation transformation:transformationList){
                    transformation.setSquid_id(squid_id);
                }
                transformationDao.insert(transformationList);
            }
        }catch (Exception e){
            e.printStackTrace();
            throw e;
        }
        return 0;
    }
    /**
     *批量新增Transformation
     * @param
     * @throws Exception
     */
    @Transactional(rollbackFor = {Exception.class})
    public int insertTransformation(List<Transformation> transformationList)throws Exception{
        int count=0;
        try {
            if(transformationList.size()==0){
                throw new Exception("数据不完整");
            }
            if(null!=transformationList&&transformationList.size()>0){
                count=transformationDao.insert(transformationList);
            }
        }catch (Exception e){
            e.printStackTrace();
            throw e;
        }
        return count;
    }


    /**
     * 删除Trans面板上的对象
     * @param transLinkIds 需要删除的Translink id 集合
     * @param transIds 需要删除的 Trans id 集合
     * @throws Exception 如果中途失败，抛出包含错误码的异常
     * @return 收到影响的Trans
     * flag 用来判断是单独删除transformation面板调用的还是在删除squidLink调用的。
     * 单独删除transformation时前台会把所有需要删除的transformationLink都传过来。后台就不再需要查询Link下游的trans是否还有关联的LInk
     * 而删除squidLink时。需要查询transformation面板上trans下游是否还有关联的Link
     *
     */
    @Transactional(rollbackFor = {Exception.class})
    public List<Transformation> deleteTransformationPanelObj(List<Integer> transLinkIds, List<Integer> transIds,Integer flag) throws Exception{
        int count = 0;
        List<Transformation> changedTrans = null;
        try{
            //删除Trans link 并且同步收到影响的Trans
            changedTrans = transformationLinkService.deleteTransformationLinkIds(transLinkIds, transIds,flag);
            //根据TransformationId删除inputs
            transformationInputsDao.deleteByTransId(transIds);
            //删除Trans
            count = transformationDao.deleteTransformations(transIds);
        } catch (Exception e){
            e.printStackTrace();
            throw e;
        }
        if (changedTrans == null && changedTrans.size() <= 0){
            throw new ErrorMessageException(MessageCode.ERR_DELETE.value());
        }

        return changedTrans;
    }

    /**
     * 单独添加Transformation
     * @param transformation
     * @return
     */
    @Transactional(rollbackFor = {Exception.class})
    public int addTransformation(Transformation transformation) throws Exception{
        int count=0;
        try {
            count=transformationDao.insertSelective(transformation);
        }catch (Exception e){
            throw e;
        }
        return count;
    }
}
