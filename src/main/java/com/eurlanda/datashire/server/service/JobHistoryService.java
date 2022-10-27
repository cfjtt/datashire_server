package com.eurlanda.datashire.server.service;

import com.eurlanda.datashire.server.dao.JobHistoryDao;
import com.eurlanda.datashire.server.model.JobHistory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

/**
 * Created by test on 2017/10/26.
 */

@Service
public class JobHistoryService {

    private static final Logger logger = LoggerFactory.getLogger(JobHistoryService.class);

    @Autowired
    JobHistoryDao jobHistoryDao;

    public int deleteByPrimaryKey(String taskId){

        return jobHistoryDao.deleteByPrimaryKey(taskId);
    }
    @Transactional
    public int insert(JobHistory record){
        return jobHistoryDao.insert(record);
    }
    @Transactional
    public int insertSelective(JobHistory record){
        return jobHistoryDao.insertSelective(record);
    }
    @Transactional
    public JobHistory selectByPrimaryKey(String taskId){
        return jobHistoryDao.selectByPrimaryKey(taskId);
    }
    @Transactional
    public int updateByPrimaryKeySelective(JobHistory record){
        return jobHistoryDao.updateByPrimaryKeySelective(record);
    }
    @Transactional
    public int updateByPrimaryKey(JobHistory record){
        return jobHistoryDao.updateByPrimaryKey(record);
    }
    @Transactional
    public int selectJobHistoryCoutByJobId(Integer jobId){
        return jobHistoryDao.selectJobHistoryCoutByJobId(jobId);
    }
    @Transactional
    public List<JobHistory> selectJobHistoryPaged(Integer jobId, Integer pageIndex, Integer pageSize){
        Integer totalCount = jobHistoryDao.selectJobHistoryCoutByJobId(jobId);
        Integer totalPage = totalCount % pageSize == 0 ? totalCount / pageSize
                : totalCount / pageSize + 1;
        //对非法的pageIndex和pageSize进行限制
        if (pageIndex < 1) {
            logger.debug("--------pageIndex异常:---"+pageIndex+"-----------");
            pageIndex = 1;
        } else if (pageIndex > totalPage && totalPage != 0) {
            logger.debug("--------pageIndex大于totalPage:---"+pageIndex+"--"+totalPage+"---------");
            pageIndex = totalPage;
        }
        //pageSize异常，默认为5
        if (pageSize < 1){
            logger.debug("--------pageSize异常:---"+pageIndex+"-----------");
            pageSize = 5;
        }
        Integer homePage = (pageIndex - 1) * pageSize;
        return jobHistoryDao.selectJobHistoryPaged(jobId,homePage,pageSize);
    }







}
