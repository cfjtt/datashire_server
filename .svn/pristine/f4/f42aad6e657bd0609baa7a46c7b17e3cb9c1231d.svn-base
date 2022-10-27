package com.eurlanda.datashire.server.service;

import com.eurlanda.datashire.server.dao.ScheduleJobDao;

import com.eurlanda.datashire.server.model.ScheduleJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Service
public class ScheduleJobService{
    private static final Logger logger = LoggerFactory.getLogger(ScheduleJobService.class);
    @Autowired
    private ScheduleJobDao dao;

    @Transactional
    public int deleteByPrimaryKey(Integer id) {
        return dao.deleteByPrimaryKey(id);
    }

    @Transactional
    public int insert(ScheduleJob record) {
        return dao.insert(record);
    }

    @Transactional
    public int insertSelective(ScheduleJob record) {
        return dao.insertSelective(record);
    }

    public ScheduleJob selectByPrimaryKey(Integer id) {
        return dao.selectByPrimaryKey(id);
    }

    @Transactional
    public int updateByPrimaryKeySelective(ScheduleJob record) {
        return dao.updateByPrimaryKeySelective(record);
    }

    @Transactional
    public int updateByPrimaryKey(ScheduleJob record) {
        return dao.updateByPrimaryKey(record);
    }

    @Transactional
    public List<ScheduleJob> selectScheduleJobByRepositoryId(Integer repositoryId){
        return dao.selectScheduleJobByRepositoryId(repositoryId);
    }
    public List<ScheduleJob> selectBySelective(ScheduleJob job){
        return dao.selectBySelective(job);
    }
}
