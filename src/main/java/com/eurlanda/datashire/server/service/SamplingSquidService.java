package com.eurlanda.datashire.server.service;

import com.eurlanda.datashire.server.dao.SamplingSquidDao;
import com.eurlanda.datashire.server.model.SamplingSquid;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class SamplingSquidService {
    @Autowired
    private SamplingSquidDao samplingSquidDao;
    public int insertSamplingSquid(SamplingSquid samplingSquid){
        samplingSquidDao.insertSamplingSquid(samplingSquid);
        return samplingSquid.getId();
    }
    public  int updateSamplingSquid(SamplingSquid samplingSquid){
        return  samplingSquidDao.updateSamplingSquid(samplingSquid);
    }
    public   SamplingSquid selectByPrimaryKey(Integer id){
        return samplingSquidDao.selectByPrimaryKey(id);
    }
}
