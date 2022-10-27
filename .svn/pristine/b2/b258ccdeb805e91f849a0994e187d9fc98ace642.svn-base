package com.eurlanda.datashire.server.service;

import com.eurlanda.datashire.server.dao.TrainingSquidDao;
import com.eurlanda.datashire.server.model.TrainingDBSquid;
import com.eurlanda.datashire.server.model.TrainingFileSquid;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class TrainingSquidService {
    @Autowired
    private TrainingSquidDao squidDao;

    /**
     * 添加TrainDb
     * @param dbSquid
     * @return
     */
    @Transactional
    public int insertTrainDBSquid(TrainingDBSquid dbSquid){
        squidDao.insertTrainDBSquid(dbSquid);
        return dbSquid.getId();
    }

    /**
     * 添加FileSquid
     * @param fileSquid
     * @return
     */
    @Transactional
    public int insertTrainFileSquid(TrainingFileSquid fileSquid){
        squidDao.insertTrainFileSquid(fileSquid);
        return fileSquid.getId();
    }

    /**
     * 更新DBSquid
     * @param dbSquid
     * @return
     */
    @Transactional
    public int updateTrainDBSquid(TrainingDBSquid dbSquid){
        return squidDao.updateTrainDBSquid(dbSquid);
    }

    /**
     * 更新fileSquid
     * @param fileSquid
     * @return
     */
    @Transactional
    public int updateTrainFileSquid(TrainingFileSquid fileSquid){
        return squidDao.updateTrainFileSquid(fileSquid);
    }
}
