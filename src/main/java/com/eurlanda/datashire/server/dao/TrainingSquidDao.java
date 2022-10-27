package com.eurlanda.datashire.server.dao;

import com.eurlanda.datashire.server.model.TrainingDBSquid;
import com.eurlanda.datashire.server.model.TrainingFileSquid;
import org.springframework.stereotype.Repository;

@Repository
public interface TrainingSquidDao {
    int insertTrainDBSquid(TrainingDBSquid dbSquid);
    int insertTrainFileSquid(TrainingFileSquid fileSquid);
    int updateTrainDBSquid(TrainingDBSquid dbSquid);
    int updateTrainFileSquid(TrainingFileSquid fileSquid);
}
