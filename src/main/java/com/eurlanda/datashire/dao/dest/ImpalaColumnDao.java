package com.eurlanda.datashire.dao.dest;

import com.eurlanda.datashire.dao.IBaseDao;
import com.eurlanda.datashire.entity.dest.DestImpalaColumn;

import java.util.List;

/**
 * Created by dzp on 2016/5/13.
 */
public interface ImpalaColumnDao extends IBaseDao {

    List<DestImpalaColumn> getImpalaColumnBySquidId(boolean inSession, int squidId );

    void deleteImpalaColumnBySquidId(int squidId);

    void deleteImpalaColumn(int squidId, int columnId);
}
