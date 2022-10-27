package com.eurlanda.datashire.dao.dest;

import com.eurlanda.datashire.dao.IBaseDao;
import com.eurlanda.datashire.entity.dest.EsColumn;

import java.util.List;

/**
 * Created by zhudebin on 15-9-17.
 */
public interface IEsColumnDao extends IBaseDao {

    List<EsColumn> getEsColumnsBySquidId(int squidId);

    List<EsColumn> getEsColumnsBySquidId(boolean inSession, int squidId);//定义一个新接口，方便下面调用

    void deleteEsColumnBySquidId(int squidId);

    void deleteEsColumn(int squidId, int columnId);
}
