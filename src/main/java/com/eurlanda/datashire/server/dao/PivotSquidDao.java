package com.eurlanda.datashire.server.dao;

import com.eurlanda.datashire.server.model.PivotSquid;
import org.springframework.stereotype.Repository;

/**
 * Created by test on 2017/11/24.
 */
@Repository
public interface PivotSquidDao {
    int insertPivotSquid(PivotSquid pivotSquid);
    int updatePivotSquid(PivotSquid pivotSquid);
    PivotSquid selectByPrimaryKey(Integer id);
    /**
     * 验证前台传递的数据是否发生变换
     */
    PivotSquid selectPivot(PivotSquid pivotSquid);

}
