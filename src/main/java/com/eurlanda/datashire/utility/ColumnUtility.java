package com.eurlanda.datashire.utility;

import com.eurlanda.datashire.entity.Column;

/**
 * Created by yhc on 3/3/2016.
 */
public class ColumnUtility {
    /**
     * 检查是否为系统列
     * TODO: 应该移到公共类
     * @return
     */
    public static boolean checkIsSystemColumn(Column currentColumn){
        if (currentColumn.getName().equals("start_date") || currentColumn.getName().equals("end_date") ||
            currentColumn.getName().equals("end_date")  || currentColumn.getName().equals("active_flag") ||
            currentColumn.getName().equals("version") || currentColumn.getName().equals("key")){
            return true;
        }else {
            return false;
        }
    }
}
