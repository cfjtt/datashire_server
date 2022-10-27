package com.eurlanda.datashire.adapter.db;

import com.eurlanda.datashire.entity.DBConnectionInfo;

public class RedShiftAdapter extends AbsDBAdapter implements INewDBAdapter {
    public RedShiftAdapter(DBConnectionInfo info) {
        super(info);
    }

}
