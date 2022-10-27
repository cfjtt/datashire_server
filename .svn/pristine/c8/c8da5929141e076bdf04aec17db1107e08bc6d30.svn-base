package com.eurlanda.datashire.sprint7.service.squidflow.subservice;

import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.entity.WeiBoExtractSquid;

import java.sql.SQLException;

public class WeiBoExtractSquidServiceSub extends ExtractServicesub {

    public WeiBoExtractSquidServiceSub(String token) {
        super(token);
    }

    protected WeiBoExtractSquidServiceSub(IRelationalDataManager adapter) {
        super(adapter);
        // TODO Auto-generated constructor stub
    }

    /**
     * 更新WeiBoExtractSquid
     * @param weiBoExtractSquid
     * @return
     * @author bo.dang
     * @date 2014年5月12日
     */
    public WeiBoExtractSquid updateWeiBoExtractSquid(WeiBoExtractSquid weiBoExtractSquid){
        try {
            adapter.openSession();
            adapter.update2(weiBoExtractSquid);
        } catch (SQLException e) {
            logger.error("更新XmlExtractSquid失败！", e);
            try {
                adapter.rollback();
            } catch (SQLException e1) { // 数据库回滚失败（程序不能处理该异常）！
                logger.fatal("rollback err!", e1);
            }
        } finally {
            adapter.closeSession();
        }
        return weiBoExtractSquid;
    }
}
