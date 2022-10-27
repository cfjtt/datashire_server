package com.eurlanda.datashire.server.service;

import com.eurlanda.datashire.server.dao.ReferenceColumnGroupDao;
import com.eurlanda.datashire.server.model.ReferenceColumnGroup;
import com.eurlanda.datashire.utility.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by eurlanda - new 2 on 2017/7/25.
 * ReferenceGroup处理类
 */
@Service
public class ReferenceGroupService {
    private static final Logger logger = LoggerFactory.getLogger(ReferenceGroupService.class);
    @Autowired
    private ReferenceColumnGroupDao referenceColumnGroupDao;
    /**
     * 创建ReferenceColumn
     *
     * @param
     * @return
     * @author bo.dang          // update bug：728  by yi.zhou
     * @date 2014年5月10日
     */
    public ReferenceColumnGroup mergeReferenceColumnGroup(int fromSquid, int toSquidId) {
        ReferenceColumnGroup columnGroup = null;
        Map<String,Integer> map =new HashMap<>();
        try {
            map.put("fromSquid",fromSquid);
            map.put("toSquidId",toSquidId);
            columnGroup =referenceColumnGroupDao.selectGroupBySquidId(map);
            if(StringUtils.isNull(columnGroup)){
                int cnt= referenceColumnGroupDao.selectGroupCountBySquidId(toSquidId);
                columnGroup = new ReferenceColumnGroup();
                columnGroup.setReference_squid_id(toSquidId);
                columnGroup.setRelative_order(cnt+1);
                referenceColumnGroupDao.insertReferenceColumnGroup(columnGroup);
            }
        } catch (Exception e){
            e.printStackTrace();
            logger.error("Insert ReferenceColumnGroup异常", e);
        }
        return columnGroup;
    }
}
