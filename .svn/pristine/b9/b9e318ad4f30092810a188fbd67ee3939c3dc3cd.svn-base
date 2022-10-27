package com.eurlanda.datashire.complieValidate;

import com.eurlanda.datashire.adapter.IRelationalDataManager;
import com.eurlanda.datashire.entity.BuildInfo;
import com.eurlanda.datashire.entity.Squid;

import java.util.List;
import java.util.Map;

/**
 * Created by Eurlanda-dev on 2016/8/9.
 */
public interface ICompileValidateService {

    /**
     * 验证connection是否合法
     */
    public List<BuildInfo> validateConnection(List<BuildInfo> buildInfoList, Map<String, Object> map, Squid squid, IRelationalDataManager adapter);

    /**
     * 验证是否是孤立的Squid
     */
    public List<BuildInfo> validateSingleSquid(List<BuildInfo> buildInfoList, Map<String, Object> map, Squid squid, IRelationalDataManager adapter);

    /**
     * 验证落地表名是否为空
     */
    public List<BuildInfo> validateExtractName(List<BuildInfo> buildInfoList, Map<String, Object> map, Squid squid, IRelationalDataManager adapter);
}
