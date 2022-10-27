package com.eurlanda.datashire.server.dao;

import com.eurlanda.datashire.server.model.NormalizerSquid;
import org.apache.ibatis.annotations.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Map;

/**
 * Created by eurlanda - new 2 on 2017/6/29.
 */
@Repository
public interface NormalizerSquidDao {
    //添加NormalizerSquid
    int insertNormalizerSquid(NormalizerSquid normalizerSquid);
    //修改NormalizerSquid
    int updateNormalizerSquid(NormalizerSquid normalizerSquid);
    //根据ID查TransformationType的信息
    Map<String,Object> findTransformationType(int id);
    //根据From_squid_id查squid_type_id
    Map<String,Object> findSquidBySquidId(int fromSquidId);
    //获取标准化模型
    List<Map<String, Object>> getNormalizer(@Param("map") Map<String,Object> map);
}
