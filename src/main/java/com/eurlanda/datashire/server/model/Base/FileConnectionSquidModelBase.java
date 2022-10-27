package com.eurlanda.datashire.server.model.Base;

/**
 * Created by My PC on 7/5/2017.
 * 文件类型链接Squid父类
 * 这个类也因为引擎使用服务端老实体类的原因现在也无法继续使用了
 */
@Deprecated
public class FileConnectionSquidModelBase extends SquidModelBase {

    private Integer max_travel_depth;

    public Integer getMax_travel_depth() {
        return max_travel_depth;
    }
    public void setMax_travel_depth(Integer max_travel_depth) {
        this.max_travel_depth = max_travel_depth;
    }

}
