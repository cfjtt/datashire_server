package com.eurlanda.datashire.server.model;


import java.io.Serializable;
import java.util.List;

public class ReferenceColumnGroup implements Serializable{
    private Integer id;

    private Integer reference_squid_id;

    private Integer relative_order;

    private List<ReferenceColumn> referenceColumnList;

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public Integer getReference_squid_id() {
        return reference_squid_id;
    }

    public void setReference_squid_id(Integer reference_squid_id) {
        this.reference_squid_id = reference_squid_id;
    }

    public Integer getRelative_order() {
        return relative_order;
    }

    public void setRelative_order(Integer relative_order) {
        this.relative_order = relative_order;
    }

    public List<ReferenceColumn> getReferenceColumnList() {
        return referenceColumnList;
    }

    public void setReferenceColumnList(List<ReferenceColumn> referenceColumnList) {
        this.referenceColumnList = referenceColumnList;
    }
}