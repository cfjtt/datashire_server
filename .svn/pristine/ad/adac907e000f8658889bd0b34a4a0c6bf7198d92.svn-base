package com.eurlanda.datashire.server.model.Base;

import com.eurlanda.datashire.annotation.ColumnMpping;

import javax.xml.bind.annotation.XmlAttribute;
import java.io.Serializable;
import java.sql.Types;

/**
 * Created by zhudebin on 2017/6/12.
 */
public class BaseObject implements Serializable{

    @ColumnMpping(name="id", desc="", nullable=false, precision=0, type= Types.INTEGER, valueReg=">=1")
    protected Integer id;

    @ColumnMpping(name="name", desc="", nullable=true, precision=300, type=Types.VARCHAR, valueReg="")
    protected String name;

    @ColumnMpping(name="key", desc="", nullable=true, precision=36, type=Types.VARCHAR, valueReg="")
    protected String key;

    protected int type;

    public int getType() {
        return type;
    }
    public void setType(int type) {
        this.type = type;
    }

    public Integer getId() {
        return id;
    }
    public void setId(Integer id) {
        this.id = id;
    }

    @XmlAttribute(name="Name")
    public String getName() {
        return name;
    }
    public void setName(String name) {
        this.name = name;
    }

    public String getKey() {
        return key;
    }
    public void setKey(String key) {
        this.key = key;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + id;
        result = prime * result + ((key == null) ? 0 : key.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        BaseObject other = (BaseObject) obj;
        if (!id.equals(other.id))
            return false;
        if (key == null) {
            if (other.key != null)
                return false;
        } else if (!key.equals(other.key))
            return false;
        return true;
    }
}
