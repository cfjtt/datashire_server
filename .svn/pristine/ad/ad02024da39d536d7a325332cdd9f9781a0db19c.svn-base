package com.eurlanda.datashire.entity.dest;

import com.eurlanda.datashire.annotation.ColumnMpping;
import com.eurlanda.datashire.annotation.MultitableMapping;
import com.eurlanda.datashire.entity.Squid;
import com.eurlanda.datashire.enumeration.DSObjectType;
import com.eurlanda.datashire.enumeration.SquidTypeEnum;

import java.sql.Types;

/**
 * Created by yhc on 4/26/2016.
 */
@MultitableMapping(pk="ID", name = {"DS_SQUID"}, desc = "HDFS 落地 SquidModelBase")
public class DestHDFSSquid extends Squid implements DestSquid {
    public DestHDFSSquid(){
        this.setSquid_type(SquidTypeEnum.DEST_HDFS.value());
        this.setType(DSObjectType.DEST_HDFS.value());
    }

    @ColumnMpping(name="HOST", desc="", nullable=true, precision=50, type= Types.VARCHAR, valueReg="")
    private String host;
    @ColumnMpping(name="HDFS_PATH", desc="", nullable=true, precision=200, type= Types.VARCHAR, valueReg="")
    private String hdfs_path;
    @ColumnMpping(name="FILE_FORMATE", desc="", nullable=true, precision=0, type= Types.INTEGER, valueReg="")
    private int file_formate;
    @ColumnMpping(name="ZIP_TYPE", desc="", nullable=true, precision=0, type= Types.INTEGER, valueReg="")
    private int zip_type;
    @ColumnMpping(name="SAVE_TYPE", desc="", nullable=true, precision=0, type= Types.INTEGER, valueReg="")
    private int save_type;
    @ColumnMpping(name="COLUMN_DELIMITER", desc="", nullable=true, precision=30, type= Types.VARCHAR, valueReg="")
    private String column_delimiter;
    @ColumnMpping(name="ROW_DELIMITER", desc="", nullable=true, precision=30, type= Types.VARCHAR, valueReg="")
    private String row_delimiter;

    public String getRow_delimiter() {
        return row_delimiter;
    }

    public void setRow_delimiter(String row_delimiter) {
        this.row_delimiter = row_delimiter;
    }

    public String getColumn_delimiter() {
        return column_delimiter;
    }

    public void setColumn_delimiter(String column_delimiter) {
        this.column_delimiter = column_delimiter;
    }

    public String getHost() {
        return host;
    }
    public void setHost(String host) {
        this.host = host;
    }



    public String getHdfs_path() {
        return hdfs_path;
    }
    public void setHdfs_path(String hdfs_path) {
        this.hdfs_path = hdfs_path;
    }



    public int getFile_formate() {
        return file_formate;
    }
    public void setFile_formate(int file_formate) {
        this.file_formate = file_formate;
    }



    public int getZip_type() {
        return zip_type;
    }
    public void setZip_type(int zip_type) {
        this.zip_type = zip_type;
    }



    public int getSave_type() {
        return save_type;
    }
    public void setSave_type(int save_type) {
        this.save_type = save_type;
    }
}
