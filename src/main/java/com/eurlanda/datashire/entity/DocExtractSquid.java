package com.eurlanda.datashire.entity;

import com.eurlanda.datashire.annotation.ColumnMpping;
import com.eurlanda.datashire.annotation.MultitableMapping;
import com.eurlanda.datashire.enumeration.DSObjectType;

import java.sql.Types;
import java.util.List;

@MultitableMapping(name = {"DS_SQUID"}, pk="ID", desc = "")
public class DocExtractSquid extends DataSquid{
    public DocExtractSquid(){
		this.setType(DSObjectType.SQUID.value());
    }
	@ColumnMpping(name="doc_format", desc="", nullable=true, precision=1, type=Types.INTEGER, valueReg="")
	private int doc_format;
	@ColumnMpping(name="row_format", desc="0:delimited,1:fixedLength,2:text", nullable=true, precision=1, type=Types.INTEGER, valueReg="")
	private int row_format;
	@ColumnMpping(name="delimiter", desc="", nullable=true, precision=500, type=Types.VARCHAR, valueReg="")
	private String delimiter;
	@ColumnMpping(name="field_length", desc="", nullable=true, precision=1, type=Types.INTEGER, valueReg="")
	private int field_length;
	@ColumnMpping(name="header_row_no", desc="", nullable=true, precision=1, type=Types.INTEGER, valueReg="")
	private int header_row_no;
	@ColumnMpping(name="first_data_row_no", desc="", nullable=true, precision=1, type=Types.INTEGER, valueReg="")
	private int first_data_row_no;
	@ColumnMpping(name="row_delimiter", desc="", nullable=true, precision=500, type=Types.VARCHAR, valueReg="") //fixed bug 687 by bo.dang
	private String row_delimiter;
	@ColumnMpping(name="row_delimiter_position", desc="", nullable=true, precision=1, type=Types.INTEGER, valueReg="")
	private int row_delimiter_position;
    @ColumnMpping(name="compressicon_codec", desc="压缩格式,0没有压缩,大于零的为压缩枚举", nullable=true, precision=1, type=Types.INTEGER, valueReg="")
    private int compressiconCodec;

	public DocExtractSquid(Squid s){
        if(s!=null && s.getId()>0){
            this.setId(s.getId());
            this.setKey(s.getKey());
            this.setCreation_date(s.getCreation_date());
            this.setName(s.getName());
            this.setLocation_x(s.getLocation_x());
            this.setLocation_y(s.getLocation_y());
            this.setSquid_width(s.getSquid_width());
            this.setSquid_height(s.getSquid_height());
            this.setSquid_type(s.getSquid_type());
            this.setSquidflow_id(s.getSquidflow_id());
            this.setIs_show_all(s.isIs_show_all());
            this.setSource_is_show_all(s.isSource_is_show_all());
            this.setSource_transformation_group_x(s.getSource_transformation_group_x());
            this.setSource_transformation_group_y(s.getSource_transformation_group_y());
            this.setTransformation_group_x(s.getTransformation_group_x());
            this.setTransformation_group_y(s.getTransformation_group_y());
            this.setTable_name(s.getTable_name());
            this.setFilter(s.getFilter());
            this.setEncoding(s.getEncoding());
            this.setDescription(s.getDescription());
        }
    }
	private List<Column> columns;
    // 索引index
    private List<SquidIndexes> squidIndexesList;
	
	//private List<ReferenceColumnGroup> sourceColumns;
	private List<ReferenceColumn> sourceColumns;
	
	private List<TransformationLink> transformationLinks;
	private List<Transformation> transformations;
	private List<Transformation> fromTransformations;//get by transformationLink -> from_transformation_id
	private List<Transformation> toTransformations;//get by transformationLink -> to_transformation_id
	public int getDoc_format() {
		return doc_format;
	}
	public void setDoc_format(int doc_format) {
		this.doc_format = doc_format;
	}
	public int getRow_format() {
		return row_format;
	}
	public void setRow_format(int row_format) {
		this.row_format = row_format;
	}
	public String getDelimiter() {
		return delimiter;
	}
	public void setDelimiter(String delimiter) {
		this.delimiter = delimiter;
	}
	public int getField_length() {
		return field_length;
	}
	public void setField_length(int field_length) {
		this.field_length = field_length;
	}
	public int getHeader_row_no() {
		return header_row_no;
	}
	public void setHeader_row_no(int header_row_no) {
		this.header_row_no = header_row_no;
	}
	public int getFirst_data_row_no() {
		return first_data_row_no;
	}
	public void setFirst_data_row_no(int first_data_row_no) {
		this.first_data_row_no = first_data_row_no;
	}
	public String getRow_delimiter() {
		return row_delimiter;
	}
	public void setRow_delimiter(String row_delimiter) {
		this.row_delimiter = row_delimiter;
	}
	public int getRow_delimiter_position() {
		return row_delimiter_position;
	}
	public void setRow_delimiter_position(int row_delimiter_position) {
		this.row_delimiter_position = row_delimiter_position;
	}
	public List<Column> getColumns() {
		return columns;
	}
	public void setColumns(List<Column> columns) {
		this.columns = columns;
	}
	public List<ReferenceColumn> getSourceColumns() {
		return sourceColumns;
	}
	public void setSourceColumns(List<ReferenceColumn> sourceColumns) {
		this.sourceColumns = sourceColumns;
	}
	public List<TransformationLink> getTransformationLinks() {
		return transformationLinks;
	}
	public void setTransformationLinks(List<TransformationLink> transformationLinks) {
		this.transformationLinks = transformationLinks;
	}
	public List<Transformation> getTransformations() {
		return transformations;
	}
	public void setTransformations(List<Transformation> transformations) {
		this.transformations = transformations;
	}
	public List<Transformation> getFromTransformations() {
		return fromTransformations;
	}
	public void setFromTransformations(List<Transformation> fromTransformations) {
		this.fromTransformations = fromTransformations;
	}
	public List<Transformation> getToTransformations() {
		return toTransformations;
	}
	public void setToTransformations(List<Transformation> toTransformations) {
		this.toTransformations = toTransformations;
	}
    public List<SquidIndexes> getSquidIndexesList() {
        return squidIndexesList;
    }
    public void setSquidIndexesList(List<SquidIndexes> squidIndexesList) {
        this.squidIndexesList = squidIndexesList;
    }

    public int getCompressiconCodec() {
        return compressiconCodec;
    }

    public void setCompressiconCodec(int compressiconCodec) {
        this.compressiconCodec = compressiconCodec;
    }
}
