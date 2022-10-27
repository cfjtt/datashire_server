package com.eurlanda.datashire.server.model.Base;


import com.eurlanda.datashire.entity.Column;
import com.eurlanda.datashire.entity.ReferenceColumn;
import com.eurlanda.datashire.entity.Transformation;
import com.eurlanda.datashire.entity.TransformationLink;

import java.util.List;

/**
 * Created by My PC on 7/5/2017.
 * DataSquid类型的父类
 * DataSquid是指带有Trans，Column与ReferenceColumn的Squid
 */
public class DataSquidBaseModel extends SquidModelBase {

    protected List<Column> Columns;
    protected List<ReferenceColumn> SourceColumns;
    protected List<TransformationLink> TransformationLinks;
    protected List<Transformation> Transformations;

    public List<Column> getColumns() {
        return Columns;
    }

    public void setColumns(List<Column> columns) {
        Columns = columns;
    }

    public List<ReferenceColumn> getSourceColumns() {
        return SourceColumns;
    }

    public void setSourceColumns(List<ReferenceColumn> sourceColumns) {
        SourceColumns = sourceColumns;
    }

    public List<TransformationLink> getTransformationLinks() {
        return TransformationLinks;
    }

    public void setTransformationLinks(List<TransformationLink> transformationLinks) {
        TransformationLinks = transformationLinks;
    }

    public List<Transformation> getTransformations() {
        return Transformations;
    }

    public void setTransformations(List<Transformation> transformations) {
        Transformations = transformations;
    }
}
