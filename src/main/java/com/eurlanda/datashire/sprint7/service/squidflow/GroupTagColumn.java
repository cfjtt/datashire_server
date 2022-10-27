package com.eurlanda.datashire.sprint7.service.squidflow;

import com.eurlanda.datashire.entity.ReferenceColumn;

import java.util.List;

/**
 * Created by Eurlanda on 2017/3/8.
 */
public class GroupTagColumn extends ReferenceColumn{
    private List<ReferenceColumn> groupColumns;
    private List<ReferenceColumn> sortingColumns;
    private List<ReferenceColumn> taggingColumns;

    public List<ReferenceColumn> getGroupColumns() {
        return groupColumns;
    }

    public void setGroupColumns(List<ReferenceColumn> groupColumns) {
        this.groupColumns = groupColumns;
    }

    public List<ReferenceColumn> getSortingColumns() {
        return sortingColumns;
    }

    public void setSortingColumns(List<ReferenceColumn> sortingColumns) {
        this.sortingColumns = sortingColumns;
    }

    public List<ReferenceColumn> getTaggingColumns() {
        return taggingColumns;
    }

    public void setTaggingColumns(List<ReferenceColumn> taggingColumns) {
        this.taggingColumns = taggingColumns;
    }
}
