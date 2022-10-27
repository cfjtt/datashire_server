package com.eurlanda.datashire.sprint7.service.squidflow.subservice;

import com.eurlanda.datashire.entity.MongodbExtractSquid;
import com.eurlanda.datashire.entity.SourceColumn;
import com.eurlanda.datashire.enumeration.datatype.SystemDatatype;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import org.bson.types.ObjectId;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by e56 on 2015/5/22.
 */
public class NoSqlServiceSub {

    public static List<SourceColumn> getCSVList(List<Map<String,Object>> listMap,Map<String,MongoDbObjectType> param, MongodbExtractSquid mongodbExtractSquid) {
        List<SourceColumn> sourceColumnList = new ArrayList<SourceColumn>();
        int i = 0;
        for (Map.Entry<String,MongoDbObjectType> entry : param.entrySet()) {
//            grow = docGrowList.get(y);
            String columnTypeName = "";
            SystemDatatype columnType = SystemDatatype.UNKNOWN;
            if(entry.getValue().getValue() instanceof LinkedHashMap){
                columnType=SystemDatatype.MAP;
            }else if (entry.getValue().getValue() instanceof ArrayList){


                columnType=SystemDatatype.ARRAY;
            }else{
                columnTypeName = entry.getValue().getValue().getClass().getSimpleName();
            }

            if(columnTypeName.equalsIgnoreCase("String")){
                columnType=SystemDatatype.NVARCHAR;
            }else if(columnTypeName.equalsIgnoreCase("Integer")){
                columnType=SystemDatatype.INT;
            }else if(columnTypeName.equalsIgnoreCase("Long")){
                columnType=SystemDatatype.BIGINT;
            }else if(columnTypeName.equalsIgnoreCase("Date")){
                columnType=SystemDatatype.DATETIME;
            }else if(columnTypeName.equalsIgnoreCase("Float")||columnTypeName.equalsIgnoreCase("Double")){
                columnType=SystemDatatype.FLOAT;
            }else if(columnTypeName.equalsIgnoreCase("Char")){
                columnType=SystemDatatype.NCHAR;
            }else if(columnTypeName.equals("Byte")||columnTypeName.equals("Boolean")){
                columnType=SystemDatatype.BIT;
            }else if (columnTypeName.equals("ObjectId")){
                columnType=SystemDatatype.NVARCHAR;
            }else if(columnTypeName.equals("byte[]")){
                columnType=SystemDatatype.BINARY;
            }else if(!"".equals(columnTypeName)){
//                System.out.println();
            }
            i++;
            SourceColumn sourceColumn = new SourceColumn();
            sourceColumn.setName(entry.getKey());
            sourceColumn.setData_type(columnType.value());
            if (sourceColumn.getData_type()==9){
                if(entry.getValue().getLength().intValue()>255) {
                    sourceColumn.setLength(new Float(entry.getValue().getLength().intValue() * 1.3f).intValue());
                }else{
                    sourceColumn.setLength(entry.getValue().getLength().intValue());
                }
            }else {
                sourceColumn.setLength(0);
            }
            sourceColumn.setRelative_order(i);
            sourceColumn.setSource_table_id(mongodbExtractSquid.getSource_table_id());
            sourceColumn.setNullable(true);
            sourceColumnList.add(sourceColumn);
        }
        return sourceColumnList;
    }

    //获得所有列
    public static Map<String,Class> getColumnListByCollection(DB db,String tableName,Integer limit){
        Map<String,Class> mapFuck = new HashMap<String,Class>();

        DBCollection dbCollection = db.getCollection("user");
        if(dbCollection.getCount()>0){
            DBCursor dbCursor = dbCollection.find().limit(limit);
            while (dbCursor.hasNext()){
                DBObject object = dbCursor.next();
                BasicDBObject basicDBObject=(BasicDBObject)object;
                basicDBObject.entrySet();
                for (Map.Entry entry:basicDBObject.entrySet()) {
                    mapFuck.put(entry.getKey().toString(),entry.getValue().getClass());
                }
            }
        }
        return mapFuck;
    }


    //获得所有列
    public static Map<String,MongoDbObjectType> getColumnListByCollection(List<Map<String,Object>> dataList){
        Map<String,MongoDbObjectType> mapFuc = new HashMap<>();

            for (Map<String,Object> object:dataList){
                for (Map.Entry<String,Object> entry:object.entrySet()) {
                    if(entry.getValue()==null){
                    	entry.setValue("");
                    }
                    if (!mapFuc.containsKey(entry.getKey())) {
                        MongoDbObjectType mongoDbObjectType = new MongoDbObjectType();
                        mongoDbObjectType.setValue(entry.getValue());
                        if(entry.getValue().getClass().equals(String.class)||entry.getValue().getClass().equals(ObjectId.class)) {
                            mongoDbObjectType.setLength(entry.getValue().toString().length());
                            if(mongoDbObjectType.getLength()<255){
                                mongoDbObjectType.setLength(255);
                            }
                        }
                        mongoDbObjectType.setName(entry.getKey());
                        mapFuc.put(entry.getKey(),mongoDbObjectType);
                    }else if(entry.getValue()!=null&&
//                    		mapFuc.get(entry.getKey()).getValue()!=null&&
                            mapFuc.get(entry.getKey()).getValue().getClass().equals(String.class)&&
                            mapFuc.get(entry.getKey()).getLength()<entry.getValue().toString().length()) {
                        mapFuc.get(entry.getKey()).setLength(entry.getValue().toString().length());
                    }
                }
            }
        return mapFuc;
    }

    /**
     * 获得所有数据
     * @param db
     * @param tableName
     * @param limit
     * @return
     */
    public static List<Map<String,Object>> getDataListByCollection(DB db,String tableName,Integer limit){
        List<Map<String,Object>> listMap = new ArrayList<>();
        DBCollection dbCollection = db.getCollection(tableName);
        if(dbCollection.getCount()>0){
            DBCursor dbCursor = dbCollection.find().limit(limit);
            while (dbCursor.hasNext()){
                DBObject object = dbCursor.next();
                BasicDBObject basicDBObject=(BasicDBObject)object;
                Map<String,Object> mapFuc = new HashMap<>();
                for (Map.Entry entry:basicDBObject.entrySet()) {
                    mapFuc.put(entry.getKey().toString(),entry.getValue());
                }
                listMap.add(mapFuc);
            }
        }
        return listMap;
    }
}
