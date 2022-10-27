package com.eurlanda.test;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by e56 on 2015/5/22.
 */
public class MongoTest {
    public static void main(String s[]){
        Mongo mongo = new Mongo("192.168.137.4",27017);
        DB db = mongo.getDB("SMARTHOME");
        db.getCollectionNames();
        Map<String,Class> mapFuck = getFuckListByCollection(db,"SMARTSWITCH.SETTINGS.LEARNLIGHT",100);
        for(Map.Entry entry : mapFuck.entrySet()){
            System.out.println("key:"+entry.getKey()+":valueClass:"+entry.getValue());
        }
    }
    private static Map<String,Class> getFuckListByCollection(DB db,String tableName,Integer limit){
        Map<String,Class> mapFuck = new HashMap<String,Class>();

        DBCollection dbCollection = db.getCollection(tableName);
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

}
