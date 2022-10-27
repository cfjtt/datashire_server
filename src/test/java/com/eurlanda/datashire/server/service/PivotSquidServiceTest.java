package com.eurlanda.datashire.server.service;

import com.alibaba.fastjson.JSONArray;
import com.eurlanda.datashire.server.dao.ColumnDao;
import com.eurlanda.datashire.server.dao.PivotSquidDao;
import com.eurlanda.datashire.server.model.Column;
import com.eurlanda.datashire.server.model.PivotSquid;
import com.eurlanda.datashire.utility.JsonUtil;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by test on 2017/11/24.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration({"classpath:config/applicationContext.xml"})
public class PivotSquidServiceTest {

    @Autowired
    private PivotSquidService pivotSquidService;
    @Autowired
    private PivotSquidDao pivotSquidDao;
    @Autowired
    private ColumnDao columnDao;


    @Test
    public void insertPivot(){
        PivotSquid pivot = new PivotSquid();
        pivot.setSquidflow_id(54485);
        pivot.setName("今晚打老虎");
        pivot.setSquid_type(77);
        pivot.setLocation_x(116);
        pivot.setLocation_y(38);
        pivot.setSquid_width(70);
        pivot.setSquid_height(70);

        pivotSquidService.insertPivotSquid(pivot);
    }

    @Test
    public void updatePivot(){
        PivotSquid pivot = new PivotSquid();
        pivot.setId(296518);
        pivot.setSquidflow_id(54485);
        pivot.setName("老虎打今晚");
        pivot.setSquid_type(77);
        pivot.setLocation_x(222);
        pivot.setLocation_y(333);
        pivot.setSquid_width(70);
        pivot.setSquid_height(70);
        pivotSquidService.updatePivotSquid(pivot);
    }

    @Test
    public void insertBash(){
        List<Column> columns = new ArrayList<>();
        Column column1 = new Column();
        column1.setName("xxx1");
        column1.setSquid_id(284268);
        column1.setData_type(9);

        Column column2 = new Column();
        column2.setName("xxx2");
        column2.setSquid_id(284268);
        column2.setData_type(9);

        Column column3 = new Column();
        column3.setName("xxx3");
        column3.setSquid_id(284268);
        column3.setData_type(9);

        columns.add(column1);
        columns.add(column2);
        columns.add(column3);
        columnDao.insert(columns);
        System.out.println("xxxxx");
    }


    @Test
    public void testJson(){
        String json = "[\"a\",\"b\"]";
        JSONArray jsonArray = JSONArray.parseArray(json);

        List<String> list = JSONArray.parseArray(json,String.class);
        System.out.println("Xxx");
    }

    @Test
    public void testSelectPivot(){
        PivotSquid pivotSquid = new PivotSquid();
        pivotSquid.setId(297351);
        pivotSquid.setPivotColumnId(4718769);
        pivotSquid.setAggregationType(3);
        pivotSquid.setPivotColumnValue("[\"啊\"]");
        pivotSquid.setValueColumnId(47187712);
        pivotSquid.setGroupByColumnIds("4718773,4718775");
        PivotSquid count = pivotSquidDao.selectPivot(pivotSquid);
        System.out.println(1);
    }


    @Test
    public void testRex(){
        String regex="^[+-]?\\d+(\\.\\d+)?$";
        String test = "今晚";
        boolean flag = test.matches(regex);
        System.out.println("xxxx");

    }
}
