package com.eurlanda.test;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Created by Eurlanda-dev on 2016/1/27.
 */
public class ColumnSortTest {

    public static void main(String[]args){
        List<Column> list = new ArrayList<Column>();
        Column C1 = new Column();
            C1.setOredr(23);
        Column C2 = new Column();
            C2.setOredr(45);
        Column C3 = new Column();
            C3.setOredr(32);
            list.add(C1);
            list.add(C2);
            list.add(C3);

        for (Column c:list) {
            System.out.print(c.getOredr()+" ");
        }
        Collections.sort(list, new Comparator<Column>() {
            @Override
            public int compare(Column o1, Column o2) {
                if (o1.getOredr() > o2.getOredr()) {
                    return 1;
                }
                if (o1.getOredr() < o2.getOredr()) {
                    return -1;
                }
                    return 0;
            }
        });
        System.out.print("==排序后====");
        for (Column c:list) {
            System.out.print(c.getOredr()+" ");
        }

    }

}
