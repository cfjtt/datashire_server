package com.eurlanda.datashire.server.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by zhudebin on 16/2/25.
 */
public class ScanTest {


   @Test
   public void testTable() throws IOException{
       Configuration conf = getConfig();
       HBaseHelper helper = HBaseHelper.getHelper(conf);
       helper.createTable("table22","cf1", "cf2");
       helper.createTable("table11","cf1", "cf2");
   }

    @Test
    public void initDate11() throws IOException{
        Configuration conf = getConfig();
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table1 = connection.getTable(TableName.valueOf("table11"));
        short s = 1;
        long b = 3;
        BigDecimal bigDecimal = new BigDecimal(0.33);
        connection.getAdmin().listTableNames();
        for(int i=3; i<5000; i++) {
            Put put11 = new Put(Bytes.toBytes(i));
            put11.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("NVARCHAR"),
                    Bytes.toBytes("name-0" + i));
            put11.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("INT"),
                    Bytes.toBytes((20 + i)%90));
            put11.addColumn(Bytes.toBytes("cf2"), Bytes.toBytes("NCHAR"),
                    Bytes.toBytes("addr-0" + i));
            put11.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("FLOAT"),
                    Bytes.toBytes(12.3d + i));
            put11.addColumn(Bytes.toBytes("cf2"), Bytes.toBytes("FLOAT"),
                    Bytes.toBytes(1.11f+i));
            put11.addColumn(Bytes.toBytes("cf2"), Bytes.toBytes("BIG"),
                    Bytes.toBytes(true));
            put11.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("BIGINT"),
                    Bytes.toBytes(b+i));
            put11.addColumn(Bytes.toBytes("cf2"), Bytes.toBytes("SMALLINT"),
                    Bytes.toBytes(s));
            put11.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("DECIMAL"),
                    Bytes.toBytes(bigDecimal));
            table1.put(put11);
        }
        table1.close();
    }



    private static void printlnResultScanner1(ResultScanner rs) {
        long n = 0;
        for (Result result : rs) {
            n++;
            System.out.println("======== row :" + n);
            List<Cell> cells = result.getColumnCells(Bytes.toBytes("cf1"), Bytes.toBytes("NVARCHAR"));
            if(cells != null && cells.size() >0) {
                System.out.println("cf1:nvarchar  " + Bytes.toString(cells.get(0).getValueArray()));
            }
            cells = result.getColumnCells(Bytes.toBytes("cf1"), Bytes.toBytes("INT"));
            if(cells != null && cells.size() >0) {
                System.out.println("cf1:int" + Bytes.toInt(cells.get(0).getValueArray()));
            }
            cells = result.getColumnCells(Bytes.toBytes("cf2"), Bytes.toBytes("NCHAR"));
            if(cells != null && cells.size() >0) {
                System.out.println("cf2:nchar" + Bytes.toString(cells.get(0).getValueArray()));
            }
            cells = result.getColumnCells(Bytes.toBytes("cf1"), Bytes.toBytes("FLOAT"));
            if(cells != null && cells.size() >0) {
                System.out.println("cf1:float" + Bytes.toFloat(cells.get(0).getValueArray()));
            }

            cells = result.getColumnCells(Bytes.toBytes("cf1"), Bytes.toBytes("BIGINT"));
            if(cells != null && cells.size() >0) {
                System.out.println("cf1:bigint" + Bytes.toLong(cells.get(0).getValueArray()));
            }
            cells = result.getColumnCells(Bytes.toBytes("cf2"), Bytes.toBytes("SMALLINT"));
            if(cells != null && cells.size() >0) {
                System.out.println("cf2:smallint" + Bytes.toShort(cells.get(0).getValueArray()));
            }
            cells = result.getColumnCells(Bytes.toBytes("cf1"), Bytes.toBytes("DECIMAL"));
            if(cells != null && cells.size() >0) {
                System.out.println("cf1:decimal" + Bytes.toBigDecimal(cells.get(0).getValueArray()));
            }
        }
        System.out.println(" 总数 为: " + n);
    }


    @Test
    public void test22()
    {
        Short s = 3;
        byte[] bs = Bytes.toBytes(s);
        short ss = Bytes.toShort(bs);
        System.out.println(ss);
    }

    @Test
    public void testScanRowFilter11() throws IOException {
        Configuration conf = getConfig();
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf("table1"));
        Scan scan = new Scan();
        Filter rowFilter = new PageFilter(10);
        scan.setFilter(rowFilter);
        ResultScanner rs = table.getScanner(scan);
        printlnResultScanner1(rs);
        rs.close();
        connection.close();
    }



    @Test
    public void test1() throws IOException {
        Configuration conf = getConfig();
        HBaseHelper helper = HBaseHelper.getHelper(conf);
        helper.dropTable("testtable");
        helper.createTable("testtable", "colfam1");
        System.out.println("Adding rows to table...");
        helper.fillTable("testtable", 1, 10, 5, 2, true, false, "colfam1");

        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf("testtable"));
        // vv FilterListExample
        List<Filter> filters = new ArrayList<Filter>();

        Filter filter1 = new RowFilter(CompareFilter.CompareOp.GREATER_OR_EQUAL,
                new BinaryComparator(Bytes.toBytes("row-03")));
        filters.add(filter1);

        Filter filter2 = new RowFilter(CompareFilter.CompareOp.LESS_OR_EQUAL,
                new BinaryComparator(Bytes.toBytes("row-06")));
        filters.add(filter2);

        Filter filter3 = new QualifierFilter(CompareFilter.CompareOp.EQUAL,
                new RegexStringComparator("col-0[03]"));
        filters.add(filter3);

        FilterList filterList1 = new FilterList(filters);

        Scan scan = new Scan();
        scan.setFilter(filterList1);
        ResultScanner scanner1 = table.getScanner(scan);
        // ^^ FilterListExample
        System.out.println("Results of scan #1 - MUST_PASS_ALL:");
        int n = 0;
        // vv FilterListExample
        for (Result result : scanner1) {
            for (Cell cell : result.rawCells()) {
                System.out.println("Cell: " + cell + ", Value: " +
                        Bytes.toString(cell.getValueArray(), cell.getValueOffset(),
                                cell.getValueLength()));
                // ^^ FilterListExample
                n++;
                // vv FilterListExample
            }
        }
        scanner1.close();

        FilterList filterList2 = new FilterList(
                FilterList.Operator.MUST_PASS_ONE, filters);

        scan.setFilter(filterList2);
        ResultScanner scanner2 = table.getScanner(scan);
        // ^^ FilterListExample
        System.out.println("Total cell count for scan #1: " + n);
        n = 0;
        System.out.println("Results of scan #2 - MUST_PASS_ONE:");
        // vv FilterListExample
        for (Result result : scanner2) {
            for (Cell cell : result.rawCells()) {
                System.out.println("Cell: " + cell + ", Value: " +
                        Bytes.toString(cell.getValueArray(), cell.getValueOffset(),
                                cell.getValueLength()));
                // ^^ FilterListExample
                n++;
                // vv FilterListExample
            }
        }
        scanner2.close();
        // ^^ FilterListExample
        System.out.println("Total cell count for scan #2: " + n);
    }

    @Test
    public void testPageScan() throws Exception {
        Configuration conf = getConfig();

        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf("testtable"));

        Scan scan = new Scan();

        Filter filter = new PageFilter(100l);

        scan.setFilter(filter);

        ResultScanner rs = table.getScanner(scan);
        printlnResultScanner(rs);
        rs.close();
        connection.close();
    }

    @Test
    public void testScanRowFilter() throws IOException {
        Configuration conf = getConfig();
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf("testtable"));

        Scan scan = new Scan();
        Filter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(200)));
        scan.setFilter(rowFilter);
        ResultScanner rs = table.getScanner(scan);
        printlnResultScanner(rs);
        rs.close();
        connection.close();
    }

    @Test
    public void testDropTable() throws IOException {
        Configuration conf = getConfig();

        HBaseHelper helper = HBaseHelper.getHelper(conf);
        helper.dropTable("testtable");
    }

    @Test
    public void testCreateTable() throws IOException {
        Configuration conf = getConfig();

        HBaseHelper helper = HBaseHelper.getHelper(conf);
        helper.createTable("testtable", "cf1", "cf2");
    }

    private static Configuration getConfig() {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "e160");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        return conf;
    }

    private static void printlnResultScanner(ResultScanner rs) {
        long n = 0;
        for (Result result : rs) {
            n ++;
            System.out.println("======== row :" + n);

            for (Cell cell : result.rawCells()) {
                System.out.println("Cell: " + cell + ", Value: " +
                        Bytes.toString(cell.getValueArray(), cell.getValueOffset(),
                                cell.getValueLength()));
            }
        }
        System.out.println(" 总数 为: " + n);
    }

    @Test
    public void test10() {
        byte[] bs = Bytes.toBytes("h");

        System.out.println(Bytes.toInt(bs));
    }

    @Test
    public void initData() throws IOException {
        // vv CRUDExample
        Configuration conf = getConfig();

        // vv CRUDExample
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf("testtable"));

        //
        connection.getAdmin().listTableNames();

        for(int i=3; i<1000; i++) {
            Put put2 = new Put(Bytes.toBytes(i));
            put2.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("name"),
                    Bytes.toBytes("name-0" + i));
            put2.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("age"),
                    Bytes.toBytes((20 + i)%90));
            put2.addColumn(Bytes.toBytes("cf2"), Bytes.toBytes("addr"),
                    Bytes.toBytes("addr-0" + i));
            table.put(put2);
        }

        table.close();
    }
}
