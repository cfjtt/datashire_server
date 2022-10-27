package com.eurlanda.test;

import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by zhudebin on 15-7-29.
 */
public class DBTest {

    /**
     * <P>Only column descriptions matching the catalog, schema, table
     * and column name criteria are returned.  They are ordered by
     * <code>TABLE_CAT</code>,<code>TABLE_SCHEM</code>,
     * <code>TABLE_NAME</code>, and <code>ORDINAL_POSITION</code>.
     *
     * <P>Each column description has the following columns:
     *  <OL>
     *  <LI><B>TABLE_CAT</B> String => table catalog (may be <code>null</code>)
     *  <LI><B>TABLE_SCHEM</B> String => table schema (may be <code>null</code>)
     *  <LI><B>TABLE_NAME</B> String => table name
     *  <LI><B>COLUMN_NAME</B> String => column name
     *  <LI><B>DATA_TYPE</B> int => SQL type from java.sql.Types
     *  <LI><B>TYPE_NAME</B> String => Data source dependent type name,
     *  for a UDT the type name is fully qualified
     *  <LI><B>COLUMN_SIZE</B> int => column size.
     *  <LI><B>BUFFER_LENGTH</B> is not used.
     *  <LI><B>DECIMAL_DIGITS</B> int => the number of fractional digits. Null is returned for data types where
     * DECIMAL_DIGITS is not applicable.
     *  <LI><B>NUM_PREC_RADIX</B> int => Radix (typically either 10 or 2)
     *  <LI><B>NULLABLE</B> int => is NULL allowed.
     *      <UL>
     *      <LI> columnNoNulls - might not allow <code>NULL</code> values
     *      <LI> columnNullable - definitely allows <code>NULL</code> values
     *      <LI> columnNullableUnknown - nullability unknown
     *      </UL>
     *  <LI><B>REMARKS</B> String => comment describing column (may be <code>null</code>)
     *  <LI><B>COLUMN_DEF</B> String => default value for the column, which should be interpreted as a string when the value is enclosed in single quotes (may be <code>null</code>)
     *  <LI><B>SQL_DATA_TYPE</B> int => unused
     *  <LI><B>SQL_DATETIME_SUB</B> int => unused
     *  <LI><B>CHAR_OCTET_LENGTH</B> int => for char types the
     *       maximum number of bytes in the column
     *  <LI><B>ORDINAL_POSITION</B> int => index of column in table
     *      (starting at 1)
     *  <LI><B>IS_NULLABLE</B> String  => ISO rules are used to determine the nullability for a column.
     *       <UL>
     *       <LI> YES           --- if the column can include NULLs
     *       <LI> NO            --- if the column cannot include NULLs
     *       <LI> empty string  --- if the nullability for the
     * column is unknown
     *       </UL>
     *  <LI><B>SCOPE_CATALOG</B> String => catalog of table that is the scope
     *      of a reference attribute (<code>null</code> if DATA_TYPE isn't REF)
     *  <LI><B>SCOPE_SCHEMA</B> String => schema of table that is the scope
     *      of a reference attribute (<code>null</code> if the DATA_TYPE isn't REF)
     *  <LI><B>SCOPE_TABLE</B> String => table name that this the scope
     *      of a reference attribute (<code>null</code> if the DATA_TYPE isn't REF)
     *  <LI><B>SOURCE_DATA_TYPE</B> short => source type of a distinct type or user-generated
     *      Ref type, SQL type from java.sql.Types (<code>null</code> if DATA_TYPE
     *      isn't DISTINCT or user-generated REF)
     *   <LI><B>IS_AUTOINCREMENT</B> String  => Indicates whether this column is auto incremented
     *       <UL>
     *       <LI> YES           --- if the column is auto incremented
     *       <LI> NO            --- if the column is not auto incremented
     *       <LI> empty string  --- if it cannot be determined whether the column is auto incremented
     *       </UL>
     *   <LI><B>IS_GENERATEDCOLUMN</B> String  => Indicates whether this is a generated column
     *       <UL>
     *       <LI> YES           --- if this a generated column
     *       <LI> NO            --- if this not a generated column
     *       <LI> empty string  --- if it cannot be determined whether this is a generated column
     *       </UL>
     *  </OL>
     *
     * <p>The COLUMN_SIZE column specifies the column size for the given column.
     * For numeric data, this is the maximum precision.  For character data, this is the length in characters.
     * For datetime datatypes, this is the length in characters of the String representation (assuming the
     * maximum allowed precision of the fractional seconds component). For binary data, this is the length in bytes.  For the ROWID datatype,
     * this is the length in bytes. Null is returned for data types where the
     * column size is not applicable.
     *
     * @throws Exception
     */
    @Test
    public void testColumnMeta() throws Exception {

        Map<Integer, String> infoMap = new HashMap<>();
        infoMap.put(1, "TABLE_CAT");
        infoMap.put(2, "TABLE_SCHEM");
        infoMap.put(3, "TABLE_NAME");
        infoMap.put(4, "COLUMN_NAME");
        infoMap.put(5, "DATA_TYPE");
        infoMap.put(6, "TYPE_NAME");
        infoMap.put(7, "COLUMN_SIZE");
        infoMap.put(8, "BUFFER_LENGTH");
        infoMap.put(9, "DECIMAL_DIGITS");
        infoMap.put(10, "NUM_PREC_RADIX");
        infoMap.put(11, "NULLABLE");
        infoMap.put(12, "REMARKS");
        infoMap.put(13, "COLUMN_DEF");
        infoMap.put(14, "SQL_DATA_TYPE");
        infoMap.put(15, "SQL_DATETIME_SUB");
        infoMap.put(16, "CHAR_OCTET_LENGTH");
        infoMap.put(17, "ORDINAL_POSITION");
        infoMap.put(18, "IS_NULLABLE");
        infoMap.put(19, "SCOPE_CATALOG");
        infoMap.put(20, "SCOPE_SCHEMA");
        infoMap.put(21, "SCOPE_TABLE");
        infoMap.put(22, "SOURCE_DATA_TYPE");
        infoMap.put(23, "IS_AUTOINCREMENT");
        infoMap.put(24, "IS_GENERATEDCOLUMN");

        Class.forName("com.mysql.jdbc.Driver");
        Connection con = DriverManager.getConnection("jdbc:mysql://192.168.137.4:3306/demodb", "root", "root");

        ResultSet rs = con.getMetaData().getColumns(null, null, null, null);

        while (rs.next()) {

            System.out.println("------------------ column --------------");

            ResultSetMetaData rmd = rs.getMetaData();

            int count = rmd.getColumnCount();

            for(int i=1; i<= count; i++) {
                String columnName = rmd.getColumnName(i);
                System.out.println("\t--" + columnName + "\t---" + rs.getObject(columnName));
            }

            /**
            for(String key : infoMap.values()) {
                System.out.println("\t" + key + "\t" + rs.getObject(key));
            }
             */

            System.out.println();
        }
    }

    /**
     * 测试sqlserver 的数据类型，
     * 4的驱动，现在会把date类型的返回为字符串类型
     */
    @Test
    public void testSqlserverDataType() throws Exception {
        Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        Connection con = DriverManager.getConnection("jdbc:sqlserver://192.168.137.1:1433;DatabaseName=AdventureWorks2008", "sa", "squiding@eurlanda");

        ResultSet rs = con.createStatement().executeQuery("select max(birthdate) from HumanResources.Employee");
        int type = rs.getMetaData().getColumnType(1);
        if(type == Types.NVARCHAR) {
            System.out.println(" string ");
        }
    }

}
