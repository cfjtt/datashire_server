package com.eurlanda.datashire.adapter.db;

import com.eurlanda.datashire.adapter.datatype.DataTypeConvertor;
import com.eurlanda.datashire.entity.DBConnectionInfo;
import com.eurlanda.datashire.entity.operation.BasicTableInfo;
import com.eurlanda.datashire.entity.operation.ColumnInfo;
import com.eurlanda.datashire.enumeration.DataBaseType;
import com.eurlanda.datashire.enumeration.datatype.DbBaseDatatype;
import com.eurlanda.datashire.utility.ReturnValue;
import com.eurlanda.datashire.utility.SQLUtils;
import com.eurlanda.datashire.utility.StringUtils;
import com.eurlanda.datashire.utility.objectsql.TemplateParser;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * oracle
 *
 * @author jiwei.zhang
 * @date 2014-1-21
 */
public class OracleAdapter extends AbsDBAdapter implements INewDBAdapter {
    private String sequenceTemplate = "create sequence ${sequenceName}";
    private String triggerTemplate = "create trigger ${triggerName} before insert on ${tableName} for each row begin select ${sequenceName}.NEXTVAL INTO :new.${columnName} from dual; end;";

    public OracleAdapter(DBConnectionInfo info) {
        super(info);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void createTable(BasicTableInfo table) {
        super.createTable(table, new ReturnValue());
    }

    @Override
    public List<BasicTableInfo> getAllTables(String filter) {
        String sql = "";
        if (StringUtils.isBlank(filter)) {
            sql = "SELECT table_name FROM user_tables UNION ALL  select view_name from user_views UNION ALL select synonym_name from user_synonyms";
        } else {
            sql = "SELECT table_name FROM user_tables where table_name like '"
                    + SQLUtils.sqlserverFilter(filter) + "' UNION ALL  select view_name from user_views where view_name  like '" + SQLUtils.sqlserverFilter(filter) + "' UNION ALL select synonym_name from user_synonyms where synonym_name like '"+SQLUtils.sqlserverFilter(filter)+"'";
        }
        List<Map<String, Object>> results = this.jdbcTemplate.queryForList(sql, null);
        List<BasicTableInfo> tableList = new ArrayList<BasicTableInfo>();

        for (Map<String, Object> tableMap : results) {        // 表
            String tableName = tableMap.get("TABLE_NAME").toString();
            BasicTableInfo tableInfo = new BasicTableInfo(tableName);
            tableList.add(tableInfo);
        }
        return tableList;
    }

    @Override
    public List<ColumnInfo> getTableColumns(String tableName, String DatabaseName) {
        List<ColumnInfo> columnList = new ArrayList<ColumnInfo>();
        //获取所有的同义词，使用实际的表名查询
        String synmSql = "select table_name from user_synonyms where synonym_name=?";
        Map<String,Object> synonymsMap = this.jdbcTemplate.queryForMap(synmSql,null,tableName.toUpperCase());
        if(synonymsMap!=null){
            tableName = synonymsMap.get("TABLE_NAME")+"";
        }
        String colsql = "select * from user_tab_columns where table_name=?";
        List<Map<String, Object>> colList = this.jdbcTemplate.queryForList(colsql, null, tableName.toUpperCase());
        String pkSql = "select cu.* from user_cons_columns cu, user_constraints au where cu.constraint_name = au.constraint_name and  au.constraint_type = 'P' AND cu.table_name = ? ";
        String uniqueSql = "select * from user_cons_columns cu, user_constraints au where cu.constraint_name=au.constraint_name and  au.constraint_type = 'U' and  cu.table_name=? ";
        List<Map<String, Object>> pKList = this.jdbcTemplate.queryForList(pkSql, null, tableName.toUpperCase());
        List<Map<String, Object>> UniqueList = this.jdbcTemplate.queryForList(uniqueSql, null, tableName.toUpperCase());
        for (Map<String, Object> col : colList) {
            ColumnInfo column = new ColumnInfo();
            column.setTableName(tableName);
            //column.setComment(rs.getString("REMARKS"));
            column.setOrderNumber((Integer) col.get("rownum"));
            column.setName(col.get("COLUMN_NAME").toString());
            String typeName = this.getDateType(col.get("DATA_TYPE").toString());
            if (typeName.toUpperCase().contains("INTERVAL")) {
                typeName = "INTERVAL";
            }
            typeName = typeName.replaceAll(" ","").toUpperCase();
            column.setDbBaseDatatype(DbBaseDatatype.parse(typeName));

            column.setTypeName(typeName);
            //column.setLength(((BigDecimal)col.get("DATA_LENGTH")).intValue());
            if (StringUtils.isNotNull(col.get("DATA_LENGTH"))) {
                Long lg = Long.parseLong(col.get("DATA_LENGTH") + "");
                if (lg > Long.parseLong(Integer.MAX_VALUE + "")) {
                    column.setLength(-1);
                } else {
                    column.setLength(col.get("DATA_LENGTH") == null ? 0 : Integer.parseInt(col.get("DATA_LENGTH") + ""));
                }
            }
            if (StringUtils.isNotNull(col.get("CHAR_LENGTH"))) {
                Long lg = Long.parseLong(col.get("CHAR_LENGTH") + "");
                if (lg > Long.parseLong(Integer.MAX_VALUE + "")) {
                    column.setLength(-1);
                } else {
                    if(column.getLength()==0 || column.getLength()==null) {
                        column.setLength(col.get("CHAR_LENGTH") == null ? 0 : Integer.parseInt(col.get("CHAR_LENGTH") + ""));
                    }
                }
            }
            if ("N".equals(col.get("NULLABLE").toString())) {
                column.setNullable(false);
            } else {
                column.setNullable(true);
            }
            if (null != col.get("DATA_PRECISION") && StringUtils.isNotBlank(col.get("DATA_PRECISION").toString())) {
                column.setPrecision(Integer.parseInt(col.get("DATA_PRECISION").toString()));
            } else {
                column.setPrecision(0);
            }
            if (null != col.get("DATA_SCALE") && StringUtils.isNotBlank(col.get("DATA_SCALE").toString())) {
                column.setScale(Integer.parseInt(col.get("DATA_SCALE").toString()));
            } else {
                column.setScale(0);
            }
            //设置主键
            for (Map<String, Object> pk : pKList) {
                if (column.getName().equals(pk.get("COLUMN_NAME").toString())) {
                    column.setPrimary(true);
                    break;
                }
            }
            //设置唯一约束
            for (Map<String, Object> unique : UniqueList) {
                if (column.getName().equals(unique.get("COLUMN_NAME").toString())) {
                    column.setUniqueness(true);
                    break;
                }
            }
            columnList.add(column);
        }

        return columnList;
    }
	
/*	@Override
	public List<Map<String, Object>> queryForList(ObjectSQL objSql) {
		// TODO Auto-generated method stub
		
		return super.queryForList(objSql);
	}*/

    @Override
    protected String buildColumnSql(ColumnInfo col) {
        String column_template = "${colName} ${colType} ${nullable} ${auto_increment}";
        TemplateParser parser = new TemplateParser();
        parser.addParam("colName", "\"" + col.getName() + "\"");

        //String typeStr = DataTypeManager.decode(DataBaseType.HBASE_PHOENIX, col.getSystemDatatype());
        //String typeStr = col.getSystemDatatype().name();
		/*switch (col.getSystemDatatype()) {
		case NVARCHAR:
			parser.addParam("colType", typeStr+"("+col.getLength()+")");
			break;
		default:
			parser.addParam("colType", typeStr);
		}*/
        String typeStr = DataTypeConvertor.getOutTypeByColumn(DataBaseType.ORACLE.value(), col);
        //System.out.println(typeStr);
        parser.addParam("colType", typeStr);
        if (col.isNullable() && !col.isIdentity()) {
            parser.addParam("nullable", "NULL");
        } else {
            parser.addParam("nullable", "NOT NULL");
        }
        String sql = parser.parseTemplate(column_template);
        return sql;
    }

    /**
     * TIMESTAMP类型处理
     *
     * @param dataType
     * @return
     */
    private String getDateType(String dataType) {
        String temp = null;
        if (dataType.indexOf("TIMESTAMP") > -1) {
            temp = "TIMESTAMP";
        } else {
            temp = dataType;
        }
        return temp;
    }

    @Override
    public void deleteIndex(String tableName, String indexName) {
        String temp = "DROP INDEX ${indexName}";
        TemplateParser parser = new TemplateParser();
        parser.addParam("indexName", indexName);
        String sql = parser.parseTemplate(temp);
        this.jdbcTemplate.update(sql);
    }

    public static void main(String[] args) {
        DBConnectionInfo dbs = new DBConnectionInfo();
        dbs.setDbName("orcl");
        //dbs.setDbName("eurlanda");
        dbs.setDbType(DataBaseType.ORACLE);
        dbs.setHost("192.168.137.4:1521");
        dbs.setPort(0);
        dbs.setUserName("MG");
        dbs.setPassword("eurlanda");
        try {
            INewDBAdapter adaoterSource = AdapterDataSourceManager.getAdapter(dbs);
            List<Map<String, Object>> list = adaoterSource.queryForList("select * from MG.MTL_ITEMS");
            if (list != null && list.size() > 0) {
                for (Map<String, Object> map : list) {
                    System.out.println(map);
                }
            }
            adaoterSource.close();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
