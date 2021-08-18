package org.tool.db;


import java.io.File;
import java.io.FileOutputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


/**
 * @author seven
 * @since 07.03.2013
 */
public class Export2SQL {
    int pageCount = 30000; //一次查询条数
    String ip;
    String port;
    String dbname;
    String username;
    String password;
    String filepath;
    String table;
    String fgf = ",";
    String hhf = ";\n";
    String charset = "utf8";

    public Export2SQL(String[] args) {
        this.ip = args[1];
        this.port = args[2];
        this.dbname = args[3];
        this.username = args[4];
        this.password = args[5];
        this.filepath = args[6];
        this.table = args[7].toUpperCase();
        this.charset = args[8];
    }

    private Connection getConnection() {
        return JDBCConnector.getConnection();
    }

    public void export() {
        Connection conn = null;
        Statement st = null;
        try{
            conn = getConnection();
            st = conn.createStatement();
            if(table.trim().startsWith("-") || table.trim().startsWith("+")) {
                exportAll(conn, st);
            } else {
                exportOne(conn, st, table);
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        } finally {
            try {
                st.close();
                conn.close();
            }catch(Exception e) {}
        }

    }

    public void exportAll(Connection conn, Statement st) {

        ResultSet rs = null;
        try {
            File dir = new File(filepath);
            if(!dir.exists()) {
                System.out.println(dir+" not exists!");
                System.exit(0);
            }


            int rowCount = 0;
            rs = st.executeQuery("SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA ='" + dbname+"'");
            List<String> tableList = new ArrayList<>();
            if(rs.next()) {
                String t = rs.getString(1);
                tableList.add(t);
                while(rs.next()) {
                    tableList.add(rs.getString(1));
                }
            }else {
                System.out.println(dbname+" not exists, please resume assign TABLE_SCHEMA now!");
                System.exit(0);
            }
            rs.close();
            //-(t_*,test*,ap*,apgl*,appre*,apstd*,*_log)
            String[] tabs = null;
            String isNesary = table.substring(0,1);  //+表示要，-表示过滤
            if(table.length()>3) {
                tabs = table.substring(table.indexOf("(")+1, table.lastIndexOf(")")).split(",");
            }
            for(String t : tableList) {
                boolean isFilter = false;
                if(tabs==null) {
                    exportOne(conn, st, t);
                } else {
                    for (String t2 : tabs) {
                        if ((t2.endsWith("*") && t.toUpperCase().startsWith(t2.substring(0, t2.length() - 1).toUpperCase())) ||
                                (t2.startsWith("*") && t.toUpperCase().endsWith(t2.substring(1).toUpperCase()))) {
                            isFilter=true;
                            break;
                        }
                    }
                    if(isNesary.equals("-") && isFilter) {
                        continue;
                    } else if((isNesary.equals("-") && !isFilter) || (isNesary.equals("+") && isFilter)) {
                        exportOne(conn, st, t);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    private void exportOne(Connection conn, Statement st, String one) {
        FileOutputStream fos = null;
        ResultSet rs = null;
        String[] colIdx = {};
        //1.加载驱动程序
        try {
            String[] arr = one.split("\\|");
            if(arr.length > 1) {
                one = arr[0];
                colIdx = arr[1].split(",");
            }
            try {
                rs = st.executeQuery("select count(1) from " + one);
            }catch (Exception e) {
                System.out.println("table not exists, create table "+one+" now!");
                System.exit(0);
            }
            fos = new FileOutputStream(filepath+File.separator+one+".sql");
            int rowCount = 0;

            StringBuilder colNames = new StringBuilder();
            if(rs.next()) {
                rowCount = rs.getInt(1);
            }
            rs.close();

            rs = st.executeQuery("select * from " + one + " limit 0");
            int colCount = rs.getMetaData().getColumnCount();
            ResultSetMetaData rsm = rs.getMetaData();
            for(int i=1; i<=colCount; i++) {
                if(Arrays.asList(colIdx).contains(i+"")) {
                    continue;
                }
                colNames.append(rsm.getColumnName(i));
                if(i<colCount) {
                    colNames.append(",");
                }
            }

            int pages = rowCount%pageCount == 0 ? rowCount/pageCount : rowCount/pageCount + 1;
            System.out.println(one+"有记录"+rowCount+"行，分"+pages+"次导出！");
            long t1 = System.currentTimeMillis();
            for(int i=0; i<pages; i++) {
                StringBuilder sb = new StringBuilder();
                int start = i*pageCount;
                String sql = "select * from " + one + " limit "+start+","+pageCount;
                rs = st.executeQuery(sql);

                int index = 0;
                while (rs.next()) {
                    index++;
                    sb.append("insert into ").append(one).append("(").append(colNames.toString()).append(")").append(" values(");
                    for(int j=1; j<=colCount; j++) {
                        if(Arrays.asList(colIdx).contains(j+"")) {
                            continue;
                        }
                        String col = rs.getObject(j)==null || "null".equalsIgnoreCase(rs.getObject(j).toString().trim()) || "".equalsIgnoreCase(rs.getObject(j).toString().trim()) ? "" : rs.getObject(j).toString();
                        col = col.replaceAll("\r\n","").replaceAll("\r","").replaceAll("\n","").replaceAll(",","").replaceAll("^","").replaceAll("'","");
                        if("".equals(col)) {
                            sb.append("null");
                        } else {
                            sb.append("'").append(col).append("'");
                        }
                        if(j<colCount) {
                            sb.append(fgf);
                        }
                    }
                    sb.append(")").append(hhf);
                }
                fos.write(sb.toString().getBytes(charset));
                long t2 = System.currentTimeMillis();
                System.out.println("...已导出"+index+"条记录, 耗时 "+((t2-t1)/1000)+" s！");
                sb = null;
            }
            long t2 = System.currentTimeMillis();
            System.out.println("成功导出"+rowCount+"条记录, 共耗时 "+((t2-t1)/1000)+" s！");

        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        } finally {
            try {
                fos.close();
                rs.close();
            }catch(Exception e) {}
        }
    }

}
