package org.tool.db;


import java.io.*;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;


/**
 * @author seven
 * @since 07.03.2013
 */
public class Export2File {
    int pageCount = 20000; //一次查询条数
    boolean isProxy;
    String ip;
    String port;
    String dbname;
    String username;
    String password;
    String filepath;
    String table;
    String fgf = ",";
    String hhf = "\n";
    String charset = "utf8";

    public Export2File(String[] args) {
        this.isProxy = args[0].trim().equals("-le");
        this.ip = args[1];
        this.port = args[2];
        this.dbname = args[3];
        this.username = args[4];
        this.password = args[5];
        this.filepath = args[6];
        this.table = args[7].toUpperCase();
        if(args.length>8) {
        	charset = args[8];
        	fgf = args[9];
        	hhf = args[10];
        }
    }

    public void exportAll() {
        Connection conn = null;
        Statement st = null;
        ResultSet rs = null;
        try {
            File dir = new File(filepath);
            if(!dir.exists()) {
                System.out.println(dir+" not exists!");
                System.exit(0);
            }

            conn = isProxy ? JDBCConnector.getConnectionForWindows() : JDBCConnector.getConnection();
            st = conn.createStatement();
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
        } finally {
            try {
                st.close();
                conn.close();
            }catch(Exception e) {}
        }
    }

    private void exportOne(Connection conn, Statement st, String one) {
        FileOutputStream fos = null;
        ResultSet rs = null;
        //1.加载驱动程序
        try {
            try {
                rs = st.executeQuery("select count(1) from " + one);
            }catch (Exception e) {
                System.out.println("table not exists, create table "+one+" now!");
                System.exit(0);
            }
            fos = new FileOutputStream(filepath+File.separator+one+".csv");
            int rowCount = 0;
            if(rs.next()) {
                rowCount = rs.getInt(1);
            }
            rs.close();
            int pages = rowCount%pageCount == 0 ? rowCount/pageCount : rowCount/pageCount + 1;
            //pages = 1;
            System.out.println(one+"有记录"+rowCount+"行，分"+pages+"次导出！");
            long t1 = System.currentTimeMillis();
            for(int i=0; i<pages; i++) {
                StringBuilder sb = new StringBuilder();
                int start = i*pageCount;
                String sql = "select * from " + one + " limit "+start+","+pageCount;
                rs = st.executeQuery(sql);
                int colCount = rs.getMetaData().getColumnCount();
                int index = 0;
                while (rs.next()) {
                    index++;
                    for(int j=1; j<=colCount; j++) {
                        sb.append(rs.getObject(j)==null || "null".equalsIgnoreCase(rs.getObject(j).toString().trim()) || "".equalsIgnoreCase(rs.getObject(j).toString().trim()) ? "" : rs.getObject(j));
                        if(j<colCount) {
                            sb.append(fgf);
                        }
                    }
                    sb.append(hhf);
                }
                fos.write(sb.toString().getBytes(charset));
                long t2 = System.currentTimeMillis();
                System.out.println("...已导出"+index+"条记录, 耗时 "+((t2-t1)/1000)+" s！");
            }
            long t2 = System.currentTimeMillis();
            System.out.println("成功导出"+rowCount+"条记录, 共耗时 "+((t2-t1)/1000)+" s！");

        } catch (Exception e) {
            System.out.println("导出表出错："+one);
            e.printStackTrace();
            //throw new RuntimeException(e);
        } finally {
            try {
                fos.close();
                rs.close();
            }catch(Exception e) {}
        }
    }

    public void export() {
        Connection conn = null;
        Statement st = null;
        FileOutputStream fos = null;
        int result = 0;
        //1.加载驱动程序
        try {
            conn = isProxy ? JDBCConnector.getConnectionForWindows() : JDBCConnector.getConnection();
            st = conn.createStatement();
            ResultSet rs = null;
            try {
                rs = st.executeQuery("select count(1) from " + table);
            }catch (Exception e) {
                System.out.println("table not exists, create table "+table+" now!");
                System.exit(0);
            }

            File dir = new File(filepath);
            if(!dir.exists()) {
                System.out.println(dir+" not exists!");
                System.exit(0);
            }

            fos = new FileOutputStream(filepath+File.separator+table+".csv");
            int rowCount = 0;
            if(rs.next()) {
                rowCount = rs.getInt(1);
            }
            rs.close();
            int pages = rowCount%pageCount == 0 ? rowCount/pageCount : rowCount/pageCount + 1;
            //pages = 1;
            System.out.println(table+"有记录"+rowCount+"行，分"+pages+"次导出！");
            long t1 = System.currentTimeMillis();
            for(int i=0; i<pages; i++) {
                StringBuilder sb = new StringBuilder();
                int start = i*pageCount;
                String sql = "select * from " + table + " limit "+start+","+pageCount;
                rs = st.executeQuery(sql);
                int colCount = rs.getMetaData().getColumnCount();
                int index = 0;
                while (rs.next()) {
                    index++;
                    for(int j=1; j<=colCount; j++) {
                        String col = rs.getObject(j)==null || "null".equalsIgnoreCase(rs.getObject(j).toString().trim()) || "".equalsIgnoreCase(rs.getObject(j).toString().trim()) ? "" : rs.getObject(j).toString();
                        col = col.replaceAll("\r\n","").replaceAll("\r","").replaceAll("\n","").replaceAll(",","").replaceAll("^","");
                        sb.append(col);
                        if(j<colCount) {
                            sb.append(fgf);
                        }
                    }
                    sb.append(hhf);
                }
                fos.write(sb.toString().getBytes(charset));
                rs.close();
                long t2 = System.currentTimeMillis();
                System.out.println("...累计导出"+((i+1)*index)+"条记录, 累计耗时 "+((t2-t1)/1000)+" s！");
            }
            long t2 = System.currentTimeMillis();
            System.out.println("成功导出"+rowCount+"条记录, 共耗时 "+((t2-t1)/1000)+" s！");

        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        } finally {
            try {
                fos.close();
                st.close();
                conn.close();
            }catch(Exception e) {}
        }
    }

}
