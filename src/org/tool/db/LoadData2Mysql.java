package org.tool.db;


import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;


/**
 * @author seven
 * @since 07.03.2013
 */
public class LoadData2Mysql {
    boolean isProxy;
    String filepath;
    String table;
    String charset = "utf8";
    String fgf = ",";
    String hhf = "\n";

    public LoadData2Mysql(String[] args) {
        this.isProxy = args[0].trim().equals("-ll");
        this.filepath = args[1];
        this.table = args[2].toUpperCase();
        if(args.length>3) {
        	charset = args[3];
        	fgf = args[4];
        	hhf = args[5];
        }
    }

    public void load() {
        if(table.startsWith("-")) {
            loadAll();
        }else {
            loadOne();
        }
    }

    public void loadAll() {
        Connection conn = null;
        Statement st = null;
        try {
            conn = isProxy ? JDBCConnector.getConnectionForWindows() : JDBCConnector.getConnection();
            st = conn.createStatement();

            File dir = new File(filepath);
            if(!dir.exists()) {
                System.out.println(dir+" not exists!");
                System.exit(0);
            }
            File[] files = dir.listFiles();
            if(files.length==0) {
                System.out.println(dir+" has not file more than one!");
                System.exit(0);
            }
            for(int i=0; i<files.length; i++) {
                long start = System.currentTimeMillis();
                String fpath = files[i].getPath().replaceAll("\\\\","/");
                System.out.println("load data from "+fpath);
                String _table = fpath.substring(fpath.lastIndexOf("/")+1, fpath.indexOf("."));
                String tsql = "LOAD DATA LOCAL INFILE '"+fpath +"' IGNORE INTO TABLE "+ _table +" character set "+charset+" fields terminated by '"+fgf+"' LINES TERMINATED BY '"+hhf+"' ";
                System.out.println("SQL===>  " + tsql );
                int rows = st.executeUpdate(tsql);
                st.clearBatch();

                System.out.println("importing "+ rows + " rows data into "+ _table+" cost "+(System.currentTimeMillis()-start)/1000+"s!" );
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        } finally {
            try {
                st.close();
                conn.close();
            }catch(SQLException e) {}
        }
    }

    public void loadOne() {
        Connection conn = null;
        Statement st = null;
        try {
            conn = isProxy ? JDBCConnector.getConnectionForWindows() : JDBCConnector.getConnection();
            st = conn.createStatement();

            File dir = new File(filepath);
            if(!dir.exists()) {
                System.out.println(dir+" not exists!");
                System.exit(0);
            }
            File[] files = dir.listFiles();
            if(files.length==0) {
                System.out.println(dir+" has not file more than one!");
                System.exit(0);
            }
            for(int i=0; i<files.length; i++) {
                long start = System.currentTimeMillis();
                String fpath = files[i].getPath().replaceAll("\\\\","/");
                System.out.println("load data from "+fpath);
              String tsql = "LOAD DATA LOCAL INFILE '"+fpath +"' IGNORE INTO TABLE "+ table +" character set "+charset+" fields terminated by '"+fgf+"' LINES TERMINATED BY '"+hhf+"' ";
//              boolean is7 = false, is6=false;
//              //从linux终端传过来的\u0007不行
//              if(fgf.equals("\u0007") || fgf.equals("\007") || fgf.equals("007") || fgf.equals("\\007")) {
//                  is7 = true;
//                  tsql = "LOAD DATA LOCAL INFILE '"+fpath +"' IGNORE INTO TABLE "+ table +" character set "+charset+" fields terminated by '\u0007' LINES TERMINATED BY '"+hhf+"' ";
//              }
//                if(hhf.equals("\u0006") || hhf.equals("\006") || hhf.equals("006") || hhf.equals("\\006")) {
//                    is6 = true;
//                    tsql = "LOAD DATA LOCAL INFILE '"+fpath +"' IGNORE INTO TABLE "+ table +" character set "+charset+" fields terminated by '\"+fgf+\"' LINES TERMINATED BY '\u0006' ";
//                }
//                if(is7 && is6) {
//                    tsql = "LOAD DATA LOCAL INFILE '"+fpath +"' IGNORE INTO TABLE "+table +" character set utf8 fields terminated by '\\007' LINES TERMINATED BY '\\006' ";
//                }
                System.out.println("SQL===>  " + tsql );
                int rows = st.executeUpdate(tsql);
                st.clearBatch();
                System.out.println("importing "+ rows + " rows data into "+table+" cost "+(System.currentTimeMillis()-start)/1000+"s!" );
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        } finally {
            try {
                st.close();
                conn.close();
            }catch(SQLException e) {}
        }
    }

}
