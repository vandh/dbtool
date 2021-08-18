package org.tool.db;


import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;


/**
 * @author seven
 * @since 07.03.2013
 */
public class BatchInsert2 extends Thread{
	int batchSize = 5000;
	String filepath;
	String charset;
    int skip;
    int end;

    public BatchInsert2(String[] args) {
        this.filepath = args[0];
        this.charset = args[1];
        this.skip = Integer.parseInt(args[2]);
        this.end  = Integer.parseInt(args[3]);
    }

    @Override
    public void run() {
        Connection conn = null;
        Statement st = null;
        int result = 0;
        try {
            conn = JDBCConnector.getConnection();
            conn.setAutoCommit(false);
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
                System.out.println("exec insert sql from "+fpath);

                BufferedReader fr = new BufferedReader(new InputStreamReader(new FileInputStream(fpath), charset));
                String sql = null;
                int line = 0;
                while((sql = fr.readLine()) != null) {
                	line++;
                	if(line<=skip) {
                	    continue;
                    }
                	if(line > end) {
                        st.executeBatch();
                        conn.commit();
                        break;
                    }
                	//st.executeUpdate(sql);
                	st.addBatch(sql);
                	if(line % batchSize == 0) {
                        st.executeBatch();
                	    conn.commit();
                	    st.clearBatch();
                        System.out.println(line+"行 inserted!");
                    }
                }
                System.out.println("inserting "+ line + " rows data into table cost "+(System.currentTimeMillis()-start)/1000+"s!" );
            }
            st.executeBatch();
            conn.commit();
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

    public static void main(String[] args) {
        String f = "C:\\Users\\vandh\\Desktop\\a";
        String c = "GBK";
//        new BatchInsert2(new String[]{f, c, "70001", "100000"}).start();
//        new BatchInsert2(new String[]{f, c, "100001", "200000"}).start();
//        new BatchInsert2(new String[]{f, c, "100001", "200000"}).start();
//        new BatchInsert2(new String[]{f, c, "100001", "200000"}).start();
//        new BatchInsert2(new String[]{f, c, "100001", "200000"}).start();
    }

}
