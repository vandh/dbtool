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
public class BatchInsert {
	int batchSize = 10000;
    String ip;
    String port;
    String dbname;
    String username;
    String password;
    String filepath;
    int skip;
    String charset;

    public BatchInsert(String[] args) {
        if(args.length < 9) {
            System.out.println("param must be : DB %ip %port %dbname %username %passowrd %filepath %skip %charset");
            System.exit(0);
        }
        this.ip = args[1];
        this.port = args[2];
        this.dbname = args[3];
        this.username = args[4];
        this.password = args[5];
        this.filepath = args[6];
        this.skip = Integer.parseInt(args[7]);
        this.charset = args[8];
    }

    public void insert() {
        Connection conn = null;
        Statement st = null;
        int result = 0;
        try {
            conn = DriverManager.getConnection("jdbc:mysql://"+ip+":"+port+"/"+dbname+"?useUnicode=true&characterEncoding=UTF-8&useSSL=false&serverTimezone=Asia/Shanghai&autoReconnect=true&connectTimeout=0",
                    username, password);
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
                	while(!sql.endsWith(";")) {
                	    sql += fr.readLine();
                    }
                	if(line<=skip) {
                	    continue;
                    }
                	st.addBatch(sql);
                	if(line % batchSize == 0) {
                	    st.executeBatch();
                	    conn.commit();
                        st.clearBatch();
                        System.out.println("has inserted "+ line + " by exec insert sql from "+fpath);
                    }
                }
                st.executeBatch();
                conn.commit();
                st.clearBatch();
                System.out.println("inserting "+ line + " rows data into table cost "+(System.currentTimeMillis()-start)/1000+"s!" );
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
