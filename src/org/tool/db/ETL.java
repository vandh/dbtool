package org.tool.db;

import java.io.*;
import java.nio.charset.StandardCharsets;

public class ETL {
    static String DEL = ","; //"\u0007";
    static String LDF = "^"; //"\u0006";
    private static final String proc(String s) throws Exception{
        String str = s.substring(s.indexOf("VALUES")+8, s.length() -2);
        str = str.replaceAll(LDF, "");
        str = str.replaceAll("\r\n","").replaceAll("\r","").replaceAll("\n","");
        str = str.replaceAll("', '",DEL);
        str = str.replaceAll("', ",DEL);
        str = str.replaceAll(", '",DEL);
        str = str.replaceAll(", ",DEL);
        str = str.replaceAll("'","");
        str = str.replaceAll("null","");
        return str.trim()+LDF;
    }

    public static void sql2csv(String file, String charset, String outFile, String coldel, String linedel) throws Exception {
        BufferedReader fr = null;
        FileOutputStream os = null;
        DEL = coldel == null ? DEL : coldel;
        LDF = linedel == null ? LDF : linedel;

        try {
            fr = new BufferedReader(new InputStreamReader(new FileInputStream(file), charset));
            os = new FileOutputStream(new File(outFile));

            int line = 0;
            String sql = null;
            String sql1 = null;
            int index = 0;

            StringBuilder sb = new StringBuilder();
            while ((sql = fr.readLine()) != null) {
//                if(line<102) {continue;}
                if (sql.startsWith("INSERT") && sql.endsWith(";")) {
//                    System.out.println(clean(sql));
                    sb.append(proc(sql));
                    line++;
                } else if (sql.startsWith("INSERT")) {
                    sql1 = sql;
                } else if (!sql.startsWith("INSERT") && !sql.endsWith(";")) {
                    sql1 += sql;
                } else if (sql.endsWith(";")) {
//                    System.out.println(clean(sql1+sql));
                    sb.append(proc(sql1 + sql));
                    line++;
                }
//                if(line>20) {
//                    break;
//                }
                if (line % 10000 == 0) {
                    os.write(sb.toString().getBytes(StandardCharsets.UTF_8));
                    os.flush();
                    sb.delete(0, sb.length());
                }
            }
            os.write(sb.toString().getBytes(StandardCharsets.UTF_8));
            os.flush();
            sb = null;
        }finally {
            if(os!=null) {
                os.close();
            }
            if(fr!=null) {
                fr.close();
            }
        }
    }

}
