package org.tool.db;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * del dbtool.jar && jar cvfe dbtool.jar org.tool.db.Main -C bin/ .
 * java -jar dbtool.jar -e 192.168.10.221 3307 db_2 root 123456 d:/var sys_config utf-8  \u0007 \u0006
 * java -cp /data/dwrtp/dbtool.jar:/data/dwrtp/mysql5.1.47.jar org.tool.db.Main -l 10.238.25.109 10075 rtp_com_db  root xFm5iBGK3OO6 /data/ - utf8 \u0007 \u0006
 * java -cp /data/dbtool/mysql5.jar:/data/dbtool/dbtool.jar org.tool.db.Main -i 10.238.25.190 8076 rtp_bus_db root  123456 /data/simon/abc 0 utf8
 * window下自己加的符号为\\u007,\\u006没错，在oracle导出的文件中应变为\\007,\\006
 * 实际处理： cat -A abc > a2; sed -i 's/\^F/\n/g' a2;sed -i 's/\^G/,/g' a2;  load -l , \\n; 注意必须是\\n，否则进不去
 * java -cp /data/dbtool/mysql5.jar:/data/dbtool/dbtool.jar org.tool.db.Main -l 10.238.25.109 10072 rtp_dw_db root evBKIA27vUap /data/day/tmp ra_3 gbk \\007 \\006
 * java -cp /data/dbtool/mysql5.jar:/data/dbtool/dbtool.jar org.tool.db.Main -l 10.238.25.109 10072 rtp_dw_db root evBKIA27vUap /data/testra/RA-50 ra_bak utf8 \\u0007 \\n
 *
 * flink run -c com.jw.plat.Main2 /data/dbtool/data-flink-1.0-SNAPSHOT.jar DB 10.238.25.109 10072 rtp_dw_db root evBKIA27vUap /data/day/tmp ra_3
 * java -cp /data/dbtool/data-flink-1.0-SNAPSHOT.jar com.jw.plat.Main2 DB 10.238.25.109 10072 rtp_dw_db root evBKIA27vUap /data/day/tmp ra_3
 */
public class Main {
    public static void main(String[] args) throws Exception {
        System.out.println("  ------------------------------------------------------");
        System.out.println("    Everything produced by Simon is competitive product!\n                    Simon出品，必属精品！                   ");
        System.out.println("             copyright(2021.03.10-2021.08.06)           ");
        System.out.println("  ------------------------------------------------------");
        long t1 = System.currentTimeMillis();
//        args = new String[]{"-le","10.238.25.109","10075","rtp_com_db","root","xFm5iBGK3OO6","D:/project/2021","-(*_log,*1,*2,t_*)","utf-8","\u0007","\u0006", "192.168.10.210", "1080"};
//        args = new String[]{"-le","10.238.25.109","10075","rtp_com_db","root","xFm5iBGK3OO6","D:/project/2021","sys_tenant","utf-8","\u0007","\u0006", "192.168.10.210", "1080"};
//        args = new String[]{"-le","10.238.25.109","10086","rtp_bus_db","root","2Fse8Ubm7kDZ","D:/project/2021/exit/bus","+(rtp_*,t_*)","utf-8","\u0007","\u0006", "192.168.10.210", "1080"};
//        args = new String[]{"-le","10.238.25.109","10086","rtp_bus_db","root","2Fse8Ubm7kDZ","D:/project/2021/exit/bus","rtp_bus_payment_info","utf-8","\u0007","\u0006", "192.168.10.210", "1080"};
//        args = new String[]{"-le","10.238.25.109","10075","rtp_com_db","root","xFm5iBGK3OO6","d:/var","+(sys_config*)","utf-8","\u0007","\u0006", "192.168.10.210", "1080"};
//        args = new String[]{"-sql","192.168.10.221","3307","db_2","root","123456","d:/var","-(t_*,test*,ap*,apgl*,appre*,apstd*,*_log,*1,*2,*3,*4,*5,*_bak)","utf-8"};
//        args = new String[]{"-e","127.0.0.1","3306","aijwdb","root","123456","d:/var","t_inf_partner","utf-8",",","^"};
//        args = new String[]{"-l","192.168.10.221","3307","db_3","root","123456","d:/var","-","utf8","\u0007","\u0006"};
//        args = new String[]{"-i","192.168.10.221","3307","db_2","root","123456","d:/var2","0","utf8"};
//        args = new String[]{"-ll","localhost","3306","rtp_bus_db","root","123456","D:/project/2021/exit/bus","-","utf8","\u0007","\u0006", "192.168.10.210", "1080"};
        args = new String[]{"-ll","10.238.25.109","10072","rtp_dw_db","root","evBKIA27vUap","D:\\workspace\\tools\\exportdb\\data","ra2","utf8","\u0007","\u0006", "192.168.10.210", "1080"};

        if(args==null || args.length==0 || "-sql -lsql -e -le -l -ll -i -t".indexOf(args[0])==-1 ||
                (args[0].equals("-e") && args.length!=11) ||
                (args[0].equals("-le") && args.length!=13) ||
                (args[0].equals("-sql") && args.length!=9) ||
                (args[0].equals("-lsql") && args.length!=11) ||
                (args[0].equals("-l") && args.length!=11) ||
                (args[0].equals("-ll") && args.length!=13) ||
                (args[0].equals("-i") && args.length!=9) ||
                (args[0].equals("-t") && args.length!=4) ) {
            System.out.println("  usage: ");
            System.out.println("  \t -sql [options]：导出数据到sql文件，表名|列号可以跳过，参数列表：");
            System.out.println("   \t\t[ip port dbname username passowrd filepath tablename charset]");
            System.out.println("   \t\t例1：导出表sys_config，跳过第1、2列，以utf8格式写入/data/sys_config.sql");
            System.out.println("   \t\t[-sql 10.238.25.109 10075 rtp_com_db  root *** /data/ sys_config|1,2 utf8]");
            System.out.println("   \t\t例2：导出rtp_com_db库中所有的表，排除t_,ap开头，_log,1结尾的表，以utf8格式写入/data/${table}.sql");
            System.out.println("   \t\t[-sql 10.238.25.109 10075 rtp_com_db  root *** /data/ -(t_*,test*,*_log,*1,*_bak) utf8]");
            System.out.println("   \t\t例3：导出rtp_com_db库中包含sys_,rtp_开头，_data结尾的表，以utf8格式写入/data/${table}.sql");
            System.out.println("   \t\t[-sql 10.238.25.109 10075 rtp_com_db  root *** /data/ +(sys_*,rtp_*,*_data) utf8]");
            System.out.println("  \t -lsql [options]：通过代理导出数据到sql文件，参数列表与用法参考 -sql：");
            System.out.println("   \t\t[ip port dbname username passowrd filepath tablename charset proxyHost proxyPort]");

            System.out.println("  \t -e [options]：导出数据到csv文件，参数列表：");
            System.out.println("   \t\t[ip port dbname username passowrd filepath tablename charset col-delim row-delim]");
            System.out.println("   \t\t例1：导出表sys_config，以utf8格式写入/data/sys_config.csv，列分隔符, 行分隔符\\n");
            System.out.println("   \t\t[-e 10.238.25.109 10075 rtp_com_db  root *** /data/ sys_config utf8 , \\n]");
            System.out.println("   \t\t例2：导出rtp_com_db库中所有的表，排除t_,ap开头，_log,1结尾的表，以utf8格式写入/data/${table}.csv");
            System.out.println("   \t\t[-e 10.238.25.109 10075 rtp_com_db  root *** /data/ -(t_*,test*,*_log,*1,*_bak) utf8 , \\n]");
            System.out.println("   \t\t例3：导出rtp_com_db库中包含sys_,rtp_开头，_data结尾的表，以utf8格式写入/data/${table}.csv");
            System.out.println("   \t\t[-e 10.238.25.109 10075 rtp_com_db  root *** /data/ +(sys_*,rtp_*,*_data) utf8 , \\n]");
            System.out.println("  \t-le [options]：通过代理导出数据到csv文件，参数列表与用法参考 -e：");
            System.out.println("   \t\t[ip port dbname username passowrd filepath tablename charset col-delim row-delim proxyHost proxyPort]");

            System.out.println("  \t-l [options]：导入csv文件到数据库，参数列表：");
            System.out.println("   \t\t[ip port dbname username passowrd filepath tablename charset col-delim row-delim]");
            System.out.println("  \t-ll [options]：通过代理导入csv文件到数据库，参数列表：");
            System.out.println("   \t\t[ip port dbname username passowrd filepath tablename charset col-delim row-delim proxyHost proxyPort]");

            System.out.println("  \t-i [options]：批量执行insert sql文件，支持一条sql跨多行，参数列表：");
            System.out.println("   \t\t[ip port dbname username passowrd filepath skip charset]");
            System.out.println("   \t\t例1：导入/data目录下所有的sql文件到指定的数据库，跳过第1行");
            System.out.println("   \t\t[-i 10.238.25.109 10075 rtp_com_db  root *** /data/ 1 gbk]");

            System.out.println("  \t-t [options]：将in-filepath指定的sql文件转换成utf8格式的csv文件，参数列表：");
            System.out.println("   \t\t[in-filepath charset out-filepath col-delim row-delim]");
            System.out.println("   \t\t例1：将sys_user.sql文件转换成以，分隔列，\\n为行分隔的sys_user.csv");
            System.out.println("   \t\t[-t /data/sys_user.sql gbk /data/sys_user.csv , \\n]");
            System.exit(1);
        }
        System.out.println("开始时间=="+new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));
        Constants.ip = args[1];
        Constants.port = args[2];
        Constants.dbname = args[3];
        Constants.username = args[4];
        Constants.password = args[5];
        if (args[0].equalsIgnoreCase("-e")) {
            Export2File exp = new Export2File(args);
            if(args[7].trim().startsWith("-") || args[7].trim().startsWith("+")) {
                exp.exportAll();
            } else {
                exp.export();
            }
        }else if (args[0].equalsIgnoreCase("-le")) {
            Constants.proxyHost = args[11];
            Constants.proxyPort = args[12];
            Export2File exp = new Export2File(args);
            if(args[7].trim().startsWith("-") || args[7].trim().startsWith("+")) {
                exp.exportAll();
            } else {
                exp.export();
            }
        } else if (args[0].equalsIgnoreCase("-sql")) {
            Export2SQL exp = new Export2SQL(args);
            exp.export();
        } else if (args[0].equalsIgnoreCase("-lsql")) {
            Constants.proxyHost = args[11];
            Constants.proxyPort = args[12];
            Export2SQL exp = new Export2SQL(args);
            exp.export();
        } else if (args[0].equalsIgnoreCase("-l")) {
            LoadData2Mysql loader2 = new LoadData2Mysql(new String[]{"", args[6],args[7],args[8],args[9],args[10]});
            loader2.load();
        } else if (args[0].equalsIgnoreCase("-ll")) {
            Constants.proxyHost = args[11];
            Constants.proxyPort = args[12];
            LoadData2Mysql loader2 = new LoadData2Mysql(new String[]{"", args[6],args[7],args[8],args[9],args[10]});
            loader2.load();
        } else if (args[0].equalsIgnoreCase("-i")) {
            BatchInsert loader = new BatchInsert(args);
            loader.insert();
        } else if (args[0].equalsIgnoreCase("-t")) {
            ETL.sql2csv(args[1], args[2], args[3], args[4], args[5]);
        }
        System.out.println("结束时间=="+new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));
        long t2 = System.currentTimeMillis();
        System.out.println("本次操作用时==="+((t2-t1)/1000)+" s！");
    }
}
