  ------------------------------------------------------
    Everything produced by Simon is competitive product!
                    Simon出品，必属精品！                   
             copyright(2021.03.10-2021.08.06)           
  ------------------------------------------------------
usage:
    -sql [options]：导出数据到sql文件，表名|列号可以跳过，参数列表：
        [ip port dbname username passowrd filepath tablename charset]
        例1：导出表sys_config，跳过第1、2列，以utf8格式写入/data/sys_config.sql
        [-sql 10.238.25.109 10075 rtp_com_db  root *** /data/ sys_config|1,2 utf8]
        例2：导出rtp_com_db库中所有的表，排除t_,ap开头，_log,1结尾的表，以utf8格式写入/data/${table}.sql
        [-sql 10.238.25.109 10075 rtp_com_db  root *** /data/ -(t_*,test*,*_log,*1,*_bak) utf8]
        例3：导出rtp_com_db库中包含sys_,rtp_开头，_data结尾的表，以utf8格式写入/data/${table}.sql
        [-sql 10.238.25.109 10075 rtp_com_db  root *** /data/ +(sys_*,rtp_*,*_data) utf8]
-lsql [options]：通过代理导出数据到sql文件，参数列表与用法参考 -sql：
      [ip port dbname username passowrd filepath tablename charset proxyHost proxyPort]
-e [options]：导出数据到csv文件，参数列表：
    [ip port dbname username passowrd filepath tablename charset col-delim row-delim]
    例1：导出表sys_config，以utf8格式写入/data/sys_config.csv，列分隔符, 行分隔符\n
    [-e 10.238.25.109 10075 rtp_com_db  root *** /data/ sys_config utf8 , \n]
    例2：导出rtp_com_db库中所有的表，排除t_,ap开头，_log,1结尾的表，以utf8格式写入/data/${table}.csv
    [-e 10.238.25.109 10075 rtp_com_db  root *** /data/ -(t_*,test*,*_log,*1,*_bak) utf8 , \n]
    例3：导出rtp_com_db库中包含sys_,rtp_开头，_data结尾的表，以utf8格式写入/data/${table}.csv
    [-e 10.238.25.109 10075 rtp_com_db  root *** /data/ +(sys_*,rtp_*,*_data) utf8 , \n]
-le [options]：通过代理导出数据到csv文件，参数列表与用法参考 -e：
    [ip port dbname username passowrd filepath tablename charset col-delim row-delim proxyHost proxyPort]
-l [options]：导入csv文件到数据库，参数列表：
    [ip port dbname username passowrd filepath tablename charset col-delim row-delim]
-ll [options]：通过代理导入csv文件到数据库，参数列表：
    [ip port dbname username passowrd filepath tablename charset col-delim row-delim proxyHost proxyPort]
-i [options]：批量执行insert sql文件，支持一条sql跨多行，参数列表：
    [ip port dbname username passowrd filepath skip charset]
    例1：导入/data目录下所有的sql文件到指定的数据库，跳过第1行
    [-i 10.238.25.109 10075 rtp_com_db  root *** /data/ 1 gbk]
-t [options]：将in-filepath指定的sql文件转换成utf8格式的csv文件，参数列表：
    [in-filepath charset out-filepath col-delim row-delim]
    例1：将sys_user.sql文件转换成以，分隔列，\n为行分隔的sys_user.csv
    [-t /data/sys_user.sql gbk /data/sys_user.csv , \n]
