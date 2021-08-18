package org.tool.db;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Properties;

public class JDBCConnector {
	/**
	 * 通用于linux操作系统和windows，但不能设置代理
	 * @return
	 */
	public static Connection getConnection() {
		String url = "jdbc:mysql://"+Constants.ip+":"+Constants.port+"/"+Constants.dbname+"?useUnicode=true&characterEncoding=UTF-8&useSSL=false&serverTimezone=Asia/Shanghai&autoReconnect=true&connectTimeout=0";
		Connection conn = null;
		try {
			Class.forName("com.mysql.jdbc.Driver");
		} catch (Exception e1) {
			try{
				Class.forName("com.mysql.cj.jdbc.Driver");
			}catch(Exception e) {
				e.printStackTrace();
				throw new RuntimeException(e);
			}
		}
		try{
			conn = DriverManager.getConnection(url, Constants.username, Constants.password);
		}catch(Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
		System.out.println("connect successful from "+url);
		return conn;
	}

	/**
	 * 仅使用于windows，但可设置代理
	 * @return
	 */
	public static Connection getConnectionForWindows() {
		Properties p = new Properties();
		if(Constants.proxyHost!=null) {
			p.setProperty( "socksProxyHost" ,  Constants.proxyHost);
			p.setProperty( "socksProxyPort" ,  Constants.proxyPort);
		}
		p.setProperty("user", Constants.username);
		p.setProperty("password", Constants.password);
		String url = "jdbc:mysql://"+Constants.ip+":"+Constants.port+"/"+Constants.dbname+"?useUnicode=true&characterEncoding=UTF-8&useSSL=false&serverTimezone=Asia/Shanghai&autoReconnect=true&connectTimeout=0";
		Connection conn = null;
		try {
			Class.forName("com.mysql.jdbc.Driver");
		} catch (Exception e1) {
			try{
				Class.forName("com.mysql.cj.jdbc.Driver");
			}catch(Exception e) {
				e.printStackTrace();
				throw new RuntimeException(e);
			}
		}
		try{
			conn = DriverManager.getConnection(url, p);
		}catch(Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
		System.out.println("connect successful from "+url);
		return conn;
	}


}
