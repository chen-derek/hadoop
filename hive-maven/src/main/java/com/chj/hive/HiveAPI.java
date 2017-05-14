package com.chj.hive;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class HiveAPI {

	// 网上写 org.apache.hadoop.hive.jdbc.HiveDriver ,新版本不能这样写
	private static String driverName = "org.apache.hadoop.hive.jdbc.HiveDriver";

	// 这里是hive2，网上其他人都写hive,在高版本中会报错
	private static String url = "jdbc:hive://192.168.2.251:10000/default";
	private static String user = "hive";
	private static String password = "hive";
	private static String sql = "";

	public static ResultSet countData(Statement stmt, String tableName) {
		sql = "select count(1) from " + tableName;
		System.out.println("Running:" + sql);
		ResultSet res = null;
		try {
			res = stmt.executeQuery(sql);
			System.out.println("执行“regular hive query”运行结果:");
			while (res.next()) {
				System.out.println("count ------>" + res.getString(1));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return res;
	}

	public static ResultSet selectData(Statement stmt, String tableName) {
		sql = "select * from " + tableName;
		System.out.println("Running:" + sql);
		ResultSet res = null;
		try {
			res = stmt.executeQuery(sql);
			System.out.println("执行 select * query 运行结果:");
			while (res.next()) {
				System.out.println(res.getInt(1) + "\t" + res.getString(2));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return res;
	}

	public static boolean loadData(Statement stmt, String tableName, String filepath) {
		// 目录 ，我的是hive安装的机子的虚拟机的home目录下
		sql = "load data local inpath '" + filepath + "' into table " + tableName;
		System.out.println("Running:" + sql);
		boolean result = false;
		try {
			result = stmt.execute(sql);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return result;
	}

	public static ResultSet describeTables(Statement stmt, String tableName) {
		sql = "describe " + tableName;
		System.out.println("Running:" + sql);
		ResultSet res = null;
		try {
			res = stmt.executeQuery(sql);
			System.out.println("执行 describe table 运行结果:");
			while (res.next()) {
				System.out.println(res.getString(1) + "\t" + res.getString(2));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return res;
	}

	public static ResultSet showTables(Statement stmt, String tableName) {
		if (tableName == null || tableName.equals(null)) {
			sql = "show tables";
		} else {
			sql = "show tables '" + tableName + "'";
		}
		ResultSet res = null;
		try {
			res = stmt.executeQuery(sql);
			System.out.println("执行 show tables 运行结果:");
			while (res.next()) {
				System.out.println(res.getString(1));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return res;
	}

	public static boolean createTable(Statement stmt, String tableName) {
		sql = "create table " + tableName + " (key int, value string)  row format delimited fields terminated by '\t'";
		boolean result = false;
		try {
			result = stmt.execute(sql);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return result;
	}

	public static boolean dropTable(Statement stmt, String tableName) {
		// 创建的表名
		// String tableName = "testHive";
		sql = "drop table  " + tableName;
		boolean result = false;
		try {
			stmt.execute(sql);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return result;
	}

	public static Connection getConn() {
		Connection conn = null;
		try {
			Class.forName(driverName);
			conn = DriverManager.getConnection(url);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return conn;
	}

	public static void close(Connection conn, Statement stmt) {
		try {
			if (conn != null) {
				conn.close();
				conn = null;
			}
			if (stmt != null) {
				stmt.close();
				stmt = null;
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

}
