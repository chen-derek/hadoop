package com.chj;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import com.chj.hive.HiveAPI;

public class TestHive {

	private Statement stmt = null;
	private Connection conn = null;

	@Before
	public void setConAndStatement() {

		conn = HiveAPI.getConn();
		try {
			stmt = conn.createStatement();
		} catch (SQLException e) {
			e.printStackTrace();
		} 
		assertNotNull(conn);
	}

	@Test
	public void testDropTable() {
		String tableName = "testhive";
		assertNotNull(HiveAPI.dropTable(stmt, tableName));
	}

	@Test
	public void testCreateTable() {
		boolean result = HiveAPI.createTable(stmt, "testhive");
		HiveAPI.close(conn, stmt);
		assertNotNull(result);
	}

	@Test
	public void testdescribeTables() {
		ResultSet res = HiveAPI.describeTables(stmt, "testhive");
		assertNotNull(res);
	}

	@Test
	public void testshowTables() {
		// ResultSet res=HiveAPI.showTables(stmt, "testhive");
		ResultSet res = HiveAPI.showTables(stmt, null);
		assertNotNull(res);
	}

	@Test
	public void testloadData() {
		boolean result = HiveAPI.loadData(stmt, "testhive", "user.txt");
		assertNotNull(result);
	}

	@Test
	public void testclose() {
		HiveAPI.close(conn, stmt);
	}

	@Test
	public void testSelectData() {
		ResultSet res = HiveAPI.selectData(stmt, "testhive");
		assertNotNull(res);
	}

	@Test
	public void testCountData() {
		ResultSet res = HiveAPI.countData(stmt, "testhive");
		assertNotNull(res);
	}

}
