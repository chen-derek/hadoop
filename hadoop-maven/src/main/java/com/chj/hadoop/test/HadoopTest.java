package com.chj.hadoop.test;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URL;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HadoopTest {

	static Log logger1 = LogFactory.getLog(HadoopTest.class);
	static Logger logger2 = LoggerFactory.getLogger(HadoopTest.class);

	private static final String HDFS_PATH = "hdfs://192.168.2.251:9000";
	private FileSystem fileSystem = null;
	private String localDir = "D:\\Derek\\1_dev\\bigdata\\temp\\";

	@Before
	public void Init() throws Exception {
		fileSystem = FileSystem.get(new URI(HDFS_PATH), new Configuration());
	}

	@Test
	public void readTest1() throws Exception {
		URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
		final URL url = new URL(HDFS_PATH + "/user/root/testdir/a.txt");
		final InputStream in = url.openStream();
		IOUtils.copyBytes(in, System.out, 1024, true);
	}

	/**
	 * 创建HDFS目录
	 */
	@Test
	public void mkdirTest() throws Exception {
		fileSystem.mkdirs(new Path("/output"));
	}

	/**
	 * 上传文件到HDFS目录中的文件中
	 */
	@Test
	public void uploadTest() throws Exception {
		// 创建HDFS文件
		FSDataOutputStream out = fileSystem.create(new Path("/testdir/testfile"));
		FileInputStream in = new FileInputStream("D:/test.txt");
		IOUtils.copyBytes(in, out, 1024, true);
	}

	/**
	 * 上传文件到HDFS目录中的文件中
	 */
	@Test
	public void uploadTest2() throws Exception {
		// 创建HDFS文件
		FSDataOutputStream out = fileSystem.create(new Path("/testdir/temperature.txt"));
		FileInputStream in = new FileInputStream(localDir + "temperature.txt");
		IOUtils.copyBytes(in, out, 1024, true);
	}

	/**
	 * 下载HDFS目录中的文件,在控制点数据
	 */
	@Test
	public void downloadTest1() throws Exception {
		FSDataInputStream in = fileSystem.open(new Path("/testdir/testfile"));
		IOUtils.copyBytes(in, System.out, 1024, true);
	}

	/**
	 * 下载HDFS目录中的文件,在控制点数据
	 */
	@Test
	public void downloadTest2() throws Exception {
		FSDataInputStream in = fileSystem.open(new Path("/testdir/testfile"));
		String filePath = localDir + "test.txt";
		OutputStream out = new BufferedOutputStream(new FileOutputStream(new File(filePath)));
		IOUtils.copyBytes(in, out, 1024, true);
	}

	/**
	 * 删除HDFS目录
	 */
	@Test
	public void deleteTest() throws Exception {
		logger1.info("========logger1==========");
		fileSystem.delete(new Path("/output/wordcount"), true);
	}
}
