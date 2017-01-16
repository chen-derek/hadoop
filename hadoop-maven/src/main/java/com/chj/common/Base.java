package com.chj.common;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class Base {

	public static final String HDFS_PATH = "hdfs://192.168.2.251:9000";
	private static FileSystem fileSystem = null;

	public static void init(String output) {
		try {
			fileSystem = FileSystem.get(new URI(HDFS_PATH), new Configuration());
			fileSystem.delete(new Path(output), true);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
