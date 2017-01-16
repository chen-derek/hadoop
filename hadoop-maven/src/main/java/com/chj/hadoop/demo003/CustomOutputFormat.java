package com.chj.hadoop.demo003;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CustomOutputFormat extends FileOutputFormat<Text, IntWritable> {

	public CustomOutputFormat() {
		super();
	}

	private String prefix = "custom_";

	// public abstract String getFileName();

	@Override
	public RecordWriter<Text, IntWritable> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
		Path outputDir = FileOutputFormat.getOutputPath(job);
		// 新建一个可写入的文件
		String subfix = job.getTaskAttemptID().getTaskID().toString();
		String filePath = outputDir.toString() + "/" + prefix + subfix.substring(subfix.length() - 5, subfix.length());
		System.out.println("----------------" + filePath);
		Path path = new Path(filePath);
		FSDataOutputStream fileOut = path.getFileSystem(job.getConfiguration()).create(path);
		return new CustomRecordWriter(fileOut);
	}

}
