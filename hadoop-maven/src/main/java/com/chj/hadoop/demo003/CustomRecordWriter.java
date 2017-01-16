package com.chj.hadoop.demo003;

import java.io.IOException;
import java.io.PrintWriter;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class CustomRecordWriter extends RecordWriter<Text, IntWritable> {

	private PrintWriter out;
	private String separator = ",";

	public CustomRecordWriter(FSDataOutputStream fileOut) {
		out = new PrintWriter(fileOut);
	}

	public void write(Text key, IntWritable value) throws IOException {
		out.println(key.toString() + separator + value.toString());

	}

	@Override
	public void close(TaskAttemptContext context) throws IOException, InterruptedException {
		out.close();

	}

}
