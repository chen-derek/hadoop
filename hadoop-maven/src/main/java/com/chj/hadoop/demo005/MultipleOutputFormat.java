package com.chj.hadoop.demo005;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class MultipleOutputFormat<K extends WritableComparable<?>, V extends Writable> extends FileOutputFormat<K, V> {

	static Logger logger = LoggerFactory.getLogger(MultipleOutputFormat.class);

	private MultipleRecordWriter writer = null;

	@Override
	public RecordWriter<K, V> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
		if (writer == null) {
			writer = new MultipleRecordWriter(job, getTaskOutputPath(job));
		}
		return writer;
	}

	private Path getTaskOutputPath(TaskAttemptContext job) throws IOException {
		Path workPath = null;
		OutputCommitter committer = super.getOutputCommitter(job);
		if (committer instanceof FileOutputCommitter) {
			workPath = ((FileOutputCommitter) committer).getWorkPath();
		} else {
			workPath = super.getOutputPath(job);
			if (workPath == null) {
				throw new IOException("undefined job output-path");
			}
		}
		logger.info("====getTaskOutputPath===== {}", workPath.toString());
		return workPath;
	}

	abstract String generateFileNameForKeyValue(K key, V value);

	public class MultipleRecordWriter extends RecordWriter<K, V> {
		String SEPERATOR = "mapreduce.output.textoutputformat.separator";
		TaskAttemptContext job = null;
		Path workPath = null;
		Map<String, RecordWriter<K, V>> recordWriter = null;

		public MultipleRecordWriter(TaskAttemptContext job, Path workPath) {
			super();
			this.job = job;
			this.workPath = workPath;
			this.recordWriter = new HashMap<String, RecordWriter<K, V>>();
		}

		@Override
		public void write(K key, V value) throws IOException, InterruptedException {
			String baseName = generateFileNameForKeyValue(key, value);
			logger.info("====baseName===== {}", baseName);
			RecordWriter<K, V> rw = this.recordWriter.get(baseName);
			if (rw == null) {
				rw = getBaseRecordWriter(job, baseName);
				this.recordWriter.put(baseName, rw);
			}
			rw.write(key, value);
		}

		@Override
		public void close(TaskAttemptContext context) throws IOException, InterruptedException {
			Iterator<RecordWriter<K, V>> it = this.recordWriter.values().iterator();
			while (it.hasNext()) {
				it.next().close(context);
			}
			this.recordWriter.clear();
		}

		public RecordWriter<K, V> getBaseRecordWriter(TaskAttemptContext job, String baseName) throws IOException {
			Configuration conf = job.getConfiguration();
			boolean isCompressed = getCompressOutput(job);
			String keyValueSeparator = ",";
			RecordWriter<K, V> recordWriter = null;
			if (isCompressed) {// 是否开启压缩
				// 获取压缩方式，如果没有设定，取默认压缩方式
				Class<? extends CompressionCodec> codecClass = getOutputCompressorClass(job, GzipCodec.class);
				CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
				String extension = codec.getDefaultExtension();
				Path file = new Path(this.workPath, baseName + extension);
				logger.info("=====getBaseRecordWriter===== {}", file.toString());
				FSDataOutputStream fileout = file.getFileSystem(conf).create(file, false);
				recordWriter = new LineRecordWriter<K, V>(new DataOutputStream(codec.createOutputStream(fileout)), keyValueSeparator);
			} else {
				Path file = new Path(this.workPath, baseName);
				logger.info("=====getBaseRecordWriter===== {}", file.toString());
				FSDataOutputStream fileout = file.getFileSystem(conf).create(file, false);
				recordWriter = new LineRecordWriter<K, V>(fileout, keyValueSeparator);
			}
			return recordWriter;
		}

	}
}
