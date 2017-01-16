package com.chj.hadoop.demo003;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.chj.common.Base;

public class WordCount3 extends Base {
	static Logger logger = LoggerFactory.getLogger(WordCount3.class);

	public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
			String line = value.toString();
			logger.info("Before Mapper: " + key + ", " + value);
			StringTokenizer tokenizer = new StringTokenizer(line);
			while (tokenizer.hasMoreTokens()) {
				word.set(tokenizer.nextToken());
				context.write(word, one);
			}
			logger.info("======" + "After Mapper:" + new Text(word) + ", " + one.get());
		}
	}

	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
			int sum = 0;
			Iterator<IntWritable> it = values.iterator();
			while (it.hasNext()) {
				IntWritable value = it.next();
				sum += value.get();
			}
			logger.info("Reducer ====== {} -------- {}", key, sum);
			context.write(key, new IntWritable(sum));
		}
	}

	// public static class MyFileOutPutFormat extends MyMultipleOutputFormat<Text, IntWritable> {

	// @Override
	// protected String generateFileNameForKeyValue(Text key, IntWritable value, String name) {
	// char c = key.toString().toLowerCase().charAt(0);
	// if (c >= 'a' && c <= 'z') {
	// return c + ".txt";
	// }
	// return "other.txt";
	// }

	// @Override
	// protected RecordWriter<Text, IntWritable> getRecordWriter(FileSystem fs, JobConf job, String name, Progressable arg3) throws IOException {
	// return super.getRecordWriter(fs, job, name, arg3);
	// }

	// }

	public static void main(String[] args) throws Exception {
		// args = new String[2];
		// args[0] = "hdfs://192.168.2.251:9000/testdir/testfile";
		// args[1] = "hdfs://192.168.2.251:9000/output/wordcount.out";
		String input = "/testdir/testfile";
		String output = "/output/demo003";
		init(output);
		Configuration conf = new Configuration();
		conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
		// JobConf job = new JobConf(conf);
		// job.setJobName("word count 2");
		// job.setJarByClass(WordCount2.class);
		// job.setMapperClass(TokenizerMapper.class);
		// job.setCombinerClass(IntSumReducer.class);
		// job.setReducerClass(IntSumReducer.class);
		// job.setOutputKeyClass(Text.class);
		// job.setOutputValueClass(IntWritable.class);
		// job.setOutputFormat(MyFileOutPutFormat.class);
		// FileInputFormat.addInputPath(job, new Path(HDFS_PATH + input));
		// FileOutputFormat.setOutputPath(job, new Path(HDFS_PATH + output));
		// JobClient.runJob(job);

		Job job = new Job(conf);
		job.setJobName("word count 2");
		job.setJarByClass(WordCount3.class);

		// 如果需要打成jar运行，需要下面这句
		// job.setJarByClass(NewMaxTemperature.class);

		// job执行作业时输入和输出文件的路径
		FileInputFormat.addInputPath(job, new Path(HDFS_PATH + input));
		FileOutputFormat.setOutputPath(job, new Path(HDFS_PATH + output));

		// 指定自定义的Mapper和Reducer作为两个阶段的任务处理类
		job.setMapperClass(TokenizerMapper.class);
		job.setReducerClass(IntSumReducer.class);

		// 设置最后输出结果的Key和Value的类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(CustomOutputFormat.class);
		job.setNumReduceTasks(Integer.parseInt("1"));

		// 执行job，直到完成
		job.waitForCompletion(true);
		System.out.println("Finished");
	}
}
