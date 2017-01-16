package com.chj.hadoop.demo004;

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
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.chj.common.Base;

public class WordCount4 extends Base {
	static Logger logger = LoggerFactory.getLogger(WordCount4.class);

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

		private MultipleOutputs<Text, IntWritable> out;

		@Override
		public void setup(Context cxt) {
			out = new MultipleOutputs<Text, IntWritable>(cxt);
		}

		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
			int sum = 0;
			Iterator<IntWritable> it = values.iterator();
			while (it.hasNext()) {
				IntWritable value = it.next();
				sum += value.get();
			}
			logger.info("Reducer ====== {} -------- {}", key, sum);
			// context.write(key, new IntWritable(sum));

			String fileName = "";
			char c = key.toString().toLowerCase().charAt(0);
			if (c >= 'a' && c <= 'z') {
				fileName = c + "";
			} else {
				fileName = "*";
			}

			// out.write("file", key, new IntWritable(sum), fileName);

			out.write(key, new IntWritable(sum), fileName);
		}

		@Override
		public void cleanup(Context cxt) throws IOException, InterruptedException {
			out.close();
		}
	}

	public static void main(String[] args) throws Exception {
		// args = new String[2];
		// args[0] = "hdfs://192.168.2.251:9000/testdir/testfile";
		// args[1] = "hdfs://192.168.2.251:9000/output/wordcount.out";
		String input = "/testdir/testfile";
		String output = "/output/demo004";
		init(output);
		Configuration conf = new Configuration();
		conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

		Job job = new Job(conf);
		job.setJobName("word count");
		job.setJarByClass(WordCount4.class);

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
		job.setNumReduceTasks(Integer.parseInt("1"));

		// MultipleOutputs.addNamedOutput(job, "file", TextOutputFormat.class, Text.class, IntWritable.class);

		// 执行job，直到完成
		job.waitForCompletion(true);
		System.out.println("Finished");
	}
}
