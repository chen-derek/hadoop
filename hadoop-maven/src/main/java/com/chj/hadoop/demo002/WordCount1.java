package com.chj.hadoop.demo002;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.chj.common.Base;

public class WordCount1 extends Base {
	static Logger logger = LoggerFactory.getLogger(WordCount1.class);

	public static class TokenizerMapper extends MapReduceBase implements Mapper<Object, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Object key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			String line = value.toString();
			logger.info("Before Mapper: " + key + ", " + value);
			StringTokenizer tokenizer = new StringTokenizer(line);
			while (tokenizer.hasMoreTokens()) {
				word.set(tokenizer.nextToken());
				output.collect(word, one);
			}
			logger.info("======" + "After Mapper:" + new Text(word) + ", " + one.get());

		}
	}

	public static class IntSumReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			output.collect(key, new IntWritable(sum));
			logger.info("======" + "After Reduce: " + key + ", " + sum);
		}

	}

	public static void main(String[] args) throws Exception {
		// args = new String[2];
		// args[0] = "hdfs://192.168.2.251:9000/testdir/testfile";
		// args[1] = "hdfs://192.168.2.251:9000/output/wordcount.out";

		String input = "/testdir/testfile";
		String output = "/output/demo002";
		init(output);
		Configuration conf = new Configuration();
		JobConf job = new JobConf(conf);
		job.setJobName("word count");
		job.setJarByClass(WordCount1.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(HDFS_PATH + input));
		FileOutputFormat.setOutputPath(job, new Path(HDFS_PATH + output));
		JobClient.runJob(job);

		// JobConf conf = new JobConf(WordCount.class);
		// conf.setJobName("wordcount");
		//
		// conf.setOutputKeyClass(Text.class);
		// conf.setOutputValueClass(IntWritable.class);
		//
		// conf.setMapperClass(TokenizerMapper.class);
		// conf.setCombinerClass(IntSumReducer.class);
		// conf.setReducerClass(IntSumReducer.class);
		//
		// conf.setInputFormat(TextInputFormat.class);
		// conf.setOutputFormat(TextOutputFormat.class);
		//
		// FileInputFormat.setInputPaths(conf, new Path(args[0]));
		// FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		//
		// JobClient.runJob(conf);
	}
}
