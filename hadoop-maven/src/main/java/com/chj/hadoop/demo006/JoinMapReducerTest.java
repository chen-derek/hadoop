package com.chj.hadoop.demo006;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.chj.common.Base;

public class JoinMapReducerTest extends Base {
	static class JoinMapper extends Mapper<LongWritable, Text, TextPair, Text> {
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			FileSplit fileSplit = (FileSplit) context.getInputSplit();
			String filePath = fileSplit.getPath().toString();
			String line = value.toString();
			String[] arr = line.split(",");
			if (filePath.contains("pay")) {
				context.write(new TextPair(0, arr[0]), new Text(arr[1]));
			} else if (filePath.contains("order")) {
				context.write(new TextPair(1, arr[0]), new Text(arr[1]));
			}

		}
	}

	static class JoinReducer extends Reducer<TextPair, Text, Text, Text> {

		private String outKey = "";

		@Override
		public void reduce(TextPair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			int i = 0;
			Iterator<Text> it = values.iterator();
			while (it.hasNext()) {
				if (i == 0) {
					outKey = it.next().toString();
				} else {
					Text text = it.next();
					System.out.println(i + "  ===========> " + outKey + " ======> " + text.toString());
					context.write(new Text(outKey), new Text(text.toString()));
				}
				i++;
			}
			System.out.println("===========> " + outKey + " ======> " + i);
		}
	}

	public static void main(String[] args) throws Exception {
		// 输入路径或具体某个文件
		String input1 = "/testdir/pay.txt";
		String input = "/testdir/order.txt";
		// 输出路径，必须是不存在的，空文件加也不行。
		String output = "/output/demo006";
		init(output);
		Configuration hadoopConfig = new Configuration();

		hadoopConfig.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		hadoopConfig.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
		Job job = new Job(hadoopConfig);

		// 如果需要打成jar运行，需要下面这句
		// job.setJarByClass(NewMaxTemperature.class);

		// job执行作业时输入和输出文件的路径
		FileInputFormat.addInputPath(job, new Path(HDFS_PATH + input1));
		FileInputFormat.addInputPath(job, new Path(HDFS_PATH + input));
		FileOutputFormat.setOutputPath(job, new Path(HDFS_PATH + output));

		// 指定自定义的Mapper和Reducer作为两个阶段的任务处理类
		job.setMapperClass(JoinMapper.class);
		job.setReducerClass(JoinReducer.class);

		job.setMapOutputKeyClass(TextPair.class);
		job.setMapOutputValueClass(Text.class);

		// 设置最后输出结果的Key和Value的类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setPartitionerClass(JoinPartitioner.class);
		// job.setSortComparatorClass(MegerSortComparator.class);
		job.setGroupingComparatorClass(MegerGrouptComparator.class);

		// 执行job，直到完成
		job.waitForCompletion(true);
		System.out.println("Finished");
	}
}