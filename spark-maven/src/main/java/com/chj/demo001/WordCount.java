package com.chj.demo001;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

public class WordCount {

	public static void main(String[] args) {
		/**
		 * 第1步：创建Spark的配置对象SparkConf，设置Spark程序的运行时的配置信息，*例如说通过setMaster来设置程序要链接的Spark集群的Master的URL,如果设 置 为local，则代表Spark程序在本地运行，特别适合于机器配置条件非常差（例 如 只有1G的内存）的初学者*
		 */
		SparkConf conf = new SparkConf().setAppName("Spark-Word-Count").setMaster("spark://192.168.2.251:7077");
		/**
		 * 第2步：创建SparkContext对象 SparkContext是Spark程序所有功能的唯一入口，无论是采用Scala、Java、 Python、R等都必须有一个SparkContext(不同的语言具体的类名称不同，如果是java的为javaSparkContext) SparkContext核心作用：初始化Spark应用程序运行所需要的核心组件，包括 DAGScheduler、TaskScheduler、SchedulerBackend 同时还会负责Spark程序往Master注册程序等 SparkContext是整个Spark应用程序中最为至关重要的一个对象
		 */
		JavaSparkContext sc = new JavaSparkContext(conf);// 其底层就是scala的 sparkcontext
		sc.addJar("D:/Derek/1_dev/web/maven3/repo/com/chj/spark-maven/0.0.1-SNAPSHOT/spark-maven-0.0.1-SNAPSHOT.jar");

		/**
		 * 第3步：根据具体的数据来源（HDFS、HBase、LocalFS、DB、S3等）通过 SparkContext来创建RDD JavaRDD的创建基本有三种方式：根据外部的数据来源（例如HDFS）、根据Scala 集合、由其它的RDD操作 数据会被JavaRDD划分成为一系列的Partitions，分配到每个Partition的数据 属于一个Task的处理范畴
		 */
		JavaRDD<String> lines = sc.textFile("D:/tmp/spark.txt");
		/**
		 * 第4步：对初始的JavaRDD进行Transformation级别的处理，例如map、filter 等高阶函数等的编程，来进行具体的数据计算 第4.1步：讲每一行的字符串拆分成单个的单词
		 */
		JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
			// 如果是scala由于Sam转化所以可以写成一行代码
			private static final long serialVersionUID = 1L;

			@Override
			public Iterator<String> call(String line) throws Exception {
				System.out.println(" --------- " + line);
				return Arrays.asList(line.split(",")).iterator();
			}
		});
		/**
		 * 第4步：对初始的JavaRDD进行Transformation级别的处理，例如map、filter 等高阶函数等的编程，来进行具体的数据计算 第4.2步：在单词拆分的基础上对每个单词实例计数为1，也就是word=>(word,1)
		 */
		JavaPairRDD<String, Integer> pairs = words.mapToPair(new PairFunction<String, String, Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Tuple2<String, Integer> call(String word) throws Exception {
				// TODOAuto-generatedmethodstub
				return new Tuple2<String, Integer>(word, 1);
			}
		});

		/**
		 * 第4步：对初始的RDD进行Transformation级别的处理，例如map、filter等高 阶函数等的编程，来进行具体的数据计算 第4.3步：在每个单词实例计数为1基础之上统计每个单词在文件中出现的总 次数
		 */
		final JavaPairRDD<String, Integer> wordsCount = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
			private static final long serialVersionUID = 1L;

			// 对相同的Key，进行Value的累计（包括Local和Reducer级别同时Reduce）
			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				// TODOAuto-generated method stub
				return v1 + v2;
			}
		});
		wordsCount.foreach(new VoidFunction<Tuple2<String, Integer>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public void call(Tuple2<String, Integer> pairs) throws Exception {
				// TODOAuto-generated method stub
				System.out.println(pairs._1 + ":" + pairs._2);
			}
		});
		sc.close();

	}
}
