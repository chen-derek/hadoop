package com.chj.hadoop.sequenceFile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;

import com.chj.common.Base;

public class SequenceFileWriteDemo extends Base {

	private static final String[] data = { "one -- this is one ", "two -- this is two ", "three -- this is three ", "four -- this is four " };

	public static void main(String[] args) {
		String url = HDFS_PATH + "/testdir/sequence_file";
		init(url);
		Path path = new Path(url);

		IntWritable key = new IntWritable();
		Text value = new Text();
		SequenceFile.Writer writer = null;

		try {
			Writer.Option keyOpt = Writer.keyClass(key.getClass());
			Writer.Option valueOpt = Writer.valueClass(value.getClass());
			Writer.Option fileOpt = Writer.file(path);
			// writer = SequenceFile.createWriter(fileSystem, new Configuration(), path, key.getClass(),value.getClass());
			writer = SequenceFile.createWriter(new Configuration(), fileOpt, keyOpt, valueOpt);
			for (int i = 0; i < 100; i++) {
				key.set(100 - i);
				value.set(data[i % data.length]);
				writer.append(key, value);
			}
		} catch (Exception e) {
			// TODO: handle exception
		} finally {
			IOUtils.closeStream(writer);
		}
	}
}
