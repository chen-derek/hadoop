package com.chj.hadoop.sequenceFile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

import com.chj.common.Base;

public class SequenceFileReadDemo extends Base {

	private static final String[] data = { "one -- this is one ", "two -- this is two ", "three -- this is three ", "four -- this is four " };

	public static void main(String[] args) {
		String url = HDFS_PATH + "/testdir/sequence_file";
		Path path = new Path(url);

		SequenceFile.Reader reader = null;

		try {
			Configuration conf = new Configuration();
			Reader.Option fileOpt = Reader.file(path);
			// writer = SequenceFile.createWriter(fileSystem, new Configuration(), path, key.getClass(),value.getClass());
			reader = new SequenceFile.Reader(conf, fileOpt);
			Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
			Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
			long position = reader.getPosition();
			while (reader.next(key, value)) {
				String syncSeen = reader.syncSeen() ? "*" : "";
				System.out.println(position + " -- " + syncSeen + " -- " + key + " -- " + value );
				position = reader.getPosition();
			}
		} catch (Exception e) {
			// TODO: handle exception
		} finally {
			IOUtils.closeStream(reader);
		}
	}
}
 