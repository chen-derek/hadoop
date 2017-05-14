package com.chj.hadoop.demo006;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class JoinPartitioner extends Partitioner<TextPair, Text> {

	@Override
	public int getPartition(TextPair key, Text value, int numPartitions) {
		int partition = (key.getText().hashCode() & Integer.MAX_VALUE) % numPartitions;
		System.out.println("  ===========> " + partition + " ======> " + numPartitions);
		return partition;
	}

}
