package com.chj.hadoop.demo006;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class MegerGrouptComparator extends WritableComparator {

	public MegerGrouptComparator() {
		super(TextPair.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		TextPair ta = (TextPair) a;
		TextPair tb = (TextPair) b;
		// if (ta.getType() - tb.getType() == 0) {
		// return ta.getText().compareTo(tb.getText());
		// } else {
		// return ta.getType() - tb.getType();
		// }
		return ta.getText().compareTo(tb.getText());
	}

}
