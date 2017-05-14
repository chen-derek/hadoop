package com.chj.hadoop.demo006;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class TextPair implements WritableComparable<TextPair> {

	private int type;
	private String text;

	public TextPair() {
	}

	public TextPair(int type, String text) {
		super();
		this.type = type;
		this.text = text;
	}

	public int getType() {
		return type;
	}

	public void setType(int type) {
		this.type = type;
	}

	public String getText() {
		return text;
	}

	public void setText(String text) {
		this.text = text;
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(getType());
		out.writeUTF(getText());

	}

	public void readFields(DataInput in) throws IOException {
		type = in.readInt();
		text = in.readUTF();
	}

	public int compareTo(TextPair o) {
		if (this.getType() - o.getType() == 0) {
			return this.getText().compareTo(o.getText());
		} else {
			return this.getType() - o.getType();
		}
	}

}
