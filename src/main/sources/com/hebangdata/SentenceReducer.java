package com.hebangdata;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SentenceReducer
extends Reducer<Text, IntWritable, Text, IntWritable> {

	@Override
	public void reduce(
			Text key,
			Iterable<IntWritable> values,
			Context context
	) throws IOException, InterruptedException {
		int sum = 0;

		for (final IntWritable value :
				values) {
			sum += value.get();
		}

		context.write(key, new IntWritable(sum));
	}
}
