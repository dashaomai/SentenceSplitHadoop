package com.hebangdata;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SentenceMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	public void map(
			LongWritable key, Text value, Context context
	) throws IOException, InterruptedException {
		String line = value.toString();
		final String query = context.getConfiguration().get("query");
		int pos = line.indexOf(query);
		int len = query.length();
		int count = 0;

		while (-1 < pos) {
			count++;

			line = line.substring(pos + len);
			pos = line.indexOf(query);
		}

		context.write(new Text(query), new IntWritable(count));
	}
}
