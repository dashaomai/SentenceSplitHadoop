package com.hebangdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class SentenceLauncher {
	private static final Logger log = LoggerFactory.getLogger("SentenceLauncher");

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		if (args.length != 3) {
			log.error("Usage: SentenceLauncher <input path> <output path> <query word>");
			System.exit(-1);
		}

		final Configuration conf = new Configuration();
		conf.set("query", args[2]);

		final Job job = Job.getInstance(conf);
		job.setJarByClass(SentenceLauncher.class);
		job.setJobName("Sentence Query");;

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapperClass(SentenceMapper.class);
		job.setReducerClass(SentenceReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
