package com.hadoop.examples;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MulInpFormatJob implements Tool{

	private Configuration conf;

	@Override
	public Configuration getConf(){
		return conf;
	}

	@Override
	public void setConf(Configuration conf){
		this.conf = conf;
	}

	@Override
	public int run(String args[]) throws Exception{

		Job wcJob = new Job(getConf());

		wcJob.setJobName("Multiple Input Format Sample");
		wcJob.setJarByClass(getClass());

		wcJob.setMapperClass(MulInpFormatcMap.class);
		wcJob.setMapperClass(MulInpFormatsMap.class);
		wcJob.setReducerClass(MulInpFormatReduce.class);

		wcJob.setMapOutputKeyClass(Text.class);
		wcJob.setMapOutputValueClass(LongWritable.class);
		wcJob.setOutputKeyClass(Text.class);
		wcJob.setOutputValueClass(LongWritable.class);

		MultipleInputs.addInputPath(wcJob, new Path(args[0]), TextInputFormat.class, MulInpFormatcMap.class);
		MultipleInputs.addInputPath(wcJob, new Path(args[1]), TextInputFormat.class, MulInpFormatsMap.class);
		FileOutputFormat.setOutputPath(wcJob, new Path(args[2]));

		return wcJob.waitForCompletion(true)?0:-1;
	}

	public static void main(String args[]) throws Exception{

		ToolRunner.run(new Configuration(), new MulInpFormatJob(), args);

	}
}
