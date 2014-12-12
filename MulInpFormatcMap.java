package com.hadoop.examples;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;

public class MulInpFormatcMap extends Mapper<LongWritable, Text, Text, LongWritable>{

	@Override
	protected void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException{

		String s = value.toString();
		StringTokenizer st = new StringTokenizer(s, ",");

		while(st.hasMoreTokens()){

			context.write(new Text(st.nextToken()), new LongWritable(1));

		}
	}
}
