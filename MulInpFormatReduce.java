package com.hadoop.examples;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MulInpFormatReduce extends Reducer<Text, LongWritable, Text, LongWritable>{

	@Override
	protected void reduce(Text key, Iterable<LongWritable> values, Context context)
		throws IOException, InterruptedException {

		long sum = 0;

		for(LongWritable val : values){

			sum = sum + val.get();
		}

	context.write(key, new LongWritable(sum));

	}
}
