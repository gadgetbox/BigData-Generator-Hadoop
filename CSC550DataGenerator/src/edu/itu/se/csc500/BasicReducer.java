package edu.itu.se.csc500;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

/**
 *  
 *
 * @author dev1-Gaurav @Date 26-04-2016
 *
 */


public class BasicReducer extends Reducer <LongWritable, LongWritable, LongWritable, LongWritable> {
	
	@Override
	public void reduce(LongWritable key, Iterable<LongWritable> values,
			Context context) throws IOException, InterruptedException {
		for ( LongWritable value : values) {		
			context.write(key, value);
		}
	
	}
}
