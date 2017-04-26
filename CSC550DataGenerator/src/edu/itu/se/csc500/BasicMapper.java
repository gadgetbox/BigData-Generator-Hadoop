package edu.itu.se.csc500;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

/**
 * 
 * @author gaurav-sjsu @Date 29-04-2016
 *
 */
public class BasicMapper  extends Mapper<LongWritable, LongWritable, LongWritable, LongWritable> {
	LongWritable in = new LongWritable();
    
    @Override
	public void map(LongWritable key, LongWritable value, Context context)
			throws IOException, InterruptedException {
   			context.write( key, value );
	  }
}
