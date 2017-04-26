package edu.itu.se.csc500;

import java.io.IOException;
import java.security.InvalidParameterException;
import java.util.Random;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
/**
 * 
 * @author gaurav-sjsu @Date 29-04-2016
 *
 */
public class BasicRandomRecordReader extends RecordReader<LongWritable, LongWritable> {

	Random random = new Random();
    private LineRecordReader in;

	public Long recordLimit = 0l;
	public Long recordCount = 1l;

	private LongWritable key = new LongWritable();
	private LongWritable value = new LongWritable();
	
	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public LongWritable getCurrentKey() throws IOException, InterruptedException {
		return key;
	}

	@Override
	public LongWritable getCurrentValue() throws IOException, InterruptedException {
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return (long) recordCount / (long) recordLimit;
	}

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		// Get & Validate the number of records to create from the configuration
		this.recordLimit = 650000000l; //500055000l; 
		 in = new LineRecordReader();
	     in.initialize(split, context);
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		// If we still have records to create
		if (recordCount < recordLimit) {
			// Generate random data
			long randomLong = (long) random.nextInt(1000 * 1000 )+1;			
			key.set(recordCount);
			value.set(randomLong);			
			++recordCount;
			return true;
		} else {
			// Else, return false
			  key = null;
	          value = null;
	          return false;
			
		}
	}

}
