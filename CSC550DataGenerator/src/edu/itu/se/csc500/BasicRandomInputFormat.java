package edu.itu.se.csc500;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * 
 * @author gaurav-sjsu @Date 29-04-2016
 *
 */
public class BasicRandomInputFormat extends	FileInputFormat<LongWritable, LongWritable> { 

	@Override
	public RecordReader<LongWritable, LongWritable> createRecordReader(InputSplit arg0, TaskAttemptContext arg1)
			throws IOException, InterruptedException {
	        return new BasicRandomRecordReader();
	
	}
	
	public InputSplit[]	getSplits(Job job, int numSplits){
		InputSplit[] splits = new InputSplit[numSplits];
		
		return splits;
	}


	public static class EmptyInputSplit extends InputSplit implements
			Writable {

		@Override
		public void readFields(DataInput arg0) throws IOException {
		}

		@Override
		public void write(DataOutput arg0) throws IOException {
		}

		@Override
		public long getLength() throws IOException, InterruptedException {
			return 0;
		}

		@Override
		public String[] getLocations() throws IOException,
				InterruptedException {
			return new String[0];
		}
	}

	
}
