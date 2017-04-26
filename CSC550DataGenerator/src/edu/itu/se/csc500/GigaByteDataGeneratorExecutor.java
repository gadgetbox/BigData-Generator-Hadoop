

package edu.itu.se.csc500;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.InputSampler;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**
 *  
 *
 * @author gaurav-sjsu @Date 29-04-2016
 *
 */
public class GigaByteDataGeneratorExecutor extends Configured implements Tool  {
	
	
	public static void  main(String [] args) throws Exception {
	  System.out.println("... Executing GigaByteDataGeneratorExecutor ...");
	  long start = System.currentTimeMillis();	
		int exitCode = ToolRunner.run(new Configuration(),
				new GigaByteDataGeneratorExecutor(), args);
		long end = System.currentTimeMillis();
		System.out.println("... GigaByteDataGeneratorExecutor Completed ...");
		System.out.println("Total time taken is "+((end-start)/1000)+" Seconds. ");

		System.exit(exitCode);
		}

	@Override
	public int run(String[] args) throws Exception {
		
		if (args.length != 2) {
			System.out
					.printf("two parameters are required for GigaByteDataGenerator -<inputDir to EmptyFile>/empty.text <output dir> \n");
			return -1;
		}

		Job job = new Job(getConf());
		job.setJobName("GigaByteDataGenerator");
		System.out.println(" Executing with Config: ");
		System.out.println(" No of Reducers :  "+job.getNumReduceTasks());

		job.setJarByClass(getClass());		
		job.setInputFormatClass(BasicRandomInputFormat.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));				

		//job.setNumReduceTasks(10);
			
		job.setMapperClass(BasicMapper.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(LongWritable.class);

		job.setReducerClass(BasicReducer.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(LongWritable.class);
		
		boolean success = job.waitForCompletion(true);
		BasicMergeOutput.mergeReducerOutputFiles(getConf(), new Path(args[1])+"", new Path(args[1])+File.separator+"all.txt");
		return success ? 0 : 1;
		
	}

}
