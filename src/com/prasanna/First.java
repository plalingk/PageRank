
/***
 * Author: Prasanna Lalingkar
 * Email: plalingk@uncc.edu
 * 
 * This is the main Driver class for the entire program. It contains the calls for all the other components in the project.
 * There are sysout statements indicating what parts are completed.
 */

package com.prasanna;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class First extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new First(), args);
		System.exit(res);
	}

	public int run(String[] args) throws Exception {

		int i = 0;
		int result = 0;
		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(conf);

		/*
		 * Check for previously present directories for N (length of corpus) and
		 * inverted index. If present, delete it.
		 */
		System.out.println("\nChecking if File for N exists");
		Path pathForN = new Path("N");
		// delete existing directory for N - number of pages
		if (hdfs.exists(pathForN)) {
			System.out.println("\nFile for Number pages in Corpus (N) exists, deleting file");
			hdfs.delete(pathForN, true);
		}
		System.out.println("\nChecking if File for Inverted Index exists");
		Path invertedIndex = new Path("invertedIndex");
		// delete existing directory for inverted index
		if (hdfs.exists(invertedIndex)) {
			System.out.println("\nFile for Inverted Index exists, deleting file");
			hdfs.delete(invertedIndex, true);
		}

		/*
		 * Build an Inverted index for the input corpus of pages
		 */
		System.out.println("\nBuilding Inverted Index\n");
		Job jobI = Job.getInstance(conf, "Calculate Inverted Index");
		jobI.setJarByClass(this.getClass());
		FileInputFormat.addInputPaths(jobI, args[0]);
		FileOutputFormat.setOutputPath(jobI, new Path("invertedIndex"));
		jobI.setMapperClass(InvertedIndexMap.class);
		jobI.setReducerClass(InvertedIndexReduce.class);
		jobI.setOutputKeyClass(Text.class);
		jobI.setOutputValueClass(Text.class);
		jobI.setNumReduceTasks(1);
		jobI.waitForCompletion(true);
		System.out.println("\nBuilding Inverted Index successful!");

		/*
		 * Calculate the numner of pages in the corpus - N and pass it as a
		 * context variable for the base job to use
		 */
		System.out.println("\nCalculating pages in the corpus\n");
		Job jobN = Job.getInstance(conf, "Calculate N");
		jobN.setJarByClass(this.getClass());
		FileInputFormat.addInputPaths(jobN, args[0]);
		FileOutputFormat.setOutputPath(jobN, new Path("N"));
		jobN.setMapperClass(MapN.class);
		jobN.setCombinerClass(ReduceN.class);
		jobN.setReducerClass(ReduceN.class);
		jobN.setOutputKeyClass(Text.class);
		jobN.setOutputValueClass(LongWritable.class);
		jobN.setNumReduceTasks(1);
		jobN.waitForCompletion(true);

		long N = 1;
		try {
			Path pt = new Path("N/part-r-00000");
			FileSystem fs = FileSystem.get(conf);
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
			String line;
			line = br.readLine();
			String[] tempArray = line.split("\t");
			N = Long.parseLong(tempArray[1]);
		} catch (Exception e) {
			System.out.println("\nProblem in file reading!");
		}
		System.out.println("\nNumber of Pages in the corpus are: " + N);
		conf.set("N", Long.toString(N));

		/*
		 * Base job calculates the initial pagerank and brings data in required
		 * format
		 */
		System.out.println("\nRunning the base job to create key values and initial page ranks\n");
		Job job = Job.getInstance(conf, "Base Job");
		job.setJarByClass(this.getClass());
		FileInputFormat.addInputPaths(job, args[0]);
		FileOutputFormat.setOutputPath(job, new Path(args[1] + i));
		job.setMapperClass(BaseMap.class);
		job.setReducerClass(BaseReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setNumReduceTasks(8);
		job.waitForCompletion(true);

		/*
		 * This runs the pagerank code for 10 iterations
		 */
		System.out.println("\nRunning the Page Rank job for 10 iterations\n");
		if (job.waitForCompletion(true)) {
			for (; i < 10; i++) {
				// Deleting previous outputs to keep minial data on HDFS
				if (hdfs.exists(new Path(args[1] + (i - 1)))) {
					hdfs.delete(new Path(args[1] + (i - 1)), true);
				}
				System.out.println("\nExecuting iteration: " + Integer.toString(i + 1));
				System.out.println("\n");
				Job job2 = Job.getInstance(conf, " wordcount ");
				job2.setJarByClass(this.getClass());
				FileInputFormat.addInputPaths(job2, args[1] + i);
				FileOutputFormat.setOutputPath(job2, new Path(args[1] + (i + 1)));
				job2.setMapperClass(PageRankMap.class);
				job2.setReducerClass(PageRankReduce.class);
				job2.setOutputKeyClass(Text.class);
				job2.setOutputValueClass(Text.class);
				job2.setNumReduceTasks(8);
				result = job2.waitForCompletion(true) ? 0 : 1;
			}
		}

		/*
		 * Removing the unnecessary part and sorting by pagerank in decreasing
		 * order to display
		 */
		System.out.println("\nRunning the Clean Up job to sort the Page Ranks in the descending order\n");
		if (result == 0) {
			Job job3 = Job.getInstance(conf, " wordcount ");
			job3.setJarByClass(this.getClass());
			FileInputFormat.addInputPaths(job3, args[1] + 10);
			FileOutputFormat.setOutputPath(job3, new Path(args[1]));
			job3.setMapperClass(CleanupMap.class);
			job3.setReducerClass(CleanupReduce.class);
			// create only one reducer for this job3 to globally sort
			job3.setNumReduceTasks(1);
			job3.setOutputKeyClass(DoubleWritable.class);
			job3.setOutputValueClass(Text.class);
			// sort the output in descending order using in-build comparator
			job3.setSortComparatorClass(LongWritable.DecreasingComparator.class);
			result = job3.waitForCompletion(true) ? 0 : 1;
		}

		System.out.println("\nDeleting the intermediate outputs\n");
		// deleting intermediate outputs
		if (hdfs.exists(new Path(args[1] + 9))) {
			hdfs.delete(new Path(args[1] + 9), true);
		}
		if (hdfs.exists(new Path(args[1] + 10))) {
			hdfs.delete(new Path(args[1] + 10), true);
		}

		return result;
	}

}
