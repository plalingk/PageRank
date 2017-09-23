/***
 * Author: Prasanna Lalingkar
 * Email: plalingk@uncc.edu
 * 
 * This is the Cleanup Map Class and is responsible for removing all the unnecessary things (list of URLs) from the input.
 * 
 * Input: <Key, Value> => <URL, (Initial PageRank, List-of-Urls)>
 * Output: <Key, Value> => <Page Rank, URL>
 */

package com.prasanna;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CleanupMap extends Mapper<LongWritable, Text, DoubleWritable, Text> {

	public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {
		try {
			String[] line = lineText.toString().split("\t");
			String pageName = line[0];
			String[] rank = line[1].split("@@@@");
			Double currentRank = Double.parseDouble(rank[0]);
			/*
			 * Swap the key and value, Make the page rank as the key so that it
			 * can be sorted in the decreasing order by rank
			 */
			Text value = new Text(pageName);
			DoubleWritable key = new DoubleWritable(currentRank);
			context.write(key, value);
		} catch (Exception e) {
			return;
		}
	}
}
