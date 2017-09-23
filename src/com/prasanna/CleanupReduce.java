/***
 * Author: Prasanna Lalingkar
 * Email: plalingk@uncc.edu
 * 
 * This is the Cleanup Reduce Class and it swaps the key value and emits it.
 * The output is sorted in decreasing order as we use a comparator here.
 * 
 * Input: <Key, Value> => <Page Rank, URL(Iterable)>
 * Output: <Key, Value> => <URL, Page Rank>
 */

package com.prasanna;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CleanupReduce extends Reducer<DoubleWritable, Text, Text, DoubleWritable> {

	public void reduce(DoubleWritable rank, Iterable<Text> pages, Context context)
			throws IOException, InterruptedException {
		for (Text page : pages) {
			context.write(new Text(page), rank);
		}
	}
}
