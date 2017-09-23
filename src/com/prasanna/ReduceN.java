/***
 * Author: Prasanna Lalingkar
 * Email: plalingk@uncc.edu
 * 
 * This is the ReduceN Class and is used both as a combiner (local reducer) and as a reducer.
 * It takes as the dummy key and an iterable set of values associated with this key. It then sums 
 * up all these values. 
 * 
 * Input: <Key, Value> => <dummyKey, count(Iterable)>
 * Output: <Key, Value> => <dummyKey, sumOfCounts>
 */

package com.prasanna;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReduceN extends Reducer<Text, LongWritable, Text, LongWritable> {

	public void reduce(Text dummyKey, Iterable<LongWritable> counts, Context context)
			throws IOException, InterruptedException {
		long sum = 0;
		for (LongWritable count : counts) {
			sum += count.get();
		}
		context.write(new Text("dummyKey"), new LongWritable(sum));
	}
}
