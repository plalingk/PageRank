/***
 * Author: Prasanna Lalingkar
 * Email: plalingk@uncc.edu
 * 
 * This is the Base Reduce Class and it is an identity function ie. it replicates the input as is to the output.
 * 
 * Input: <Key, Value> => <URL, Page Contents>
 * Output: <Key, Value> => <URL, (Initial PageRank, List-of-Urls)>
 * 
 */
package com.prasanna;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class BaseReduce extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text title, Text rankAndList, Context context) throws IOException, InterruptedException {
		context.write(title, rankAndList);
	}

}
