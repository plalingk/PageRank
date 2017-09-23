/***
 * Author: Prasanna Lalingkar
 * Email: plalingk@uncc.edu
 * 
 * This is the Page Rank Reduce Class. It takes input from the Base job and performs one iteration of the page rank algorithm (along with 
 * Page Rank Reduce). 
 * 
 * Input: <Key, Value> => <Page, Rank/URL_List (Iterable)>
 * Output: <Key, Value> => <URL (In URL_List), (Rank, URL_List)>
 */

package com.prasanna;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PageRankReduce extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text url, Iterable<Text> value, Context context) throws IOException, InterruptedException {
		double sum = 0.0;
		double damping = 0.85;
		StringBuilder sb = new StringBuilder();

		for (Text v : value) {
			String[] val = v.toString().split("!!!!");
			if (val[0].equals("rank")) {
				sum += Double.parseDouble(val[1]);
				continue;
			}
			if (val[0].equals("url")) {
				sb.append(val[1]);
				continue;
			}
		}
		// Calculating the new page rank
		double rank = (1 - damping) + (damping * sum);
		context.write(url, new Text(Double.toString(rank) + "@@@@" + sb.toString()));
	}
}
