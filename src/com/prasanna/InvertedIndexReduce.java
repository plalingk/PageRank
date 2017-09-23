/***
 * Author: Prasanna Lalingkar
 * Email: plalingk@uncc.edu
 * 
 * This is the Inverted Index Reduce Class. It reads the input from the Inverted Index map and collects together all the 
 * 
 * Input: <Key, Value> => <URL(Contained in the page), URL(Title)(Iterable)>
 * Output: <Key, Value> => <URL, All pages it is contained in>
 */

package com.prasanna;

import java.io.IOException;
import java.util.Arrays;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InvertedIndexReduce extends Reducer<Text, Text, Text, Text> {
	/*
	 * Iterating over all the links a page name is contained in and adding them
	 * to a String
	 */
	public void reduce(Text url, Iterable<Text> titles, Context context) throws IOException, InterruptedException {
		StringBuilder sb = new StringBuilder();
		int count = 0;
		for (Text t : titles) {
			if (count == 0) {
				sb.append(t);
			} else {
				sb.append("@@@@" + t);
			}
			count++;
		}
		/*
		 * Sorting the created inverted index by name
		 */
		String[] tempString = sb.toString().split("@@@@");
		Arrays.sort(tempString);
		StringBuilder sb1 = new StringBuilder();
		count = 0;
		for (int i = 0; i < tempString.length; i++) {
			if (count == 0) {
				sb1.append(tempString[i]);
			} else {
				sb1.append(" @@@@ " + tempString[i]);
			}
			count++;
		}
		/*
		 * outputting key value pair for a page and list of all pages it is
		 * contained in The list is seperated by a delimiter '@@@@'
		 */
		context.write(url, new Text("  --->  ".concat(sb1.toString())));
	}
}
