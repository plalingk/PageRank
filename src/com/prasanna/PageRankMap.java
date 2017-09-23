/***
 * Author: Prasanna Lalingkar
 * Email: plalingk@uncc.edu
 * 
 * This is the Page Rank Map Class. It takes input from the Base job and performs one iteration of the page rank algorithm (along with 
 * Page Rank Reduce). 
 * 
 * Input: <Key, Value> => <Offset, Page + (currentRank + URLs)>
 * Output1: <Key, Value> => <Page, Rank>
 * Output2: <Key, Value> => <Page, URL_List>
 */

package com.prasanna;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PageRankMap extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable offset, Text currRank_urlList, Context context)
			throws IOException, InterruptedException {
		try {
			// Extracting urls and ranks from the input data
			String[] line = currRank_urlList.toString().split("\t");
			String pageName = line[0];
			String[] rankAndUrls = line[1].split("@@@@");
			Double currentRank = Double.parseDouble(rankAndUrls[0]);
			String[] Urls = rankAndUrls[1].split("####");
			int length = Urls.length;

			// Writing two types of outputs - Rank and URL list
			for (int i = 0; i < length; i++) {
				context.write(new Text(Urls[i]), new Text("rank!!!!" + Double.toString(currentRank / length)));
			}
			context.write(new Text(pageName), new Text("url!!!!" + rankAndUrls[1].toString()));
		} catch (Exception e) {
			return;
		}
	}
}
