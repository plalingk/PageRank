
/***
 * Author: Prasanna Lalingkar
 * Email: plalingk@uncc.edu
 * 
 * This is the Base Map Class and is responsible for setting the initial page rank for all the pages.
 * 
 * Input: <Key, Value> => <Offset, URL + Page Contents>
 * Output: <Key, Value> => <URL, (Initial PageRank, List-of-Urls)>
 */

package com.prasanna;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class BaseMap extends Mapper<LongWritable, Text, Text, Text> {
	private static long N; // Total number of Pages in the Corpus
	private double oneByN;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		N = Long.parseLong(context.getConfiguration().get("N"));
		oneByN = (1.00 / N);
	}

	public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {
		String line = lineText.toString();
		Pattern pattern;
		Matcher matcher;
		StringBuilder sb = new StringBuilder();
		String listOfUrls;
		try {
			if (line != null && !line.trim().equals("")) {
				/*
				 * Finding the page title, pageContents and URLs in the page
				 * using a pattern and matcher for it
				 */
				pattern = Pattern.compile("<title>(.+?)</title>");
				matcher = pattern.matcher(line);
				matcher.find();
				String title = matcher.group(1);

				pattern = Pattern.compile("<text(.+?)</text>");
				matcher = pattern.matcher(line);
				matcher.find();
				String pageContents = matcher.group(1);

				pattern = Pattern.compile("\\[\\[.*?]\\]");
				matcher = pattern.matcher(pageContents);

				/*
				 * Creating a String of all urls present in a particular page
				 * the urls are seperated by a delimiter ' ??####?? '
				 */
				int count = 0;
				while (matcher.find()) {
					String url = matcher.group().replace("[[", "").replace("]]", "");
					if (!url.isEmpty()) {
						if (count == 0) {
							sb.append(url);
						} else {
							sb.append("####".concat(url));
						}
						count++;
					}
				}

				listOfUrls = sb.toString();
				/*
				 * Writing the key, value pair to output, key = page title value
				 * = initial_pagerank ??@@@@?? url1 ??####?? url2 ??####?? url3
				 */
				context.write(new Text(title.trim()),
						new Text(Double.toString(oneByN).concat("@@@@".concat(listOfUrls))));
			}
		} catch (Exception e) {
			return;
		}
	}

}
