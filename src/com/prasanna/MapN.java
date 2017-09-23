/***
 * Author: Prasanna Lalingkar
 * Email: plalingk@uncc.edu
 * 
 * This is the MapN Class. It reads the input data and outputs a dummy key and count as 1 for each row in the input data
 * This is done to count the number of the pages in the corpus. For each successfully parsed page in the corpus we return a 
 * dummy key with the value 1 associated with it.
 * 
 * Input: <Key, Value> => <URL, Page Contents>
 * Output: <Key, Value> => <dummyKey, 1>
 */

package com.prasanna;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapN extends Mapper<LongWritable, Text, Text, LongWritable> {

	public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {

		String line = lineText.toString();
		Pattern pattern;
		Matcher matcher;
		StringBuilder sb = new StringBuilder();
		String listOfUrls;
		/*
		 * Matching for patterns and extracting fields from it
		 */
		try {
			if (line != null && !line.trim().equals("")) {
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

				int count = 0;
				while (matcher.find()) {
					// Removing the nested [ ] symbols
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
				 * Writing to output only if the page and url is correctly
				 * parsed. The variables set here are only to ensure consistency
				 * when calculating N and actually parsing the documents while
				 * calculating pagerank
				 */
				context.write(new Text("dummyKey"), new LongWritable(1));
			}
		} catch (Exception e) {
			return;
		}
	}

}
