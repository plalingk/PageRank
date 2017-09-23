/***
 * Author: Prasanna Lalingkar
 * Email: plalingk@uncc.edu
 * 
 * This is the Inverted Index Map Class. It reads the input page line by line and outputs pairs for all links in the page and
 * its corresponding page title.
 * 
 * Input: <Key, Value> => <URL (Title), Page Contents>
 * Output: <Key, Value> => <URL(Contained in the page), URL(Title)>
 */

package com.prasanna;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class InvertedIndexMap extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable offset, Text lineText, Context context) throws IOException, InterruptedException {
		String line = lineText.toString();
		Pattern pattern;
		Matcher matcher;
		try {
			/*
			 * Extract the title and links in a page and output a link and title
			 * pair as a key and value
			 */
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

				while (matcher.find()) {
					// Removing the nested [ ] symbols
					String url = matcher.group().replace("[[", "").replace("]]", "");
					if (!url.isEmpty()) {
						context.write(new Text(url), new Text(title.trim()));
					}
				}
			}
		} catch (Exception e) {
			return;
		}
	}

}
