package com.example;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DocSizePairReducer extends Reducer<Text, Text, Text, Text> {

  private static final Text TAG_SIZEOFDOC   = new Text("SIZEOFDOC");   // outputs: SIZEOFDOC\t<doc>\t<count>
  private static final Text TAG_INTERSECTION = new Text("INTERSECTION"); // outputs: INTERSECTION\tA,B\t<count>

  @Override
  protected void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {

    String tag = key.toString();

    if ("DOCSIZE".equals(tag)) {
      // Count unique-term contributions per doc
      Map<String, Integer> docSizeMap = new HashMap<>();
      for (Text v : values) {
        String doc = v.toString();
        docSizeMap.put(doc, docSizeMap.getOrDefault(doc, 0) + 1);
      }
      // Emit: SIZEOFDOC\t<docId>\t<count>
      for (Map.Entry<String, Integer> e : docSizeMap.entrySet()) {
        context.write(TAG_SIZEOFDOC, new Text(e.getKey() + "\t" + e.getValue()));
      }

    } else if ("DOCPAIR".equals(tag)) {
      // Count intersections per unordered pair A,B
      Map<String, Integer> pairCountMap = new HashMap<>();
      for (Text v : values) {
        String pair = v.toString(); // "A,B"
        pairCountMap.put(pair, pairCountMap.getOrDefault(pair, 0) + 1);
      }
      // Emit: INTERSECTION\tA,B\t<count>
      for (Map.Entry<String, Integer> e : pairCountMap.entrySet()) {
        context.write(TAG_INTERSECTION, new Text(e.getKey() + "\t" + e.getValue()));
      }
    }
  }
}
