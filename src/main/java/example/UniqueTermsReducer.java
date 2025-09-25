package com.example;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UniqueTermsReducer extends Reducer<Text, Text, Text, Text> {

  private static final Text TAG_DOCSIZE = new Text("DOCSIZE");
  private static final Text TAG_DOCPAIR = new Text("DOCPAIR");
  private final Text outVal = new Text();

  @Override
  protected void reduce(Text term, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {

    // Dedup docIds defensively (one occurrence per (term,doc))
    Set<String> docs = new HashSet<>();
    for (Text v : values) {
      docs.add(v.toString());
    }

    // 1) For each doc containing this term, emit a DOCSIZE contribution
    for (String d : docs) {
      outVal.set(d);                 // "<docId>"
      context.write(TAG_DOCSIZE, outVal); // "DOCSIZE\t<docId>"
    }

    // 2) For intersections, emit all unordered pairs A,B (A<B)
    ArrayList<String> docList = new ArrayList<>(docs);
    Collections.sort(docList);
    int n = docList.size();
    for (int i = 0; i < n; i++) {
      for (int j = i + 1; j < n; j++) {
        outVal.set(docList.get(i) + "," + docList.get(j)); // "A,B"
        context.write(TAG_DOCPAIR, outVal);                // "DOCPAIR\tA,B"
      }
    }
  }
}
