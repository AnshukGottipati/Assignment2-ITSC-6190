package com.example;

import java.io.IOException;
import java.util.Locale;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.HashSet;

public class UniqueTermsMapper extends Mapper<LongWritable, Text, Text,Text> {
  private final Text outKey = new Text();
  private final Text outVal = new Text();

  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

    // One input line per call: "DocumentX term1 term2 ..."
    String line = value.toString().trim();
    if (line.isEmpty()) return;

    // Normalize & remove punctuation (keeps letters, digits, whitespace)
    line = line.toLowerCase(Locale.ROOT)
               .replaceAll("[^\\p{L}\\p{N}\\s]", " ")
               .replaceAll("\\s+", " ")
               .trim();

    String[] tokens = line.split("\\s+");
    if (tokens.length < 2) return;         // need at least docId + one term

    String docId = tokens[0];


    HashSet<String> uniqueTerms = new HashSet<>();

    for (int i = 1; i < tokens.length; i++) {
      String term = tokens[i];
      if (!term.isEmpty()) {
        uniqueTerms.add(term);
      }
    }
    for (String term : uniqueTerms) {
      outKey.set(term);
      outVal.set(docId);

      context.write(outKey,outVal);
    }
  }
}
