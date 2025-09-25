package com.example;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ComputeJacardyMapper extends Mapper<LongWritable, Text, Text, Text> {

  private final Map<String, Integer> docSizes = new HashMap<>();
  private final Text outKey = new Text();
  private final Text outVal = new Text();

  @Override
  protected void setup(Context ctx) throws IOException {
    URI[] cacheFiles = ctx.getCacheFiles();
    if (cacheFiles == null) return;

    for (URI uri : cacheFiles) {
      try (BufferedReader br = new BufferedReader(new FileReader(new File(uri.getPath())))) {
        String line;
        while ((line = br.readLine()) != null) {
          String[] parts = line.split("\\t");
          if (parts.length == 3 && ("SZ".equals(parts[0]) || "SIZEOFDOC".equals(parts[0]))) {
            try {
              docSizes.put(parts[1], Integer.parseInt(parts[2]));
            } catch (NumberFormatException ignore) {}
          }
        }
      }
    }
  }

  @Override
  protected void map(LongWritable key, Text value, org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, Text>.Context context)
      throws IOException, InterruptedException {

    String line = value.toString().trim();
    if (line.isEmpty()) return;

    String[] parts = line.split("\\t");
    if (parts.length != 3) return;

    String tag = parts[0];
    if (!( "IN".equals(tag) || "INTERSECTION".equals(tag) )) return;

    String[] ab = parts[1].split(",", 2);
    if (ab.length != 2) return;
    String A = ab[0];
    String B = ab[1];

    int inter;
    try {
      inter = Integer.parseInt(parts[2]);
    } catch (NumberFormatException e) {
      return;
    }

    Integer sizeA = docSizes.get(A);
    Integer sizeB = docSizes.get(B);
    if (sizeA == null || sizeB == null) return;

    int union = sizeA + sizeB - inter;
    if (union <= 0) return;

    double jaccard = (double) inter / (double) union;

    outKey.set(A + ", " + B);
    outVal.set(String.format("Similarity: %.6f", jaccard));
    context.write(outKey, outVal);
  }
}
