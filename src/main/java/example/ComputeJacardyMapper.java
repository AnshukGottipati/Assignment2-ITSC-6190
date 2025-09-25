package com.example;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ComputeJacardyMapper extends Mapper<LongWritable, Text, Text, Text> {

  private final Map<String, Integer> docSizes = new HashMap<>();
  private final Text outKey = new Text();
  private final Text outVal = new Text();

  @Override
  protected void setup(Context ctx) throws IOException, InterruptedException {
    Configuration conf = ctx.getConfiguration();

    // 1) Prefer an explicit side input path (HDFS/local/URI)
    String sidePath = conf.get("side.input.path");
    if (sidePath != null && !sidePath.trim().isEmpty()) {
      loadDocSizesFromPath(conf, new Path(sidePath));
      if (!docSizes.isEmpty()) return;
    }

    // 2) Fallback: any files provided via Distributed Cache (-files / job.addCacheFile)
    URI[] cacheFiles = ctx.getCacheFiles();
    if (cacheFiles != null) {
      for (URI uri : cacheFiles) {
        // If user used "#alias", the fragment is the local symlink name; otherwise use the path
        Path p = (uri.getFragment() != null) ? new Path(uri.getFragment()) : new Path(uri);
        loadDocSizesFromPath(conf, p);
      }
    }
  }

  /** Load SZ/SIZEOFDOC lines from a single file, directory, or glob (supports part-*) */
  private void loadDocSizesFromPath(Configuration conf, Path input) throws IOException {
    FileSystem fs = input.getFileSystem(conf);

    // If it's a directory, read all files under it that look like part files
    if (fs.isDirectory(input)) {
      for (FileStatus st : fs.listStatus(input)) {
        Path p = st.getPath();
        String name = p.getName();
        if (name.startsWith("part-")) {
          readOne(fs, p);
        }
      }
      return;
    }

    // If it’s a glob (e.g., /path/part-*)
    if (input.toString().contains("*") || input.toString().contains("?")) {
      FileStatus[] matches = fs.globStatus(input);
      if (matches != null) {
        for (FileStatus st : matches) {
          if (!st.isDirectory()) readOne(fs, st.getPath());
        }
      }
      return;
    }

    // Otherwise, assume it's a single file
    readOne(fs, input);
  }

  private void readOne(FileSystem fs, Path file) throws IOException {
    if (!fs.exists(file)) return;
    try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(file)))) {
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

  @Override
  protected void map(LongWritable key, Text value, Context context)
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
