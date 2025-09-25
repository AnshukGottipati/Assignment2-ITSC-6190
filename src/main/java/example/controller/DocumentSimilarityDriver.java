package com.example.controller;

import java.net.URI;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

// Adjust these imports to match your actual package names
import com.example.UniqueTermsMapper;
import com.example.UniqueTermsReducer;
import com.example.DocSizePairMapper;
import com.example.DocSizePairReducer;
import com.example.ComputeJacardyMapper;

public class DocumentSimilarityDriver extends Configured implements Tool {

  @Override
  public int run(String[] args) throws Exception {
    if (args.length < 3) {
      System.err.println("Usage: DocumentSimilarityDriver <input> <stage2_out> <final_out> [extra_cache_files...]");
      return 2;
    }

    Configuration conf = getConf();
    Path input    = new Path(args[0]); // dataset dir or file(s)
    Path stage2   = new Path(args[1]); // output of Job 2 (also input to Job 3)
    Path finalOut = new Path(args[2]);

    FileSystem fs = FileSystem.get(conf);

    // Temp for stage 1
    String runId = UUID.randomUUID().toString().replace("-", "");
    Path stage1 = new Path("/tmp/Jacardy_stage1_" + runId);

    // Clean any pre-existing outputs (okay if they don't exist)
    for (Path p : new Path[]{stage1, stage2, finalOut}) {
      if (fs.exists(p)) fs.delete(p, true);
    }

    // ---------------- Job 1: term -> [docs]  => emits inputs for size/pairs
    Job j1 = Job.getInstance(conf, "DocumentSimilarity-Stage1");
    j1.setJarByClass(DocumentSimilarityDriver.class);
    j1.setMapperClass(UniqueTermsMapper.class);
    j1.setReducerClass(UniqueTermsReducer.class);
    j1.setMapOutputKeyClass(Text.class);
    j1.setMapOutputValueClass(Text.class);
    j1.setOutputKeyClass(Text.class);
    j1.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(j1, input);
    FileOutputFormat.setOutputPath(j1, stage1);
    if (!j1.waitForCompletion(true)) return 1;

    // ---------------- Job 2: aggregate => SIZEOFDOC/SZ and INTERSECTION/IN
    Job j2 = Job.getInstance(conf, "DocumentSimilarity-Stage2");
    j2.setJarByClass(DocumentSimilarityDriver.class);
    j2.setMapperClass(DocSizePairMapper.class);
    j2.setReducerClass(DocSizePairReducer.class);
    j2.setMapOutputKeyClass(Text.class);
    j2.setMapOutputValueClass(Text.class);
    j2.setOutputKeyClass(Text.class);
    j2.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(j2, stage1);
    FileOutputFormat.setOutputPath(j2, stage2);
    if (!j2.waitForCompletion(true)) return 1;

    // ---------------- Job 3: mapper-only Jaccard computation
    Job j3 = Job.getInstance(conf, "DocumentSimilarity-Stage3");
    j3.setJarByClass(DocumentSimilarityDriver.class);
    j3.setMapperClass(ComputeJacardyMapper.class);
    j3.setNumReduceTasks(0);
    j3.setMapOutputKeyClass(Text.class);
    j3.setMapOutputValueClass(Text.class);
    j3.setOutputKeyClass(Text.class);
    j3.setOutputValueClass(Text.class);

    // Input is the whole stage2 directory; mapper filters to INTERSECTION/IN lines
    FileInputFormat.addInputPath(j3, stage2);
    FileOutputFormat.setOutputPath(j3, finalOut);

    // Add ALL stage2 part files as cache files so mapper can load SIZEOFDOC/SZ lines
    PathFilter partFilter = new PathFilter() {
      @Override public boolean accept(Path p) {
        String name = p.getName();
        return name.startsWith("part-");
      }
    };
    for (FileStatus st : fs.listStatus(stage2, partFilter)) {
      j3.addCacheFile(new URI(st.getPath().toString()));
    }
    // Also allow extra cache files from CLI if provided
    for (int i = 3; i < args.length; i++) {
      j3.addCacheFile(new URI(args[i]));
    }

    return j3.waitForCompletion(true) ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    int exit = ToolRunner.run(new Configuration(), new DocumentSimilarityDriver(), args);
    System.exit(exit);
  }
}
