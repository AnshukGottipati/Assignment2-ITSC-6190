package com.example;

import java.io.IOException;
import java.util.Locale;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DocSizePairMapper extends Mapper<LongWritable, Text, Text, Text> {
  private final Text outKey = new Text();
  private final Text outVal = new Text();

  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
        String line = value.toString().trim();
        if (line.isEmpty()) return;
        
        String[] parts = value.toString().split("\\t",2);
        if (parts.length != 2) {
            return; // skip malformed lines
        }
        outKey.set(parts[0]); //DOCSIZE or DOCPAIR
        outVal.set(parts[1]); //document Name or docA,docB
        context.write(outKey,outVal);
      }
}
 