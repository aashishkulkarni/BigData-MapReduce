package HiveExample.MapReduce;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class InfluenzaMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
  private Text state = new Text();
  private IntWritable cases = new IntWritable();

  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws java.io.IOException, InterruptedException {
    String[] tokens = value.toString().split(",");

    if (tokens.length >= 2) {
      // Assuming the state is in the second column and cases in the third column
      state.set(tokens[1]);
      cases.set(Integer.parseInt(tokens[2]));
      context.write(state, cases);
    }
  }
}