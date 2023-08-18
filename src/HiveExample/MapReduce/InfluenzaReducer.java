package HiveExample.MapReduce;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class InfluenzaReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable totalCases = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws java.io.IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        totalCases.set(sum);
        context.write(key, totalCases);
    }
}