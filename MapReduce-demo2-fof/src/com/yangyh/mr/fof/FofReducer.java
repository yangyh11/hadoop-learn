package com.yangyh.mr.fof;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @description:
 * @author: yangyh
 * @create: 2019-11-05 10:13
 */
public class FofReducer extends Reducer<Text, Text, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (Text value : values) {
            if ("R".equals(value.toString())) {
                return;
            } else {
                sum++;
            }
        }
        context.write(key, new IntWritable(sum));
    }
}
