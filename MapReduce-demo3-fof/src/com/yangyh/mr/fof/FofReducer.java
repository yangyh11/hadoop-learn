package com.yangyh.mr.fof;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @description:
 * @author: yangyh
 * @create: 2019-11-05 10:13
 */
public class FofReducer extends Reducer<MyKey, Text, Text, NullWritable> {

    @Override
    protected void reduce(MyKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int i = 2;

        for (Text value : values) {
            if (i > 0) {
                context.write(value, NullWritable.get());
                i--;
            } else {
                break;
            }
        }

    }
}
