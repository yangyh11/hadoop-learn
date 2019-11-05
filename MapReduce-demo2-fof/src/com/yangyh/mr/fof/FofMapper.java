package com.yangyh.mr.fof;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @description:
 * @author: yangyh
 * @create: 2019-11-05 10:13
 */
public class FofMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] values = line.split(" ");
        for (int i = 0; i < values.length - 1; i++) {
            for (int j = i + 1; j < values.length; j++) {
                if (i == 0) {
                    context.write(new Text(getNames(values[i], values[j])), new Text("R"));
                } else {
                    context.write(new Text(getNames(values[i], values[j])), new Text("G"));
                }
            }
        }
    }


    private String getNames(String name1, String name2) {
        int n = name1.compareTo(name2);
        if (n > 0) {
            // 字典序小的姓名在前面
            return name2 + "_" + name1;
        } else {
            return name1 + "_" + name2;
        }
    }
}
