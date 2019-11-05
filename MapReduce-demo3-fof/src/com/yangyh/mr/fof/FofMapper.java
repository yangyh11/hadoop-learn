package com.yangyh.mr.fof;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @description:
 * @author: yangyh
 * @create: 2019-11-05 10:13
 */
public class FofMapper extends Mapper<Text, Text, MyKey, Text> {

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {

//        cat_hadoop	2
        String keyStr = key.toString();
        String[] keys = keyStr.split("_");

        int num = Integer.valueOf(value.toString());

        MyKey myKey = new MyKey(keys[0], keys[1], num);
        MyKey myKey2 = new MyKey(keys[1], keys[0], num);

        context.write(myKey, new Text(myKey.toString()));
        context.write(myKey2, new Text(myKey2.toString()));
    }


}
