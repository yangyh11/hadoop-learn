package com.yangyh.mr.fof;

import org.apache.hadoop.mapreduce.Partitioner;

import javax.xml.soap.Text;

/**
 * @description:
 * @author: yangyh
 * @create: 2019-11-05 14:48
 */
public class MyPartitioner extends Partitioner<MyKey, Text> {

    @Override
    public int getPartition(MyKey myKey, Text text, int numPartitions) {
        return (myKey.getName().hashCode() & Integer.MAX_VALUE) % numPartitions;
    }
}
