package com.yangyh.mr.weather;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @description: 分区器
 * @author: yangyh
 * @create: 2019-11-05 20:09
 */
public class WeatherPartitioner extends Partitioner<WeatherKey, Text> {

    @Override
    public int getPartition(WeatherKey weatherKey, Text o2, int numPartitions) {
        // 为防止数据倾斜，按照月份分组
        return (weatherKey.getMonth().hashCode() & Integer.MAX_VALUE) % numPartitions;
    }
}
