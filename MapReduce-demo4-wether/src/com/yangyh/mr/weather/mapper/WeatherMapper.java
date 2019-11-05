package com.yangyh.mr.weather.mapper;

import com.yangyh.mr.weather.WeatherKey;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @description:
 * @author: yangyh
 * @create: 2019-11-05 19:55
 */
public class WeatherMapper extends Mapper<LongWritable, Text, WeatherKey, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 1949-10-01 14:21:02	34c
        String valueStr = value.toString();
        String[] values = valueStr.split("\t");

        String value1 = values[0];
        String[] time = value1.substring(0,10).split("-");
        Integer year = Integer.valueOf(time[0]);
        Integer month = Integer.valueOf(time[1]);
        Integer day = Integer.valueOf(time[2]);

        String value2 = values[1];
        Integer temperature = Integer.valueOf(value2.substring(0, value2.length() - 1));

        WeatherKey weatherKey = new WeatherKey(year, month, day, temperature);

        context.write(weatherKey, value);

    }
}
