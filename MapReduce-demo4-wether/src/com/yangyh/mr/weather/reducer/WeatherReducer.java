package com.yangyh.mr.weather.reducer;

import com.yangyh.mr.weather.WeatherKey;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


/**
 * @description:
 * @author: yangyh
 * @create: 2019-11-05 19:55
 */
public class WeatherReducer extends Reducer<WeatherKey, Text, Text, NullWritable> {

    @Override
    protected void reduce(WeatherKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        // 数据已经温度倒序，取前两位并且不是同一天的即可
        int day = -1;
        for (Text value : values) {
            if (day == -1) {
                context.write(value, NullWritable.get());
                day = key.getDay();
            } else if (day != key.getDay()) {
                context.write(value,NullWritable.get());
                break;
            }
        }

    }
}
