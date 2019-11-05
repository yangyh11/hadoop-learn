package com.yangyh.mr.weather;

import com.yangyh.mr.weather.mapper.WeatherMapper;
import com.yangyh.mr.weather.reducer.WeatherReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.util.Arrays;

/**
 * @description:
 * @author: yangyh
 * @create: 2019-11-05 19:41
 */
public class MainClass {
    public static void main(String[] args) throws Exception {

        if (args == null || args.length != 2) {
            System.err.println("参数错误" + Arrays.toString(args));
            System.exit(1);
        }

        Configuration configuration = new Configuration(true);
        configuration.set("mapreduce.framework.name", "local");

        Job job = Job.getInstance(configuration);

        job.setJarByClass(MainClass.class);
        job.setJobName("天气案例");

        // 设置输入输出路径
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 设置Mapper类和Reducer类
        job.setMapperClass(WeatherMapper.class);
        job.setReducerClass(WeatherReducer.class);

        // 设置map输出的key和value的类型
        job.setMapOutputKeyClass(WeatherKey.class);
        job.setMapOutputValueClass(Text.class);

        // 设置排序比较器
        job.setSortComparatorClass(WeatherSortCompartor.class);


        // 设置分组比较器
        job.setGroupingComparatorClass(WeatherGroupingCompartor.class);

        // 设置分区器
        job.setPartitionerClass(WeatherPartitioner.class);

        // 设置reducer的数量
        job.setNumReduceTasks(2);

        // 提交作业
        job.waitForCompletion(true);


    }

}
