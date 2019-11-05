package com.yangyh.mr.fof;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.util.Arrays;

/**
 * @description: 将demo2中mr作业输出的结果作为demo3作业的输入
 * @author: yangyh
 * @create: 2019-11-05 10:14
 */
public class MainClass {

    public static void main(String[] args) throws Exception {

        if (args == null || args.length != 2) {
            System.err.println("参数错误" + Arrays.toString(args));
            System.exit(1);
        }

        // 加载配置文件
        Configuration configuration = new Configuration(true);
        // 本地运行
        configuration.set("mapreduce.framework.name", "local");
        Job job = Job.getInstance(configuration);

        job.setJobName("好友推荐3");
        job.setJarByClass(MainClass.class);

        // 设置inputFormat的具体实现，key是行中第一个\t之前的部分，如果没有\t,整行就是key。value为空
        job.setInputFormatClass(KeyValueTextInputFormat.class);
//        KeyValueTextInputFormat.addInputPath(job, new Path(args[0]));

        // 设置输入输出路径
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 设置Mapper和Reducer类
        job.setMapperClass(FofMapper.class);
        job.setReducerClass(FofReducer.class);

        // 设置map输出key，value的类型
        job.setMapOutputKeyClass(MyKey.class);
        job.setMapOutputValueClass(Text.class);

        // 设置分组比较器
        job.setGroupingComparatorClass(MyGroupingComparator.class);

        // 设置分区
        job.setPartitionerClass(MyPartitioner.class);

        job.waitForCompletion(true);

    }
}
