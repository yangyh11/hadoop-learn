package com.yangyh.mr.fof;

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
 * @create: 2019-11-05 10:14
 */
public class MainClass {

    public static void main(String[] args) throws Exception {

        if (args == null || args.length != 2) {
            System.err.println("参数错误" + Arrays.toString(args));
            System.exit(1);
        }

        Configuration configuration = new Configuration(true);
        // 本地运行
        configuration.set("mapreduce.framework.name", "local");
        Job job = Job.getInstance(configuration);

        job.setJobName("好友推荐");
        job.setJarByClass(MainClass.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(FofMapper.class);
        job.setReducerClass(FofReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.waitForCompletion(true);

    }
}
