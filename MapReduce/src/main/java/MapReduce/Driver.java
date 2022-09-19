package MapReduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;

/**
 * @BelongsProject: MapReduce
 * @BelongsPackage: mapReduce
 * @Author: FUJIWARA_ROOKIE
 * @Date: 2022/9/19 11:43
 */
public class Driver {
    public static void main(String[] args) throws Exception {
        File file = new File("output");
        if (file.exists()) {
            delFile(file);
        }
        driver();
    }

    public static void delFile(File file) {
        File[] files = file.listFiles();
        if (files != null && files.length != 0) {
            for (File value : files) {
                delFile(value);
            }
        }
        file.delete();
    }

    public static void driver() throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setMapperClass(MyMaper.class);
        job.setJarByClass(Driver.class);
        job.setReducerClass(MyReduce.class);
        job.setMapOutputKeyClass(Students.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(Students.class);
        job.setOutputValueClass(NullWritable.class);
        job.setOutputFormatClass(MyOutputFormat.class);
        FileInputFormat.setInputPaths(job, "src/main/java/file/result.json");
        FileOutputFormat.setOutputPath(job, new Path("success"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
