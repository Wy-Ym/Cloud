package MapReduce;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @BelongsProject: MapReduce
 * @BelongsPackage: mapReduce
 * @Author: FUJIWARA_ROOKIE
 * @Date: 2022/9/19 12:45
 */
public class MyOutputFormat extends FileOutputFormat<Students, NullWritable> {
    public RecordWriter<Students, NullWritable> getRecordWriter(TaskAttemptContext job) throws IOException {
        return new MyRecordWriter(job);
    }
}
