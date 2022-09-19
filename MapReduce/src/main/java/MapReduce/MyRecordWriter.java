package MapReduce;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * @BelongsProject: MapReduce
 * @BelongsPackage: mapReduce
 * @Author: FUJIWARA_ROOKIE
 * @Date: 2022/9/19 13:00
 */
public class MyRecordWriter extends RecordWriter<Students, NullWritable> {
    FSDataOutputStream stream1;
    FSDataOutputStream stream2;
    FSDataOutputStream stream3;
    FSDataOutputStream stream4;

    public MyRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException {
        FileSystem fileSystem = FileSystem.get(taskAttemptContext.getConfiguration());
        stream1 = fileSystem.create(new Path("src/main/java/output\\理科_男.txt"));
        stream2 = fileSystem.create(new Path("src/main/java/output\\理科_女.txt"));
        stream3 = fileSystem.create(new Path("src/main/java/output\\文科_男.txt"));
        stream4 = fileSystem.create(new Path("src/main/java/output\\文科_女.txt"));
    }

    public void write(Students key, NullWritable value) throws IOException, InterruptedException {
        if ("男".equals(key.getSex())) {
            if (key.getClassName().startsWith("理")) {
                stream1.write(key.toString().getBytes());
                stream1.write("\n".getBytes());
            } else {
                stream3.write(key.toString().getBytes());
                stream3.write("\n".getBytes());
            }
        } else {
            if (key.getClassName().startsWith("理")) {
                stream2.write(key.toString().getBytes());
                stream2.write("\n".getBytes());
            } else {
                stream4.write(key.toString().getBytes());
                stream4.write("\n".getBytes());
            }
        }
    }

    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        stream1.close();
        stream2.close();
        stream3.close();
        stream4.close();
    }
}
