package MapReduce;

import com.alibaba.fastjson.JSON;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @BelongsProject: MapReduce
 * @BelongsPackage: mapReduce
 * @Author: FUJIWARA_ROOKIE
 * @Date: 2022/9/19 12:37
 */
public class MyMaper extends Mapper<LongWritable, Text, Students, NullWritable> {
    Students k = new Students();
    Students students = new Students();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        students = JSON.parseObject(value.toString(), Students.class);
        k.set(students.getId(),students.getClassName(),students.getAge(),students.getSex(),students.getClassName());
        context.write(k, NullWritable.get());
    }
}
