package MapReduce;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @BelongsProject: MapReduce
 * @BelongsPackage: mapReduce
 * @Author: FUJIWARA_ROOKIE
 * @Date: 2022/9/19 14:42
 */
public class MyReduce extends Reducer<Students, NullWritable, Students, NullWritable> {
    @Override
    protected void reduce(Students key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        for (NullWritable ignored :values){
            context.write(key,NullWritable.get());
        }
    }
}

