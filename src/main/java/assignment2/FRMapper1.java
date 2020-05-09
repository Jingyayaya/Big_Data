package assignment2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

// fragment and replicate the file.
public  class FRMapper1 extends Mapper<Object, Text, IntWritable, Text> {
    public static String DELEIMETER=",";

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        StringBuilder s=new StringBuilder("1");
        s.append(",").append(value.toString());

        int index=(key.hashCode() & Integer.MAX_VALUE) % 3;
        for(int i=0; i < 3; i++){
            context.write(new IntWritable(index*3+i), new Text(s.toString()));
        }

    }
}
