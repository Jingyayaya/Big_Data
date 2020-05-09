package assignment2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

// The mapper used to compute the home runs for every player in each year

public  class HRPlayerMapper extends Mapper<Object, Text, Text, Text> {
    public static String DELEIMETER=",";

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        if(key.toString().equals("0"))
            return;

        String[] strSplits=value.toString().split(DELEIMETER);
        String playerId= strSplits[0];
        String yearId=strSplits[1];

        StringBuilder newkey=new StringBuilder(playerId);
        newkey.append(DELEIMETER)
                .append(yearId);

        context.write(new Text(newkey.toString()), value);
    }
}