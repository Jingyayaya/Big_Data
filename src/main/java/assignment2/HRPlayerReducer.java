package assignment2;

import com.google.common.base.Strings;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

// The reducer used to compute the home runs for every player in each year
public  class HRPlayerReducer extends Reducer<Text, Text, NullWritable, Text> {
    public static String DELEIMETER=",";

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Iterator<Text> iterator=values.iterator();
        String playerid="";
        String yearid="";

        int hr=0;

        String[] strSplits=null;

        while(iterator.hasNext()){
            strSplits=iterator.next().toString().split(DELEIMETER);
            if(strSplits !=null){
                if(Strings.isNullOrEmpty(playerid)){
                    playerid= strSplits[0];
                    yearid=strSplits[1];
                }
                hr+= Strings.isNullOrEmpty(strSplits[11])? 0:Integer.parseInt(strSplits[11]);
            }
        }

        if(!Strings.isNullOrEmpty(playerid)){
            StringBuilder s=new StringBuilder();
            s.append(playerid)
                    .append(DELEIMETER)
                    .append(yearid)
                    .append(DELEIMETER)
                    .append(hr);

            context.write(NullWritable.get(), new Text(s.toString()));
        }
    }
}

