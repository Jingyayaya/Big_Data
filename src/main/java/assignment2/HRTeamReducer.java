package assignment2;

import com.google.common.base.Strings;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

// The reducer used to compute the home runs for every team in each year
public  class HRTeamReducer extends Reducer<Text, Text, NullWritable, Text> {
    public static String DELEIMETER=",";

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Iterator<Text> iterator=values.iterator();
        String yearid="";
        String teamid="";
        String franchid="";

        int hr=0;

        String[] strSplits=null;

        while(iterator.hasNext()){
            strSplits=iterator.next().toString().split(DELEIMETER);
            if(strSplits !=null && StringUtils.isNumeric(strSplits[19])){
                if(Strings.isNullOrEmpty(teamid)){
                    yearid=strSplits[0];
                    teamid=strSplits[2];
                    franchid=strSplits[3];
                }
                hr+=Strings.isNullOrEmpty(strSplits[19])? 0: Integer.parseInt(strSplits[19]);
            }
        }

        if(!Strings.isNullOrEmpty(teamid)){
            StringBuilder s=new StringBuilder();
            s.append(teamid)
                    .append(DELEIMETER)
                    .append(yearid)
                    .append(DELEIMETER)
                    .append(franchid)
                    .append(DELEIMETER)
                    .append(hr);

            context.write(NullWritable.get(), new Text(s.toString()));
        }
    }
}