package assignment2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

// The mapper used to compute the home runs for every team in each year

public  class HRTeamMapper extends Mapper<Object, Text, Text, Text> {
    public static String DELEIMETER=",";

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        if(key.toString().equals("0"))
            return;
        String[] strSPlits=value.toString().split(DELEIMETER);
        String teamid="";
        String yearid="";
        if(strSPlits != null && strSPlits.length > 1){
            teamid=strSPlits[2];
            yearid=strSPlits[0];
        }
        StringBuilder newkey=new StringBuilder(teamid);
        newkey.append(DELEIMETER)
                .append(yearid);

        context.write(new Text(newkey.toString()), value);

    }
}