package assignment2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

// The reducer used to join the result with People.csv on playerid
public class PlayerNameReducer extends Reducer<IntWritable, Text, NullWritable, Text> {
    public static String DELEIMETER=",";
    public static int mordern_era=1900;

    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        ArrayList<String> hr_table=new ArrayList<>();
        ArrayList<String> people_table=new ArrayList<>();

        int flag=0;
        for(Text v:values){
            String[] strSPlits=v.toString().split(DELEIMETER);
            if(strSPlits != null && strSPlits.length > 0)
                flag=Integer.parseInt(strSPlits[0]);

            int indexof= v.toString().indexOf(",");
            if(flag==1){
                hr_table.add(v.toString().substring(indexof+1));
            }
            else if(flag==2){
                people_table.add(v.toString().substring(indexof+1));
            }
        }

        for(String hr: hr_table){
            String[] hrSplits=hr.split(DELEIMETER);

            String playerId=hrSplits[0];
            String team_name=hrSplits[hrSplits.length -1];
            int yearid= Integer.parseInt(hrSplits[1]);


            for(String people: people_table){
                String[] peopleSplits=people.split(DELEIMETER);
                String id=peopleSplits[0];

                StringBuilder sb=new StringBuilder();
                if(playerId.equals(id) && yearid > mordern_era){
                    sb.append(team_name)
                            .append(" ")
                            .append(peopleSplits[13])
                            .append(" ")
                            .append(peopleSplits[14])
                            .append(" ")
                            .append(yearid);
                    context.write(NullWritable.get(), new Text(sb.toString()));
                }
            }
        }
    }
}
