package assignment2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

// Compute the result based on the condition of playerHR > teamHR && playerYear==teamYear.
public  class CompareReducer extends Reducer<IntWritable, Text, NullWritable, Text> {
    public static String DELEIMETER=",";
    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        ArrayList<String> playerhr_count_table=new ArrayList<>();
        ArrayList<String> teamhr_count_table=new ArrayList<>();

        int flag=0;
        for(Text v:values){
            String[] strSPlits=v.toString().split(DELEIMETER);
            if(strSPlits != null && strSPlits.length > 0)
                flag=Integer.parseInt(strSPlits[0]);

            int indexof= v.toString().indexOf(",");
            if(flag==1){
                playerhr_count_table.add(v.toString().substring(indexof+1));
            }
            else if(flag==2){
                teamhr_count_table.add(v.toString().substring(indexof+1));
            }
        }

        for(String player: playerhr_count_table){
            String[] player_splits=player.split(DELEIMETER);
            int playerHR=Integer.parseInt(player_splits[2]);
            int playerYear=Integer.parseInt(player_splits[1]);

            for(String team: teamhr_count_table){
                String[] team_splits=team.split(DELEIMETER);
                int teamHR=Integer.parseInt(team_splits[3]);
                int teamYear=Integer.parseInt(team_splits[1]);

                StringBuilder sb=new StringBuilder();
                if(playerHR > teamHR && playerYear==teamYear){
                    sb.append(player_splits[0])
                            .append(DELEIMETER)
                            .append(player_splits[1])
                            .append(DELEIMETER)
                            .append(team_splits[0])
                            .append(DELEIMETER)
                            .append(team_splits[2]);
                    context.write(NullWritable.get(), new Text(sb.toString()));
                }
            }
        }
    }
}