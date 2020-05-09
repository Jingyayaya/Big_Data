package assignment2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

// The reducer used to join the result with TeamsFranchises.csv franchisesid

public class TeamNameReducer extends Reducer<IntWritable, Text, NullWritable, Text> {
    public static String DELEIMETER=",";

    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        ArrayList<String> hr_table=new ArrayList<>();
        ArrayList<String> franchise_table=new ArrayList<>();

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
                franchise_table.add(v.toString().substring(indexof+1));
            }
        }

        for(String hr: hr_table){
            String[] hrSplits=hr.split(DELEIMETER);
            String franchiseid=hrSplits[3];

            for(String franchise: franchise_table){
                String[] strSplits=franchise.split(DELEIMETER);
                String id=strSplits[0];

                StringBuilder sb=new StringBuilder();
                if(franchiseid.equals(id)){
                    sb.append(hr)
                            .append(DELEIMETER)
                            .append(strSplits[1]);

                    context.write(NullWritable.get(), new Text(sb.toString()));
                }
            }
        }
    }

}
