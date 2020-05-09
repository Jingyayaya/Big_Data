package assignment2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;


public class HRcount {
    public static String DELEIMETER=",";

    public static Job getCountJob(Configuration conf, String inputPath, String outputPath,Class mapper, Class reducer) throws IOException{
        Job job= Job.getInstance(conf, "HRCount");
        job.setJarByClass(HRcount.class);

        job.setMapperClass(mapper);
        job.setReducerClass(reducer);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        return job;
    }

    public static Job getFRJoinJob(Configuration conf, String inputPath1, String inputPath2, String outputPath, Class mapper1, Class mapper2, Class reducer)throws IOException{
        Job job= Job.getInstance(conf, "join");
        job.setJarByClass(HRcount.class);
        job.setReducerClass(reducer);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(inputPath1), TextInputFormat.class, mapper1);
        MultipleInputs.addInputPath(job, new Path(inputPath2), TextInputFormat.class, mapper2);

        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        return job;
    }

    public static void main(String[] args) throws IOException{
        if(args.length !=2){
            System.out.println("The first parameter is the value of fs.defaultFS, like hdfs://localhost:9000");
            System.out.println("The second parameter is the directory path that includes input csv files, like /usr/data/");
            System.exit(0);
        }
        Configuration conf=new Configuration();
        conf.set("fs.defaultFS",args[0]);

        String battingInputPath=args[1].endsWith("/")? args[1]+"Batting.csv": args[1]+"/"+"Batting.csv";

        String peoplePath=args[1].endsWith("/")? args[1]+"People.csv": args[1]+"/"+"People.csv";
        String teamInputPath=args[1].endsWith("/")? args[1]+"Teams.csv": args[1]+"/"+"Teams.csv";
        String teamsFranchisesPath= args[1].endsWith("/")? args[1]+"TeamsFranchises.csv": args[1]+"/"+"TeamsFranchises.csv";

        String playerhr_count="/users/output/playerHR";
        String teamhr_count="/users/output/teamHR";

        Job playerHR_count_job=getCountJob(conf,battingInputPath, playerhr_count,HRPlayerMapper.class, HRPlayerReducer.class);
        Job teamHR_count_job=getCountJob(conf, teamInputPath, teamhr_count, HRTeamMapper.class, HRTeamReducer.class);

        ControlledJob playerHR_count_controlled_job=new ControlledJob(playerHR_count_job,null);
        ControlledJob teamHR_count_controlled_job=new ControlledJob(teamHR_count_job,null);

        ArrayList<ControlledJob> order=new ArrayList<>();
        order.add(playerHR_count_controlled_job);
        order.add(teamHR_count_controlled_job);


        String compare_result="/users/output/compare_result";

        Job compare_join_job=getFRJoinJob(conf,playerhr_count,teamhr_count,compare_result ,FRMapper1.class, FRMapper2.class, CompareReducer.class);
        ControlledJob compare_join_controlled_job =new ControlledJob(compare_join_job,order);

        ArrayList<ControlledJob> order1=new ArrayList<>();
        order1.add(compare_join_controlled_job);
        String team_name_result="/users/output/team_name_result";

        Job teamid_name_join_job=getFRJoinJob(conf,compare_result,teamsFranchisesPath,team_name_result,FRMapper1.class, FRMapper2.class, TeamNameReducer.class);
        ControlledJob team_join_controlled_job=new ControlledJob(teamid_name_join_job,order1);

        ArrayList<ControlledJob> order2=new ArrayList<>();
        order2.add(team_join_controlled_job);
        String result="/users/Wang";
        Job playerid_name_join_job=getFRJoinJob(conf,team_name_result,peoplePath,result,FRMapper1.class, FRMapper2.class,PlayerNameReducer.class);
        ControlledJob player_join_controlled_job=new ControlledJob(playerid_name_join_job,order2);


        JobControl jc=new JobControl("job_chaining");
        //player HR computation
        jc.addJob(playerHR_count_controlled_job);
        //teams HR computation
       jc.addJob(teamHR_count_controlled_job);
        //players HR compare with teams HR
        jc.addJob(compare_join_controlled_job);

        //Teams.csv joins with TeamsFranchises.csv on franchiseid
        jc.addJob(team_join_controlled_job);

        //Batting.csv joins with People.csv on playerid
        jc.addJob(player_join_controlled_job);

        Thread jcThread = new Thread(jc);
        jcThread.start();

        while(true){
            if (jc.allFinished()) {
                System.out.println(jc.getSuccessfulJobList());
                jc.stop();

                FileSystem fileSystem=FileSystem.get(conf);
                Path outputPath=new Path("/users/output");

                //If the file exists, delete it.
                if(fileSystem.exists(outputPath)){
                    fileSystem.delete(outputPath,true);
                }
                return;
            }
            if(jc.getFailedJobList().size() > 0){
                System.out.println(jc.getFailedJobList());
                jc.stop();
                return;
            }
        }


        //jc.run();



    }

}
