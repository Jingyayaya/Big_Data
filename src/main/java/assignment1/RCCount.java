package assignment1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class RCCount {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // distribute the lines with the same plyaerID into the same reducer
            String[] vals=value.toString().split(",");
            context.write(new Text(vals[0]), value);
        }
    }

    public static class RCReducer extends Reducer<Text,Text,Text, FloatWritable> {
        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {

            float h=0, bb=0, ibb=0,hbp=0, ab=0, sf=0, sh=0, b2=0, b3=0, hr=0;
            float OBP_1=0;
            float OBP_2=0;
            float TB=0;
            boolean flag=true;

            for (Text val : values) {
                String[] splitvalues=val.toString().split(",");
                try{
                    h += getValue(splitvalues,8);
                    bb += getValue(splitvalues,15);
                    ibb += getValue(splitvalues,17);
                    hbp += getValue(splitvalues,18);

                    ab += getValue(splitvalues,6);
                    sf += getValue(splitvalues,20);
                    sh += getValue(splitvalues,19);

                    b2 += getValue(splitvalues,9);
                    b3 += getValue(splitvalues,10);
                    hr += getValue(splitvalues,11);
                }
                catch(Exception e){
                    flag=false;
                    System.err.println(e.getStackTrace());
                }
            }
            OBP_1 = h + bb +ibb +hbp;
            OBP_2 = ab + bb + ibb + hbp + sf + sh;
            TB = h- (b2 + b3 + hr) + 2* b2 + 3 * b3 + 4* hr;

            //The first line in the csv file will be filtered.
            if(flag){
                float res=OBP_2 == 0? 0: TB* OBP_1  / OBP_2;
                context.write(key, new FloatWritable(res));
            }

        }

        private float getValue(String[] splitvalues, int index){
            //If the field is empty, return 0
            if(index > splitvalues.length-1 || splitvalues[index].isEmpty())
                return 0;

            return Float.parseFloat(splitvalues[index]);
        }
    }

    public static void main(String[] args) throws Exception {

        if(args.length != 3){
            System.out.println("The first parameter is the value of fs.defaultFS, like hdfs://localhost:9000");
            System.out.println("The second parameter is input address, like /data/Batting.csv");
            System.out.println("The third parameter is output address, like /data/output_RCCount");
            System.exit(0);
        }
        Configuration conf = new Configuration();

        //change the default file system by the first parameter.
        conf.set("fs.defaultFS",args[0]);

        Job job = Job.getInstance(conf, "RC count");

        job.setJarByClass(RCCount.class);
        job.setMapperClass(RCCount.TokenizerMapper.class);
        job.setReducerClass(RCCount.RCReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileSystem fileSystem=FileSystem.get(conf);

        //Set the output file path by the third parameter
        Path outputPath=new Path(args[2]);

        //If the file exists, delete it.
        if(fileSystem.exists(outputPath)){
            fileSystem.delete(outputPath,true);
        }
        // Set the input file path by the second parameter
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, outputPath);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
