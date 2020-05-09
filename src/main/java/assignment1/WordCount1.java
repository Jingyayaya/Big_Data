package assignment1;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount1 {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, Text>{

        //private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        public int convert_str_int(String myString) {
            int number;

            try {
                number = Integer.parseInt(myString);
            }
            catch(NumberFormatException e) {
                number = 0;
            }

            return number;
        }
        public String get_str(String[] tokens, int index) {
            String str = "0";

            try {
                str = tokens[index];
            }
            catch(ArrayIndexOutOfBoundsException e) {
            }

            return str;
        }

        public Double compute_OBP(String[] tokens) {

            int h = convert_str_int(get_str(tokens, 8));
            int bb = convert_str_int(get_str(tokens, 15));
            int ibb = convert_str_int(get_str(tokens, 17));
            int hbp = convert_str_int(get_str(tokens, 18));

            int numerator = h + bb + ibb + hbp;

            int ab = convert_str_int(get_str(tokens, 6));
            int sf = convert_str_int(get_str(tokens, 20));
            int sh = convert_str_int(get_str(tokens, 19));

            int denomenator = ab + bb + ibb + hbp + sf + sh;
            if(denomenator == 0)
                return 0.0;

            Double OBP = Double.POSITIVE_INFINITY;

            try {
                OBP = ((double) numerator / denomenator);
                if (OBP == Double.POSITIVE_INFINITY || OBP == Double.NEGATIVE_INFINITY)
                    throw new ArithmeticException();
            }

            catch(ArithmeticException e){
            }

            return OBP;
        }

        public int compute_TB(String[] tokens) {
            int h = convert_str_int(get_str(tokens, 8));
            int two_b = convert_str_int(get_str(tokens, 9));
            int three_b = convert_str_int(get_str(tokens, 10));
            int hr = convert_str_int(get_str(tokens, 11));

            int one_b = h - (two_b + three_b + hr);

            int TB = one_b + 2 * two_b + 3 * three_b + 4 * hr;

            return TB;
        }

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
   /*StringTokenizer itr = new StringTokenizer(value.toString());
   while (itr.hasMoreTokens()) {
     word.set(itr.nextToken());
     context.write(word, one);
   }*/

            String splitstring = value.toString();
            String[] tokens = splitstring.split(",");
            word.set(tokens[0]);

            //Double TBP = compute_OBP(tokens);
            //int TB = compute_TB(tokens);
            //Double RC = (TBP * TB);
            final IntWritable one = new IntWritable(1);
            //final DoubleWritable one = new DoubleWritable(RC);
            context.write(word, value);
        }
    }

    public static class IntSumReducer
            extends Reducer<Text,Text,Text,DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();
        public int convert_str_int(String myString) {
            int number;

            try {
                number = Integer.parseInt(myString);
            }
            catch(NumberFormatException e) {
                number = 0;
            }

            return number;
        }
        public String get_str(String[] tokens, int index) {
            String str = "0";

            try {
                str = tokens[index];
            }
            catch(ArrayIndexOutOfBoundsException e) {
            }

            return str;
        }
        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            double sum = 0.0;
            double h = 0;
            double bb = 0;
            double ibb = 0;
            double hbp = 0;
            double ab = 0;
            double sf = 0;
            double sh = 0;
            double two_b = 0;
            double three_b = 0;
            double hr = 0;
            for (Text val : values) {
                String splitstring = val.toString();
                String[] tokens = splitstring.split(",");

                h += convert_str_int(get_str(tokens, 8));
                bb += convert_str_int(get_str(tokens, 15));
                ibb += convert_str_int(get_str(tokens, 17));
                hbp += convert_str_int(get_str(tokens, 18));

                ab += convert_str_int(get_str(tokens, 6));
                sf += convert_str_int(get_str(tokens, 20));
                sh += convert_str_int(get_str(tokens, 19));

                two_b += convert_str_int(get_str(tokens, 9));
                three_b += convert_str_int(get_str(tokens, 10));
                hr += convert_str_int(get_str(tokens, 11));
            }

            double one_b = h - (two_b + three_b + hr);

            double TB = one_b + 2 * two_b + 3 * three_b + 4 * hr;


            double numerator = h + bb + ibb + hbp;

            double denomenator = ab + bb + ibb + hbp + sf + sh;
            Double OBP = Double.POSITIVE_INFINITY;
            if(denomenator == 0.0){
                OBP=0.0;
            }
            else{
                try {
                    OBP = ((double) numerator / denomenator);
                    if (OBP == Double.POSITIVE_INFINITY || OBP == Double.NEGATIVE_INFINITY)
                        throw new ArithmeticException();
                }

                catch(ArithmeticException e){
                }
            }

            result.set(TB*OBP);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount1.class);
        job.setMapperClass(TokenizerMapper.class);
        //job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}

