import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;

public class SQLCount {

    public static class City_Mapper
            extends Mapper<LongWritable, Text, Text, Text> {

        // private final static IntWritable one = new IntWritable(1);
        //private Text word = new Text();

        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {
            //StringTokenizer itr = new StringTokenizer(value.toString());
            String[] city_rec = value.toString().split("\t");
            if(Integer.parseInt(city_rec[4])>=1000000) {
                System.out.println(city_rec[2] + " : " + city_rec[4]);
                context.write(new Text(city_rec[2]), new Text("city," + city_rec[4]));
            }
        }
    }


    public static class Country_Mapper extends Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] country_rec = value.toString().split("\t");
            System.out.println(country_rec[0]+","+country_rec[1]);

            context.write(new Text(country_rec[0]), new Text("country,"+country_rec[1]));

        }
    }

    public static class Main_Reducer
            extends Reducer<Text,Text,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int counter = 0;
            Text country_name=new Text();
            for (Text val : values) {
                System.out.println(key+":-"+val.toString());

                String[] data = val.toString().split(",");
                if (data[0].equals("country"))
                    country_name.set(data[1]);
                else
                    counter++;
            }
            if (counter>=3) {
                result.set(counter);
                context.write(country_name, result);
            }

        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 3) {
            System.err.println("Usage: sqlcount <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "SQL count");
        job.setJarByClass(SQLCount.class);
//        job.setMapperClass(City_Mapper.class);
//        job.setMapperClass(Country_Mapper.class);
        //job.setCombinerClass(Main_Reducer.class);
        job.setReducerClass(Main_Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        MultipleInputs.addInputPath(job,new Path(otherArgs[0]), TextInputFormat.class,City_Mapper.class);
        MultipleInputs.addInputPath(job,new Path(otherArgs[1]),TextInputFormat.class,Country_Mapper.class);
        job.setOutputValueClass(IntWritable.class);
        //for (int i = 0; i < otherArgs.length - 1; ++i) {
        // FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        //System.out.println(otherArgs[i]);
        // }
        FileOutputFormat.setOutputPath(job,
                new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

