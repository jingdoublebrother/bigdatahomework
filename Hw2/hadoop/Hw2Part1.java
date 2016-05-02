/*
 * File: Hw2Part1.java
 * Type: java
 * Author: liqing(liqingdoublebrother@gmail.com)
 * Modified: 2016-05-02
 */
import java.io.IOException;
import java.lang.Float;
import java.util.StringTokenizer;
import java.text.DecimalFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * A class to get statistic average values with mapreduce API.
 * And the input text files have format like following:
 *     source_1 destination_1 value_1
 *     source_2 destination_2 value_2
 *     ......
 * Then outputs the result like following:
 *     source_1 destination_1 count_1 average_1
 *     source_2 destination_2 count_2 average_2
 *     ......
 * And a count is the count of the occurrence source-destination pair,
 * a average is the average value.
 */
public class Hw2Part1
{
     public static class SDTMapper
             extends Mapper<Object, Text, Text, DoubleWritable>
         {
             private Text sd = new Text();
             private DoubleWritable time = new DoubleWritable();

             public void map( Object key, Text value, Context context )
                 throws IOException, InterruptedException
             {

                 // StringTokenizer splits a line to separate parts saved in tokens.
                 StringTokenizer tokens = new StringTokenizer( value.toString() );

                 if( tokens.countTokens() == 3 )
                 {
                    sd.set( tokens.nextToken() + " " + tokens.nextToken() );
                    time.set( new Double(tokens.nextToken()) );

                    context.write( sd, time );
                 }
             }
         }

     public static class CountAndAverageReducer
             extends Reducer<Text, DoubleWritable, Text, Text>
         {
              public Text result = new Text();

              public void reduce( Text key, Iterable<DoubleWritable> values, Context context )
                  throws IOException, InterruptedException
              {
                  int count = 0;
                  double average = 0;
                  for( DoubleWritable t:values )
                  {
                      count++;
                      average += t.get();
                  }
                  average /=(double)count;

                  // To get a decimal format with 3 bits after the point.
                  DecimalFormat df = new DecimalFormat("0.000");

                  result.set( String.valueOf(count) + " " + df.format( average ) );

                  context.write( key, result );
              }
         }

     public static void main( String[] args )throws Exception
     {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2)
        {
          System.err.println("Usage: Hw2Part1 <in> [<in>...] <out>");
          System.exit(2);
        }

        Job job = Job.getInstance( conf, "count and average" );
        job.setJarByClass(Hw2Part1.class);

        job.setMapperClass( SDTMapper.class );
        job.setReducerClass( CountAndAverageReducer.class );

        job.setMapOutputKeyClass( Text.class );
        job.setMapOutputValueClass( DoubleWritable.class );

        job.setOutputKeyClass( Text.class );
        job.setOutputValueClass( Text.class );

        for( int i=0; i<otherArgs.length-1; i++ )
            FileInputFormat.addInputPath( job, new Path( otherArgs[i] ) );
        FileOutputFormat.setOutputPath( job, new Path( otherArgs[ otherArgs.length-1 ] ) );

        System.exit( job.waitForCompletion(true) ? 0 : 1 );
     }

}
