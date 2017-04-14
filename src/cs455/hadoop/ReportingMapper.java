package cs455.hadoop;

import cs455.utils.RawDataWritable;
import cs455.utils.ReportingWritable;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;

/**
 * Created by eloza on 4/14/17.
 */
public class ReportingMapper extends Mapper <Text, Text, Text, Text>{
    public void map(Text key, Text value, Mapper.Context context)
            throws IOException, InterruptedException {
        context.write(key, value);
//        String state = value.toString();
//        String line = value.toString();
//        String[] stateValues =



    }

}
