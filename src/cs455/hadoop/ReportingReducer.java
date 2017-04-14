package cs455.hadoop;

import cs455.utils.ReportingWritable;
import cs455.utils.StateDataWritable;
import cs455.utils.StateWritable;

import cs455.utils.RawDataWritable;
import cs455.utils.StateDataWritable;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;
import java.util.*;

/**
 * Created by eloza on 4/14/17.
 */
public class ReportingReducer extends Reducer <Text, StateDataWritable, Text, ReportingWritable>{

    Map<String, StateDataWritable> statesData = new LinkedHashMap<>();
    float aveRooms95Perc = 0;
    String mostElderlyState = "NULL";

    public void reduce (Text key, Iterable<StateDataWritable> values, Context context)
            throws IOException, InterruptedException{

        float maxValue = Float.MIN_VALUE;
        List<Float> aveRooms = new ArrayList<Float>();

        for (StateDataWritable value : values) {
            System.out.println("THE VALUEIS:==================:");
            System.out.println(value.toString());
            String kk = value.state;
            statesData.put(kk, value);
            if (value.percentElderly > maxValue){
                maxValue = value.percentElderly;
                mostElderlyState = kk;
            }
            aveRooms.add(value.aveRooms);
        }

        Collections.sort(aveRooms);
        int roomIndex = (int) Math.ceil((double) aveRooms.size() * (0.95));
        System.out.println("ROOM INDEX: " +roomIndex);
        System.out.println("AVE ROOM SIZE: " + aveRooms.size());
        aveRooms95Perc = aveRooms.get(roomIndex - 1);

        ReportingWritable answ = new ReportingWritable (statesData, aveRooms95Perc, mostElderlyState);
        context.write(new Text("USA"), answ);


    }



}
