package cs455.hadoop;

import cs455.utils.RawDataWritable;
import cs455.utils.ReportingWritable;
import cs455.utils.StateDataWritable;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;

/**
 * Created by eloza on 4/14/17.
 */
public class ReportingMapper extends Mapper <Text, Text, Text, StateDataWritable>{

    float[] rentVOwned = new float[2];
    float[] marriedVNmarried = new float[2];
    float[] hispanicAge = new float[6];
    float[] ruralVUrban = new float[2];
    String medianOwnValue = "";
    String medianRentContract ="";
    float aveRooms = 0;
    float percentElderly = 0;

    public void map(Text key, Text value, Mapper.Context context)
            throws IOException, InterruptedException {

        String state = key.toString();
        String line = value.toString();
        String[] stateValues = line.split(",");

        rentVOwned[0] = Float.parseFloat(stateValues[0]);
        rentVOwned[1] = Float.parseFloat(stateValues[1]);
        marriedVNmarried[0] = Float.parseFloat(stateValues[2]);
        marriedVNmarried[1] = Float.parseFloat(stateValues[3]);
        hispanicAge[0] = Float.parseFloat(stateValues[4]);
        hispanicAge[1] = Float.parseFloat(stateValues[5]);
        hispanicAge[2] = Float.parseFloat(stateValues[6]);
        hispanicAge[3] = Float.parseFloat(stateValues[7]);
        hispanicAge[4] = Float.parseFloat(stateValues[8]);
        hispanicAge[5] = Float.parseFloat(stateValues[9]);
        ruralVUrban[0] = Float.parseFloat(stateValues[10]);
        ruralVUrban[1] = Float.parseFloat(stateValues[11]);
        medianOwnValue = stateValues[12];
        medianRentContract = stateValues[13];
        aveRooms = Float.parseFloat(stateValues[14]);
        percentElderly = Float.parseFloat(stateValues[15]);

        StateDataWritable stateInfo = new StateDataWritable (rentVOwned, marriedVNmarried, hispanicAge, ruralVUrban,
                medianOwnValue, medianRentContract, aveRooms, percentElderly, state);


        context.write(new Text("USA"), stateInfo);



    }

}
