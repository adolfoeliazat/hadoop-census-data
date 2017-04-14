package cs455.hadoop;

import cs455.utils.ReportingWritable;
import cs455.utils.StateDataWritable;
import cs455.utils.StateWritable;

import cs455.utils.RawDataWritable;
import cs455.utils.StateDataWritable;
import org.apache.hadoop.fs.Stat;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;
import java.util.*;

/**
 * Created by eloza on 4/14/17.
 */
public class ReportingReducer extends Reducer <Text, StateDataWritable, Text, ReportingWritable>{

    Map<String, StateDataWritable> statesData = new LinkedHashMap<>();
    ArrayList<StateDataWritable> sDArr = new ArrayList<>();
    float aveRooms95Perc = 0;
    String mostElderlyState = "NULL";

//    public void reduce (Text key, Iterable<StateDataWritable> values, Context context) throws IOException, InterruptedException{
//        float maxValue = Float.MIN_VALUE;
//        List<Float> aveRooms = new ArrayList<Float>();
//
//        for(StateDataWritable stateValue : values){
//            System.out.println(stateValue.toString());
//            String hashKey = stateValue.state;
//            StateDataWritable tempThing = new StateDataWritable(stateValue.rentVOwned, stateValue.marriedVNmarried, stateValue.hispanicAge, stateValue.ruralVUrban,
//                    stateValue.medianOwnValue, stateValue.medianRentContract, stateValue.aveRooms, stateValue.percentElderly, stateValue.state);
//            //statesData.putIfAbsent(hashKey, stateValue);
//            sDArr.add(tempThing);
//        }
//        for(Map.Entry<String, StateDataWritable> entry : statesData.entrySet()){
//            System.out.println("The Key is: " + entry.getKey() + "\t The Value is: " + entry.getValue());
//        }
//
//        for(StateDataWritable entry:sDArr){
//            System.out.println(entry);
//        }
//
//        ReportingWritable answ = new ReportingWritable (statesData, aveRooms95Perc, mostElderlyState);
//        context.write(new Text("USA"), answ);
//    }

    public void reduce (Text key, Iterable<StateDataWritable> values, Context context)
            throws IOException, InterruptedException{

        float maxValue = Float.MIN_VALUE;
        List<Float> aveRooms = new ArrayList<Float>();

        for (StateDataWritable value : values) {
            String kk = value.state;
            StateDataWritable tempThing = new StateDataWritable(value.rentVOwned, value.marriedVNmarried, value.hispanicAge, value.ruralVUrban,
                    value.medianOwnValue, value.medianRentContract, value.aveRooms, value.percentElderly, value.state);

            statesData.put(kk, tempThing);
            if (value.percentElderly > maxValue){
                maxValue = value.percentElderly;
                mostElderlyState = kk;
            }
            aveRooms.add(value.aveRooms);
        }


        Collections.sort(aveRooms);
        int roomIndex = (int) Math.ceil((double) aveRooms.size() * (0.95));
        aveRooms95Perc = aveRooms.get(roomIndex - 1);

        ReportingWritable answ = new ReportingWritable (statesData, aveRooms95Perc, mostElderlyState);
        context.write(new Text("USA"), answ);


    }



}
