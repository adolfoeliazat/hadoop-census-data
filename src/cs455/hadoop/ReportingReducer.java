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

    public void reduce (Text key, Iterable<StateDataWritable> values, Context context) throws IOException, InterruptedException{
        float maxValue = Float.MIN_VALUE;
        List<Float> aveRooms = new ArrayList<Float>();

        for(StateDataWritable stateValue : values){
            System.out.println(stateValue.toString());
            String hashKey = stateValue.state;
            StateDataWritable tempThing = new StateDataWritable(stateValue.rentVOwned, stateValue.marriedVNmarried, stateValue.hispanicAge, stateValue.ruralVUrban,
                    stateValue.medianOwnValue, stateValue.medianRentContract, stateValue.aveRooms, stateValue.percentElderly, stateValue.state);
            //statesData.putIfAbsent(hashKey, stateValue);
            sDArr.add(tempThing);
        }
        for(Map.Entry<String, StateDataWritable> entry : statesData.entrySet()){
            System.out.println("The Key is: " + entry.getKey() + "\t The Value is: " + entry.getValue());
        }

        for(StateDataWritable entry:sDArr){
            System.out.println(entry);
        }

        ReportingWritable answ = new ReportingWritable (statesData, aveRooms95Perc, mostElderlyState);
        context.write(new Text("USA"), answ);
    }

//    public void reduce (Text key, Iterable<StateDataWritable> values, Context context)
//            throws IOException, InterruptedException{
//
//        float maxValue = Float.MIN_VALUE;
//        List<Float> aveRooms = new ArrayList<Float>();
//
//        for (StateDataWritable value : values) {
//            System.out.println("THE VALUEIS:==================:");
//            System.out.println(value.toString());
//            String kk = value.state;
//            System.out.println(kk);
//            System.out.println(value.toString());
//            System.out.println("Before");
//            StringBuilder sb2 = new StringBuilder();
//            for (Map.Entry<String,StateDataWritable> entry : statesData.entrySet()){
//                sb2.append("The Key is: " + entry.getKey() + "\n");
//                sb2.append("The State is: " + entry.getValue().state + "\n");
//            }
//            System.out.println(sb2.toString());
//
//            statesData.put(kk, value);
//            StringBuilder sb1 = new StringBuilder();
//            for (Map.Entry<String,StateDataWritable> entry : statesData.entrySet()){
//                sb1.append("The Key is: " + entry.getKey() + "\n");
//                sb1.append("The State is: " + entry.getValue().state + "\n");
//            }
//            System.out.println(sb1.toString());
//            if (value.percentElderly > maxValue){
//                maxValue = value.percentElderly;
//                mostElderlyState = kk;
//            }
//            aveRooms.add(value.aveRooms);
//            System.out.println(statesData.get(kk).toString());
//        }
//
//        System.out.println("WE ARE GETTIN To the bootm===========");
//        StringBuilder sb = new StringBuilder();
//        for (Map.Entry<String,StateDataWritable> entry : statesData.entrySet()){
//            sb.append("The Key is: " + entry.getKey() + "\n");
//            sb.append("The State is: " + entry.getValue().state + "\n");
//        }
//        System.out.println(sb.toString());
//
//        Collections.sort(aveRooms);
//        int roomIndex = (int) Math.ceil((double) aveRooms.size() * (0.95));
//        System.out.println("ROOM INDEX: " +roomIndex);
//        System.out.println("AVE ROOM SIZE: " + aveRooms.size());
//        aveRooms95Perc = aveRooms.get(roomIndex - 1);
//
//        ReportingWritable answ = new ReportingWritable (statesData, aveRooms95Perc, mostElderlyState);
//        context.write(new Text("USA"), answ);
//
//
//    }



}
