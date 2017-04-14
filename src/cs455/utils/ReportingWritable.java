package cs455.utils;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created by eloza on 4/14/17.
 */
public class ReportingWritable implements Writable{
    Map<String, StateDataWritable> statesData = new LinkedHashMap<>();
    float aveRooms95Perc = 0;
    String mostElderlyState = "NULL";


    public ReportingWritable(){}

    public ReportingWritable (Map<String, StateDataWritable> statesData, float aveRooms95Perc, String mostElderlyState) {
        this.statesData = statesData;
        this.aveRooms95Perc = aveRooms95Perc;
        this. mostElderlyState = mostElderlyState;

    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeFloat(aveRooms95Perc);
        WritableUtils.writeString(out, mostElderlyState);
        out.writeInt(statesData.size());
        for(Map.Entry<String, StateDataWritable> entry : statesData.entrySet()){
            WritableUtils.writeString(out, entry.getKey());
            entry.getValue().write(out);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        aveRooms95Perc = in.readFloat();
        mostElderlyState = WritableUtils.readString(in);
        int si = in.readInt();
        while(si > 0){
            float[] rentVOwned = new float[2];
            float[] marriedVNmarried = new float[2];
            float[] hispanicAge = new float[6];
            float[] ruralVUrban = new float[2];
            String medianOwnValue = "";
            String medianRentContract ="";
            float aveRooms = 0;
            float percentElderly = 0;

            String state = WritableUtils.readString(in);
            rentVOwned[0] = in.readFloat();
            rentVOwned[1] = in.readFloat();
            marriedVNmarried[0] = in.readFloat();
            marriedVNmarried[1] = in.readFloat();
            hispanicAge[0] = in.readFloat();
            hispanicAge[1] = in.readFloat();
            hispanicAge[2] = in.readFloat();
            hispanicAge[3] = in.readFloat();
            hispanicAge[4] = in.readFloat();
            hispanicAge[5] = in.readFloat();
            ruralVUrban[0] = in.readFloat();
            ruralVUrban[1] = in.readFloat();
            medianOwnValue = WritableUtils.readString(in);
            medianRentContract = WritableUtils.readString(in);
            aveRooms = in.readFloat();
            percentElderly = in.readFloat();
            state = WritableUtils.readString(in);
            StateDataWritable statD = new StateDataWritable(rentVOwned, marriedVNmarried, hispanicAge, ruralVUrban, medianOwnValue,
                    medianRentContract, aveRooms, percentElderly, state);
            statesData.put(state, statD);
            si--;
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append("95th Percentile of Average Number of Rooms in Houses: " + aveRooms95Perc + "\n");
        sb.append("\tState with Highest Number of Elderly People: " + mostElderlyState + "\n");

        for(Map.Entry<String, StateDataWritable> entry: statesData.entrySet()){
            sb.append("\t" + entry.getKey() + "\n");
            entry.getValue().toString(sb);
        }

        return sb.toString();


    }
}
