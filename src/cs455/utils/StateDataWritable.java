package cs455.utils;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by eloza on 4/13/17.
 */
public class StateDataWritable implements Writable {

    public float[] rentVOwned = new float[2];
    public float[] marriedVNmarried = new float[2];
    public float[] hispanicAge = new float[6];
    public float[] ruralVUrban = new float[2];
    public String medianOwnValue = "";
    public String medianRentContract ="";
    public float aveRooms = 0;
    public float percentElderly = 0;
    public String state = "";

    public StateDataWritable(){}

    public StateDataWritable (float[] rentVOwned, float[] marriedVNmarried, float[] hispanicAge, float[] ruralVUrban,
                              String medianOwnValue, String medianRentContract, float aveRooms, float percentElderly,
                              String state) {

        this.rentVOwned[0] = rentVOwned[0]; this.rentVOwned[1] = rentVOwned[1];

        this. marriedVNmarried[0] = marriedVNmarried[0]; this.marriedVNmarried[1] = marriedVNmarried[1];

        this.hispanicAge[0] = hispanicAge[0]; this.hispanicAge[1] = hispanicAge[1]; this.hispanicAge[2] = hispanicAge[2];
        this.hispanicAge[3] = hispanicAge[3]; this.hispanicAge[4] = hispanicAge[4]; this.hispanicAge[5] = hispanicAge[5];

        this.ruralVUrban[0] = ruralVUrban[0]; this.ruralVUrban[1] = ruralVUrban[1];

        this.medianOwnValue = medianOwnValue;
        this.medianRentContract = medianRentContract;
        this.aveRooms = aveRooms;
        this.percentElderly = percentElderly;
        this.state = state;

    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeFloat(rentVOwned[0]);
        out.writeFloat(rentVOwned[1]);
        out.writeFloat(marriedVNmarried[0]);
        out.writeFloat(marriedVNmarried[1]);
        out.writeFloat(hispanicAge[0]);
        out.writeFloat(hispanicAge[1]);
        out.writeFloat(hispanicAge[2]);
        out.writeFloat(hispanicAge[3]);
        out.writeFloat(hispanicAge[4]);
        out.writeFloat(hispanicAge[5]);
        out.writeFloat(ruralVUrban[0]);
        out.writeFloat(ruralVUrban[1]);
        WritableUtils.writeString(out, medianOwnValue);
        WritableUtils.writeString(out, medianRentContract);
        out.writeFloat(aveRooms);
        out.writeFloat(percentElderly);
        WritableUtils.writeString(out, state);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
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
    }

    @Override
    public String toString() {
        String val = rentVOwned[0] + "," + rentVOwned[1] + "," + marriedVNmarried[0] + "," + marriedVNmarried[1] + "," +
                hispanicAge[0] + "," + hispanicAge[1] + "," +hispanicAge[2] + "," +hispanicAge[3] + "," +hispanicAge[4] + "," +hispanicAge[5] + "," +
                ruralVUrban[0] + "," + ruralVUrban[1] + "," + medianOwnValue + "," + medianRentContract + "," + aveRooms + "," +
                percentElderly + "," + state;
        return val;

    }

    public void toString(StringBuilder sb) {

        sb.append("\t\tState: " + state +"\n");
        sb.append("\t\tRent vs. Own Demographics:" + "\n");
        sb.append("\t\t\t" + "Percent Owner Occupied: " + rentVOwned[0] + "%\n");
        sb.append("\t\t\t" + "Percent Renter Occupied: " + rentVOwned[1] + "%\n");

        sb.append("\t\tNever Married Population Statistics:" + "\n");
        sb.append("\t\t\t" + "Male Never Married: " + marriedVNmarried[0] + "%\n");
        sb.append("\t\t\t" + "Female Never Married: " + marriedVNmarried[1] + "%\n");

        sb.append("\t\tHispanic Age Demographics:" + "\n");
        sb.append("\t\t\t" + "Male 18 and Under: " + hispanicAge[0] + "%\n");
        sb.append("\t\t\t" + "Male 19 to 29: " + hispanicAge[1] + "%\n");
        sb.append("\t\t\t" + "Male 30 to 39: " + hispanicAge[2] + "%\n");
        sb.append("\t\t\t" + "Female 18 and Under: " + hispanicAge[3] + "%\n");
        sb.append("\t\t\t" + "Female 19 to 29: " + hispanicAge[4] + "%\n");
        sb.append("\t\t\t" + "Female 30 to 39: " + hispanicAge[5] + "%\n");

        sb.append("\t\tUrban vs Rural Demographics:" + "\n");
        sb.append("\t\t\t" + "Percent Urban Households: " + ruralVUrban[0] + "%\n");
        sb.append("\t\t\t" + "Percent Rural Households: " + ruralVUrban[1] + "%\n");

        sb.append("\t\tMedian Value of House Occupied by Owner: " + medianOwnValue + "\n");
        sb.append("\t\t\tMedian Contract of House Occupied by Renter: " + medianRentContract + "\n");
        sb.append("\t\t\tAverage Number of Rooms per House: " + aveRooms + "\n");
        sb.append("\t\t\tPercent Elderly People: " + percentElderly + "%\n");
    }
}
