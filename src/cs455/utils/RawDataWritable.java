package cs455.utils;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created by eloza on 4/11/17.
 */
public class RawDataWritable implements Writable {

    //------ Variable Info
    public int population =0;
    public int popInsideUrban =0;
    public int popOutsideUrban=0;
    public int popMale=0;
    public int popFemale=0;
    public int ownerOccupied=0;
    public int renterOccupied=0;

    

    //------ Lists of Info
    public int[] maritalStatusMale = new int[4];
    public int[] maritalStatusFemale = new int[4];

    public int[] ageDemographics = new int[31];
    public int[] ageHispanicMale = new int[31];
    public int[] ageHispanicFemale = new int[31];

    public int[] rooms = new int[9];
    public int[] urbanVRural = new int[4];
    public int[] ownValue = new int[20];
    public int[] rentContract = new int[16];


    public RawDataWritable() {
        Arrays.fill(maritalStatusMale, 0);
        Arrays.fill(maritalStatusFemale, 0);
        Arrays.fill(ageDemographics, 0);
        Arrays.fill(ageHispanicMale, 0);
        Arrays.fill(ageHispanicFemale, 0);
        Arrays.fill(rooms, 0);
        Arrays.fill(urbanVRural, 0);
        Arrays.fill(ownValue, 0);
        Arrays.fill(rentContract, 0);
    }

    public RawDataWritable(int population, int popInsideUrban, int popOutsideUrban, int popMale, int popFemale,
                           int ownerOccupied, int renterOccupied,
                           int neverMarriedMale, int marriedMale, int separatedMale, int widowedMale,
                           int neverMarriedFemale, int marriedFemale, int separatedFemale, int widowedFemale,
                           int aUnder1, int a1and2, int a3and4, int a5, int a6, int a7to9, int a10and11, int a12and13,
                           int a14, int a15, int a16, int a17, int a18, int a19, int a20, int a21, int a22to24,
                           int a25to29, int a30to34, int a35to39, int a40to44, int a45to49, int a50to54, int a55to59,
                           int a60and61, int a62to64, int a65to69, int a70to74, int a75to79, int a80to84, int a85andUp,
                           int hMUnder1, int hM1and2, int hM3and4, int hM5, int hM6, int hM7to9, int hM10and11, int hM12and13,
                           int hM14, int hM15, int hM16, int hM17, int hM18, int hM19, int hM20, int hM21, int hM22to24,
                           int hM25to29, int hM30to34, int hM35to39, int hM40to44, int hM45to49, int hM50to54, int hM55to59,
                           int hM60and61, int hM62to64, int hM65to69, int hM70to74, int hM75to79, int hM80to84, int hM85andUp,
                           int hFUnder1, int hF1and2, int hF3and4, int hF5, int hF6, int hF7to9, int hF10and11, int hF12and13,
                           int hF14, int hF15, int hF16, int hF17, int hF18, int hF19, int hF20, int hF21, int hF22to24,
                           int hF25to29, int hF30to34, int hF35to39, int hF40to44, int hF45to49, int hF50to54, int hF55to59,
                           int hF60and61, int hF62to64, int hF65to69, int hF70to74, int hF75to79, int hF80to84, int hF85andUp,
                           int room1, int room2, int room3, int room4, int room5, int room6, int room7, int room8, int room9,
                           int insideUrban, int outsideUrban, int rural, int nDefined,
                           int ownLess15000, int own15000to19999, int own20000to24999, int own25000to29999, int own30000to34999,
                           int own35000to39999, int own40000to44999, int own45000to49999, int own50000to59999, int own60000to74999,
                           int own75000to99999, int own100000to124999, int own125000to149999, int own150000to174999,
                           int own175000to199999, int own200000to249999, int own250000to299999, int own300000to399999,
                           int own400000to499999, int own500000andUp,
                           int rentUnder100, int rent100to149, int rent150to199, int rent200to249, int rent250to299,
                           int rent300to349, int rent350to399, int rent400to449, int rent450to499, int rent500to549,
                           int rent550to599, int rent600to649, int rent650to699, int rent700to749, int rent750to999,
                           int rent1000andUp){

        this.population = population; this.popInsideUrban = popInsideUrban; this.popOutsideUrban = popOutsideUrban;
        this.popMale = popMale; this.popFemale = popFemale; this.ownerOccupied = ownerOccupied; this.renterOccupied = renterOccupied;

        maritalStatusMale[0] = neverMarriedMale; maritalStatusMale[1] = marriedMale;
        maritalStatusMale[2] = separatedMale; maritalStatusMale[3] = widowedMale;

        maritalStatusFemale[0] = neverMarriedFemale; maritalStatusFemale[1] = marriedFemale;
        maritalStatusFemale[2] = separatedFemale; maritalStatusFemale[3] = widowedFemale;

        ageDemographics[0] = aUnder1; ageDemographics[1] = a1and2; ageDemographics[2] = a3and4;
        ageDemographics[3] = a5; ageDemographics[4] = a6; ageDemographics[5] = a7to9; ageDemographics[6] = a10and11;
        ageDemographics[7] = a12and13; ageDemographics[8] = a14; ageDemographics[9] = a15;
        ageDemographics[10] = a16; ageDemographics[11] = a17; ageDemographics[12] = a18; ageDemographics[13] = a19;
        ageDemographics[14] = a20; ageDemographics[15] = a21; ageDemographics[16] = a22to24; ageDemographics[17] = a25to29;
        ageDemographics[18] = a30to34; ageDemographics[19] = a35to39; ageDemographics[20] = a40to44;
        ageDemographics[21] = a45to49; ageDemographics[22] = a50to54; ageDemographics[23] = a55to59;
        ageDemographics[24] = a60and61; ageDemographics[25] = a62to64; ageDemographics[26] = a65to69;
        ageDemographics[27] = a70to74; ageDemographics[28] = a75to79; ageDemographics[29] = a80to84;
        ageDemographics[30] = a85andUp;

        ageHispanicMale[0] = hMUnder1; ageHispanicMale[1] = hM1and2; ageHispanicMale[2] = hM3and4;
        ageHispanicMale[3] = hM5; ageHispanicMale[4] = hM6; ageHispanicMale[5] = hM7to9; ageHispanicMale[6] = hM10and11;
        ageHispanicMale[7] = hM12and13; ageHispanicMale[8] = hM14; ageHispanicMale[9] = hM15;
        ageHispanicMale[10] = hM16; ageHispanicMale[11] = hM17; ageHispanicMale[12] = hM18; ageHispanicMale[13] = hM19;
        ageHispanicMale[14] = hM20; ageHispanicMale[15] = hM21; ageHispanicMale[16] = hM22to24; ageHispanicMale[17] = hM25to29;
        ageHispanicMale[18] = hM30to34; ageHispanicMale[19] = hM35to39; ageHispanicMale[20] = hM40to44;
        ageHispanicMale[21] = hM45to49; ageHispanicMale[22] = hM50to54; ageHispanicMale[23] = hM55to59;
        ageHispanicMale[24] = hM60and61; ageHispanicMale[25] = hM62to64; ageHispanicMale[26] = hM65to69;
        ageHispanicMale[27] = hM70to74; ageHispanicMale[28] = hM75to79; ageHispanicMale[29] = hM80to84;
        ageHispanicMale[30] = hM85andUp;

        ageHispanicFemale[0] = hFUnder1; ageHispanicFemale[1] = hF1and2; ageHispanicFemale[2] = hF3and4;
        ageHispanicFemale[3] = hF5; ageHispanicFemale[4] = hF6; ageHispanicFemale[5] = hF7to9; ageHispanicFemale[6] = hF10and11;
        ageHispanicFemale[7] = hF12and13; ageHispanicFemale[8] = hF14; ageHispanicFemale[9] = hF15;
        ageHispanicFemale[10] = hF16; ageHispanicFemale[11] = hF17; ageHispanicFemale[12] = hF18; ageHispanicFemale[13] = hF19;
        ageHispanicFemale[14] = hF20; ageHispanicFemale[15] = hF21; ageHispanicFemale[16] = hF22to24; ageHispanicFemale[17] = hF25to29;
        ageHispanicFemale[18] = hF30to34; ageHispanicFemale[19] = hF35to39; ageHispanicFemale[20] = hF40to44;
        ageHispanicFemale[21] = hF45to49; ageHispanicFemale[22] = hF50to54; ageHispanicFemale[23] = hF55to59;
        ageHispanicFemale[24] = hF60and61; ageHispanicFemale[25] = hF62to64; ageHispanicFemale[26] = hF65to69;
        ageHispanicFemale[27] = hF70to74; ageHispanicFemale[28] = hF75to79; ageHispanicFemale[29] = hF80to84;
        ageHispanicFemale[30] = hF85andUp;
        


        rooms[0] = room1; rooms[1] = room2; rooms[2] = room3; rooms[3] = room4;
        rooms[4] = room5; rooms[5] = room6; rooms[6] = room7; rooms[7] = room8;
        rooms[8] = room9;

        urbanVRural[0] = insideUrban; urbanVRural[1] = outsideUrban;
        urbanVRural[2] = rural; urbanVRural[3] = nDefined;

        ownValue[0] = ownLess15000; ownValue[1] = own15000to19999;
        ownValue[2] = own20000to24999; ownValue[3] = own25000to29999;
        ownValue[4] = own30000to34999; ownValue[5] = own35000to39999;
        ownValue[6] =  own40000to44999; ownValue[7] =own45000to49999;
        ownValue[8] = own50000to59999; ownValue[9] = own60000to74999;
        ownValue[10] = own75000to99999; ownValue[11] = own100000to124999;
        ownValue[12] = own125000to149999; ownValue[13] = own150000to174999;
        ownValue[14] = own175000to199999; ownValue[15] = own200000to249999;
        ownValue[16] = own250000to299999; ownValue[17] = own300000to399999;
        ownValue[18] = own400000to499999; ownValue[19] = own500000andUp;

        rentContract[0] = rentUnder100; rentContract[1] = rent100to149;
        rentContract[2] = rent150to199; rentContract[3] = rent200to249;
        rentContract[4] = rent250to299; rentContract[5] = rent300to349;
        rentContract[6] = rent350to399; rentContract[7] = rent400to449;
        rentContract[8] = rent450to499; rentContract[9] = rent500to549;
        rentContract[10] = rent550to599; rentContract[11] = rent600to649;
        rentContract[12] = rent650to699; rentContract[13] = rent700to749;
        rentContract[14] = rent750to999; rentContract[15] = rent1000andUp;

    }




    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(population); out.writeInt(popInsideUrban); out.writeInt(popOutsideUrban);
        out.writeInt(popMale); out.writeInt(popFemale); out.writeInt(ownerOccupied); out.writeInt(renterOccupied);

        for(int x : maritalStatusMale){
            out.writeInt(x);
        }

        for(int x : maritalStatusFemale){
            out.writeInt(x);
        }

        for(int x : ageDemographics){
            out.writeInt(x);
        }

        for(int x : ageHispanicMale){
            out.writeInt(x);
        }

        for(int x : ageHispanicFemale){
            out.writeInt(x);
        }

        for(int x : rooms){
            out.writeInt(x);
        }

        for(int x : urbanVRural){
            out.writeInt(x);
        }

        for(int x: ownValue){
            out.writeInt(x);
        }

        for(int x: rentContract){
            out.writeInt(x);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        population = in.readInt(); popInsideUrban = in.readInt(); popOutsideUrban = in.readInt();
        popMale = in.readInt(); popFemale = in.readInt(); ownerOccupied = in.readInt(); renterOccupied = in.readInt();
        
        readMap(maritalStatusMale, in);
        readMap(maritalStatusFemale, in);
        readMap(ageDemographics, in);
        readMap(ageHispanicMale, in);
        readMap(ageHispanicFemale, in);
        readMap(rooms, in);
        readMap(urbanVRural, in);
        readMap(ownValue, in);
        readMap(rentContract, in);

    }

    private void readMap(int[] arr, DataInput in) throws IOException {
        for(int i = 0; i < arr.length; i++){
            arr[i] = in.readInt();
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append("Population:" + population + "\n");
        sb.append("Number of People Inside Urban: " + popInsideUrban + "\n");
        sb.append("Number of People Outside Urban: " + popOutsideUrban + "\n");
        sb.append("Number of Males: " + popMale + "\n");
        sb.append("Number of Females: " + popFemale + "\n");
        sb.append("Number of Houses Occupied by Owner: " + ownerOccupied + "\n");
        sb.append("Number of Houses Occupied by Renter: " + renterOccupied + "\n");

        sb.append("Male Marital Status:" + "\n");
        for(int x : maritalStatusMale){
            sb.append("\t" + x + "\n");
        }

        sb.append("Female Marital Status:" + "\n");
        for(int x : maritalStatusFemale){
            sb.append("\t" + x + "\n");
        }

        sb.append("Age Demographics:" + "\n");
        for(int x : ageDemographics){
            sb.append("\t" + x + "\n");
        }

        sb.append("Male Age Hispanic Demographics:" + "\n");
        for(int x : ageHispanicMale){
            sb.append("\t" + x + "\n");
        }

        sb.append("Female Age Hispanic Demographics:" + "\n");
        for(int x : ageHispanicFemale){
            sb.append("\t" + x + "\n");
        }

        sb.append("Rooms per House:" + "\n");
        for(int x : rooms){
            sb.append("\t" + x + "\n");
        }

        sb.append("Urban vs. Rural Housing Demographics:"  + "\n");
        for(int x : urbanVRural){
            sb.append("\t" + x + "\n");
        }

        sb.append("Value of Owned House:"  + "\n");
        for(int x: ownValue){
            sb.append("\t" + x + "\n");
        }

        sb.append("Rent Contract:" + "\n");
        for(int x: rentContract){
            sb.append("\t" + x + "\n");
        }

        return sb.toString();

    }

    public void merge(RawDataWritable other){
        this.population += other.population; this.popInsideUrban += other.popInsideUrban; this.popOutsideUrban += other.popOutsideUrban;
        this.popMale += other.popMale; this.popFemale += other.popFemale; this.ownerOccupied += other.ownerOccupied;
        this.renterOccupied += other.renterOccupied;
        

        for(int i = 0; i < maritalStatusMale.length; i++){
            maritalStatusMale[i] += other.maritalStatusMale[i];
        }

        for(int i = 0; i < maritalStatusFemale.length; i++){
            maritalStatusFemale[i] += other.maritalStatusFemale[i];
        }

        for(int i = 0; i < ageDemographics.length; i++){
            ageDemographics[i] += other.ageDemographics[i];
        }

        for(int i = 0; i < ageHispanicMale.length; i++){
            ageHispanicMale[i] += other.ageHispanicMale[i];
        }

        for(int i = 0; i < ageHispanicFemale.length; i++){
            ageHispanicFemale[i] += other.ageHispanicFemale[i];
        }

        for(int i = 0; i < rooms.length; i++){
            rooms[i] += other.rooms[i];
        }

        for(int i = 0; i < urbanVRural.length; i++){
            urbanVRural[i] += other.rooms[i];
        }

        for(int i = 0; i < ownValue.length; i++){
            ownValue[i] += other.ownValue[i];
        }

        for(int i = 0; i < rentContract.length; i++){
            rentContract[i] += other.rentContract[i];
        }
    }


}
