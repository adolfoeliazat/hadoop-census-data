package cs455.utils;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
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
    public LinkedHashMap<String, Integer> maritalStatusMale = new LinkedHashMap<>();
    public LinkedHashMap<String, Integer> maritalStatusFemale = new LinkedHashMap<>();

    public LinkedHashMap<String, Integer> ageDemographics = new LinkedHashMap<>();
    public LinkedHashMap<String, Integer> ageHispanicMale = new LinkedHashMap<>();
    public LinkedHashMap<String, Integer> ageHispanicFemale = new LinkedHashMap<>();

    public LinkedHashMap<String, Integer> rooms = new LinkedHashMap<>();
    public LinkedHashMap<String, Integer> urbanVRural = new LinkedHashMap<>();
    public LinkedHashMap<String, Integer> ownValue = new LinkedHashMap<>();
    public LinkedHashMap<String, Integer> rentContract = new LinkedHashMap<>();


    public RawDataWritable() {}

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

        maritalStatusMale.put("Never Married Male", neverMarriedMale); maritalStatusMale.put("Married Male", marriedMale);
        maritalStatusMale.put("Separated Male", separatedMale); maritalStatusMale.put("Widowed Male", widowedMale);

        maritalStatusFemale.put("Never Married Female", neverMarriedFemale); maritalStatusFemale.put("Married Female", marriedFemale);
        maritalStatusFemale.put("Separated Female", separatedFemale); maritalStatusFemale.put("Widowed Female", widowedFemale);

        ageDemographics.put("Under 1", aUnder1); ageDemographics.put("1 and 2", a1and2); ageDemographics.put("3 and 4", a3and4);
        ageDemographics.put("5", a5); ageDemographics.put("6", a6); ageDemographics.put("7 to 9", a7to9); ageDemographics.put("10 and 11", a10and11);
        ageDemographics.put("12 and 13", a12and13); ageDemographics.put("14", a14); ageDemographics.put("15", a15);
        ageDemographics.put("16", a16); ageDemographics.put("17", a17); ageDemographics.put("18", a18); ageDemographics.put("19", a19);
        ageDemographics.put("20", a20); ageDemographics.put("21", a21); ageDemographics.put("22 to 24", a22to24); ageDemographics.put("25 to 29", a25to29);
        ageDemographics.put("30 to 34", a30to34); ageDemographics.put("35 to 39", a35to39); ageDemographics.put("40 to 44", a40to44);
        ageDemographics.put("45 to 49", a45to49); ageDemographics.put("50 to 54", a50to54); ageDemographics.put("55 to 59", a55to59);
        ageDemographics.put("60 and 64", a60and61); ageDemographics.put("62 to 64", a62to64); ageDemographics.put("65 to 69", a65to69);
        ageDemographics.put("70 to 74", a70to74); ageDemographics.put("75 to 79", a75to79); ageDemographics.put("80 to 84", a80to84);
        ageDemographics.put("85 and Up", a85andUp);

        ageHispanicMale.put("Under 1", hMUnder1); ageHispanicMale.put("1 and 2", hM1and2); ageHispanicMale.put("3 and 4", hM3and4);
        ageHispanicMale.put("5", hM5); ageHispanicMale.put("6", hM6); ageHispanicMale.put("7 to 9", hM7to9); ageHispanicMale.put("10 and 11", hM10and11);
        ageHispanicMale.put("12 and 13", hM12and13); ageHispanicMale.put("14", hM14); ageHispanicMale.put("15", hM15);
        ageHispanicMale.put("16", hM16); ageHispanicMale.put("17", hM17); ageHispanicMale.put("18", hM18); ageHispanicMale.put("19", hM19);
        ageHispanicMale.put("20", hM20); ageHispanicMale.put("21", hM21); ageHispanicMale.put("22 to 24", hM22to24); ageHispanicMale.put("25 to 29", hM25to29);
        ageHispanicMale.put("30 to 34", hM30to34); ageHispanicMale.put("35 to 39", hM35to39); ageHispanicMale.put("40 to 44", hM40to44);
        ageHispanicMale.put("45 to 49", hM45to49); ageHispanicMale.put("50 to 54", hM50to54); ageHispanicMale.put("55 to 59", hM55to59);
        ageHispanicMale.put("60 and 64", hM60and61); ageHispanicMale.put("62 to 64", hM62to64); ageHispanicMale.put("65 to 69", hM65to69);
        ageHispanicMale.put("70 to 74", hM70to74); ageHispanicMale.put("75 to 79", hM75to79); ageHispanicMale.put("80 to 84", hM80to84);
        ageHispanicMale.put("85 and Up", hM85andUp);

        ageHispanicFemale.put("Under 1", hFUnder1); ageHispanicFemale.put("1 and 2", hF1and2); ageHispanicFemale.put("3 and 4", hF3and4);
        ageHispanicFemale.put("5", hF5); ageHispanicFemale.put("6", hF6); ageHispanicFemale.put("7 to 9", hF7to9); ageHispanicFemale.put("10 and 11", hF10and11);
        ageHispanicFemale.put("12 and 13", hF12and13); ageHispanicFemale.put("14", hF14); ageHispanicFemale.put("15", hF15);
        ageHispanicFemale.put("16", hF16); ageHispanicFemale.put("17", hF17); ageHispanicFemale.put("18", hF18); ageHispanicFemale.put("19", hF19);
        ageHispanicFemale.put("20", hF20); ageHispanicFemale.put("21", hF21); ageHispanicFemale.put("22 to 24", hF22to24); ageHispanicFemale.put("25 to 29", hF25to29);
        ageHispanicFemale.put("30 to 34", hF30to34); ageHispanicFemale.put("35 to 39", hF35to39); ageHispanicFemale.put("40 to 44", hF40to44);
        ageHispanicFemale.put("45 to 49", hF45to49); ageHispanicFemale.put("50 to 54", hF50to54); ageHispanicFemale.put("55 to 59", hF55to59);
        ageHispanicFemale.put("60 and 64", hF60and61); ageHispanicFemale.put("62 to 64", hF62to64); ageHispanicFemale.put("65 to 69", hF65to69);
        ageHispanicFemale.put("70 to 74", hF70to74); ageHispanicFemale.put("75 to 79", hF75to79); ageHispanicFemale.put("80 to 84", hF80to84);
        ageHispanicFemale.put("85 and Up", hF85andUp);

        rooms.put("1 Room", room1); rooms.put("2 Rooms", room2); rooms.put("3 Rooms", room3); rooms.put("4 Rooms", room4);
        rooms.put("5 Rooms", room5); rooms.put("6 Rooms", room6); rooms.put("7 Rooms", room7); rooms.put("8 Rooms", room8);
        rooms.put("9 Rooms", room9);

        urbanVRural.put("Inside Urban Area", insideUrban); urbanVRural.put("Outside Urban Area", outsideUrban);
        urbanVRural.put("Rural Area", rural); urbanVRural.put("Not Defined in File", nDefined);

        ownValue.put("Less than $15,000", ownLess15000); ownValue.put("$15,000 to $19,999", own15000to19999);
        ownValue.put("$20,000 to $24,999", own20000to24999); ownValue.put("$25,000 to $29,999", own25000to29999);
        ownValue.put("$30,000 to $34,999", own30000to34999); ownValue.put("35,0000 to $39,999", own35000to39999);
        ownValue.put("$40,000 to $44,999", own40000to44999); ownValue.put("$45,000 to $49,999", own45000to49999);
        ownValue.put("$50,000 to $59,999", own50000to59999); ownValue.put("$60,000 to $74,999", own60000to74999);
        ownValue.put("$75,000 to $99,999", own75000to99999); ownValue.put("$100,000 to $124,999", own100000to124999);
        ownValue.put("$125,000 to $149,999", own125000to149999); ownValue.put("$150,000 to $174,999", own150000to174999);
        ownValue.put("$175,000 to $199,999", own175000to199999); ownValue.put("$200,000 to $249,999", own200000to249999);
        ownValue.put("$250,000 to $299,999", own250000to299999); ownValue.put("$300,000 to $399,999", own300000to399999);
        ownValue.put("$400,000 to $499,999", own400000to499999); ownValue.put("$500,000 and Over", own500000andUp);

        rentContract.put("Under $100", rentUnder100); rentContract.put("$100 to $149", rent100to149);
        rentContract.put("$150 to $199", rent150to199); rentContract.put("$200 to $249", rent200to249);
        rentContract.put("$250 to $299", rent250to299); rentContract.put("$300 to $349", rent300to349);
        rentContract.put("$350 to $399", rent350to399); rentContract.put("$400 to $449", rent400to449);
        rentContract.put("$450 to $499", rent450to499); rentContract.put("$500 to $549", rent500to549);
        rentContract.put("$550 to $599", rent550to599); rentContract.put("$600 to $649", rent600to649);
        rentContract.put("$650 to $699", rent650to699); rentContract.put("$700 to $749", rent700to749);
        rentContract.put("$750 to $799", rent750to999); rentContract.put("$1,000 and Above", rent1000andUp);

    }




    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(population); out.writeInt(popInsideUrban); out.writeInt(popOutsideUrban);
        out.writeInt(popMale); out.writeInt(popFemale); out.writeInt(ownerOccupied); out.writeInt(renterOccupied);

        for(Map.Entry<String, Integer> entry : maritalStatusMale.entrySet()){
            WritableUtils.writeString(out, entry.getKey()); out.writeInt(entry.getValue());
        }

        for(Map.Entry<String, Integer> entry : maritalStatusFemale.entrySet()){
            WritableUtils.writeString(out, entry.getKey()); out.writeInt(entry.getValue());
        }

        for(Map.Entry<String, Integer> entry : ageDemographics.entrySet()){
            WritableUtils.writeString(out, entry.getKey()); out.writeInt(entry.getValue());
        }

        for(Map.Entry<String, Integer> entry : ageHispanicMale.entrySet()){
            WritableUtils.writeString(out, entry.getKey()); out.writeInt(entry.getValue());
        }

        for(Map.Entry<String, Integer> entry : ageHispanicFemale.entrySet()){
            WritableUtils.writeString(out, entry.getKey()); out.writeInt(entry.getValue());
        }

        for(Map.Entry<String, Integer> entry : rooms.entrySet()){
            WritableUtils.writeString(out, entry.getKey()); out.writeInt(entry.getValue());
        }

        for(Map.Entry<String, Integer> entry : urbanVRural.entrySet()){
            WritableUtils.writeString(out, entry.getKey()); out.writeInt(entry.getValue());
        }

        for(Map.Entry<String, Integer> entry : ownValue.entrySet()){
            WritableUtils.writeString(out, entry.getKey()); out.writeInt(entry.getValue());
        }

        for(Map.Entry<String, Integer> entry : rentContract.entrySet()){
            WritableUtils.writeString(out, entry.getKey()); out.writeInt(entry.getValue());
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        population = in.readInt(); popInsideUrban = in.readInt(); popOutsideUrban = in.readInt();
        popMale = in.readInt(); popFemale = in.readInt(); ownerOccupied = in.readInt(); renterOccupied = in.readInt();

        maritalStatusMale.clear(); maritalStatusFemale.clear(); ageDemographics.clear(); ageHispanicMale.clear();
        ageHispanicFemale.clear(); rooms.clear(); urbanVRural.clear(); ownValue.clear(); rentContract.clear();


        readMap(maritalStatusMale, 4, in);
        readMap(maritalStatusFemale, 4, in);
        readMap(ageDemographics, 31, in);
        readMap(ageHispanicMale, 31, in);
        readMap(ageHispanicFemale, 31, in);
        readMap(rooms, 9, in);
        readMap(urbanVRural, 4, in);
        readMap(ownValue, 20, in);
        readMap(rentContract, 16, in);

    }

    private void readMap(Map map, int iterations, DataInput in) throws IOException {
        int i = 0;

        while(i < iterations){
            String kk = WritableUtils.readString(in); Integer vv = in.readInt();
            maritalStatusMale.put(kk, vv);
            i++;
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
        for(Map.Entry<String, Integer> entry : maritalStatusMale.entrySet()){
            sb.append("\t" + entry.getKey() + ": " + entry.getValue() + "\n");
        }

        sb.append("Female Marital Status:" + "\n");
        for(Map.Entry<String, Integer> entry : maritalStatusFemale.entrySet()){
            sb.append("\t" + entry.getKey() + ": " + entry.getValue() + "\n");
        }

        sb.append("Age Demographics:" + "\n");
        for(Map.Entry<String, Integer> entry : ageDemographics.entrySet()){
            sb.append("\t" + entry.getKey() + ": " + entry.getValue() + "\n");
        }

        sb.append("Male Age Hispanic Demographics:" + "\n");
        for(Map.Entry<String, Integer> entry : ageHispanicMale.entrySet()){
            sb.append("\t" + entry.getKey() + ": " + entry.getValue() + "\n");
        }

        sb.append("Female Age Hispanic Demographics:" + "\n");
        for(Map.Entry<String, Integer> entry : ageHispanicFemale.entrySet()){
            sb.append("\t" + entry.getKey() + ": " + entry.getValue() + "\n");
        }

        sb.append("Rooms per House:" + "\n");
        for(Map.Entry<String, Integer> entry : rooms.entrySet()){
            sb.append("\t" + entry.getKey() + ": " + entry.getValue() + "\n");
        }

        sb.append("Urban vs. Rural Housing Demographics:"  + "\n");
        for(Map.Entry<String, Integer> entry : urbanVRural.entrySet()){
            sb.append("\t" + entry.getKey() + ": " + entry.getValue() + "\n");
        }

        sb.append("Value of Owned House:"  + "\n");
        for(Map.Entry<String, Integer> entry : ownValue.entrySet()){
            sb.append("\t" + entry.getKey() + ": " + entry.getValue() + "\n");
        }

        sb.append("Rent Contract:" + "\n");
        for(Map.Entry<String, Integer> entry : rentContract.entrySet()){
            sb.append("\t" + entry.getKey() + ": " + entry.getValue() + "\n");
        }

        return sb.toString();

    }

    public void merge(RawDataWritable other){
        this.population += other.population; this.popInsideUrban += other.popInsideUrban; this.popOutsideUrban += other.popOutsideUrban;
        this.popMale += other.popMale; this.popFemale += other.popFemale; this.ownerOccupied += other.ownerOccupied;
        this.renterOccupied += other.renterOccupied;

        for(Map.Entry<String, Integer> entry : maritalStatusMale.entrySet()){
            String key = entry.getKey();
            entry.setValue(entry.getValue() + other.maritalStatusMale.get(key));
        }

        for(Map.Entry<String, Integer> entry : maritalStatusFemale.entrySet()){
            String key = entry.getKey();
            entry.setValue(entry.getValue() + other.maritalStatusFemale.get(key));
        }

        for(Map.Entry<String, Integer> entry : ageDemographics.entrySet()){
            String key = entry.getKey();
            entry.setValue(entry.getValue() + other.ageDemographics.get(key));
        }

        for(Map.Entry<String, Integer> entry : ageHispanicMale.entrySet()){
            String key = entry.getKey();
            entry.setValue(entry.getValue() + other.ageHispanicMale.get(key));
        }

        for(Map.Entry<String, Integer> entry : ageHispanicFemale.entrySet()){
            String key = entry.getKey();
            entry.setValue(entry.getValue() + other.ageHispanicFemale.get(key));
        }

        for(Map.Entry<String, Integer> entry : rooms.entrySet()){
            String key = entry.getKey();
            entry.setValue(entry.getValue() + other.rooms.get(key));
        }

        for(Map.Entry<String, Integer> entry : urbanVRural.entrySet()){
            String key = entry.getKey();
            entry.setValue(entry.getValue() + other.rooms.get(key));
        }

        for(Map.Entry<String, Integer> entry : ownValue.entrySet()){
            String key = entry.getKey();
            entry.setValue(entry.getValue() + other.ownValue.get(key));
        }

        for(Map.Entry<String, Integer> entry : rentContract.entrySet()){
            String key = entry.getKey();
            entry.setValue(entry.getValue() + other.rentContract.get(key));
        }
    }


}
