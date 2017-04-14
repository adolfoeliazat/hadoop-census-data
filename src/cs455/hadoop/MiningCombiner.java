package cs455.hadoop;

import cs455.utils.RawDataWritable;
import cs455.utils.StateDataWritable;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

import cs455.utils.RawDataWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * Created by eloza on 4/14/17.
 */
public class MiningCombiner extends Reducer <Text, RawDataWritable, Text, RawDataWritable> {
    //------ Variable Info
    private int population = 0;
    private int popInsideUrban = 0;
    private int popOutsideUrban = 0;
    private int popMale = 0;
    private int popFemale = 0;
    private int ownerOccupied = 0;
    private int renterOccupied = 0;

    //-------Initialized List Values
    private int neverMarriedMale = 0;
    private int marriedMale = 0;
    private int separatedMale = 0;
    private int widowedMale = 0;
    private int neverMarriedFemale = 0;
    private int marriedFemale = 0;
    private int separatedFemale = 0;
    private int widowedFemale = 0;

    private int aUnder1 = 0;
    private int a1and2 = 0;
    private int a3and4 = 0;
    private int a5 = 0;
    private int a6 = 0;
    private int a7to9 = 0;
    private int a10and11 = 0;
    private int a12and13 = 0;
    private int a14 = 0;
    private int a15 = 0;
    private int a16 = 0;
    private int a17 = 0;
    private int a18 = 0;
    private int a19 = 0;
    private int a20 = 0;
    private int a21 = 0;
    private int a22to24 = 0;
    private int a25to29 = 0;
    private int a30to34 = 0;
    private int a35to39 = 0;
    private int a40to44 = 0;
    private int a45to49 = 0;
    private int a50to54 = 0;
    private int a55to59 = 0;
    private int a60and61 = 0;
    private int a62to64 = 0;
    private int a65to69 = 0;
    private int a70to74 = 0;
    private int a75to79 = 0;
    private int a80to84 = 0;
    private int a85andUp = 0;

    private int hMUnder1 = 0;
    private int hM1and2 = 0;
    private int hM3and4 = 0;
    private int hM5 = 0;
    private int hM6 = 0;
    private int hM7to9 = 0;
    private int hM10and11 = 0;
    private int hM12and13 = 0;
    private int hM14 = 0;
    private int hM15 = 0;
    private int hM16 = 0;
    private int hM17 = 0;
    private int hM18 = 0;
    private int hM19 = 0;
    private int hM20 = 0;
    private int hM21 = 0;
    private int hM22to24 = 0;
    private int hM25to29 = 0;
    private int hM30to34 = 0;
    private int hM35to39 = 0;
    private int hM40to44 = 0;
    private int hM45to49 = 0;
    private int hM50to54 = 0;
    private int hM55to59 = 0;
    private int hM60and61 = 0;
    private int hM62to64 = 0;
    private int hM65to69 = 0;
    private int hM70to74 = 0;
    private int hM75to79 = 0;
    private int hM80to84 = 0;
    private int hM85andUp = 0;

    private int hFUnder1 = 0;
    private int hF1and2 = 0;
    private int hF3and4 = 0;
    private int hF5 = 0;
    private int hF6 = 0;
    private int hF7to9 = 0;
    private int hF10and11 = 0;
    private int hF12and13 = 0;
    private int hF14 = 0;
    private int hF15 = 0;
    private int hF16 = 0;
    private int hF17 = 0;
    private int hF18 = 0;
    private int hF19 = 0;
    private int hF20 = 0;
    private int hF21 = 0;
    private int hF22to24 = 0;
    private int hF25to29 = 0;
    private int hF30to34 = 0;
    private int hF35to39 = 0;
    private int hF40to44 = 0;
    private int hF45to49 = 0;
    private int hF50to54 = 0;
    private int hF55to59 = 0;
    private int hF60and61 = 0;
    private int hF62to64 = 0;
    private int hF65to69 = 0;
    private int hF70to74 = 0;
    private int hF75to79 = 0;
    private int hF80to84 = 0;
    private int hF85andUp = 0;

    private int room1 = 0;
    private int room2 = 0;
    private int room3 = 0;
    private int room4 = 0;
    private int room5 = 0;
    private int room6 = 0;
    private int room7 = 0;
    private int room8 = 0;
    private int room9 = 0;

    private int insideUrban = 0;
    private int outsideUrban = 0;
    private int rural = 0;
    private int nDefined = 0;

    private int ownLess15000 = 0;
    private int own15000to19999 = 0;
    private int own20000to24999 = 0;
    private int own25000to29999 = 0;
    private int own30000to34999 = 0;
    private int own35000to39999 = 0;
    private int own40000to44999 = 0;
    private int own45000to49999 = 0;
    private int own50000to59999 = 0;
    private int own60000to74999 = 0;
    private int own75000to99999 = 0;
    private int own100000to124999 = 0;
    private int own125000to149999 = 0;
    private int own150000to174999 = 0;
    private int own175000to199999 = 0;
    private int own200000to249999 = 0;
    private int own250000to299999 = 0;
    private int own300000to399999 = 0;
    private int own400000to499999 = 0;
    private int own500000andUp = 0;

    private int rentUnder100 = 0;
    private int rent100to149 = 0;
    private int rent150to199 = 0;
    private int rent200to249 = 0;
    private int rent250to299 = 0;
    private int rent300to349 = 0;
    private int rent350to399 = 0;
    private int rent400to449 = 0;
    private int rent450to499 = 0;
    private int rent500to549 = 0;
    private int rent550to599 = 0;
    private int rent600to649 = 0;
    private int rent650to699 = 0;
    private int rent700to749 = 0;
    private int rent750to999 = 0;
    private int rent1000andUp = 0;

    RawDataWritable stats = new RawDataWritable(population, popInsideUrban, popOutsideUrban, popMale, popFemale,
            ownerOccupied, renterOccupied,
            neverMarriedMale, marriedMale, separatedMale, widowedMale,
            neverMarriedFemale, marriedFemale, separatedFemale, widowedFemale,
            aUnder1, a1and2, a3and4, a5, a6, a7to9, a10and11, a12and13,
            a14, a15, a16, a17, a18, a19, a20, a21, a22to24,
            a25to29, a30to34, a35to39, a40to44, a45to49, a50to54, a55to59,
            a60and61, a62to64, a65to69, a70to74, a75to79, a80to84, a85andUp,
            hMUnder1, hM1and2, hM3and4, hM5, hM6, hM7to9, hM10and11, hM12and13,
            hM14, hM15, hM16, hM17, hM18, hM19, hM20, hM21, hM22to24,
            hM25to29, hM30to34, hM35to39, hM40to44, hM45to49, hM50to54, hM55to59,
            hM60and61, hM62to64, hM65to69, hM70to74, hM75to79, hM80to84, hM85andUp,
            hFUnder1, hF1and2, hF3and4, hF5, hF6, hF7to9, hF10and11, hF12and13,
            hF14, hF15, hF16, hF17, hF18, hF19, hF20, hF21, hF22to24,
            hF25to29, hF30to34, hF35to39, hF40to44, hF45to49, hF50to54, hF55to59,
            hF60and61, hF62to64, hF65to69, hF70to74, hF75to79, hF80to84, hF85andUp,
            room1, room2, room3, room4, room5, room6, room7, room8, room9,
            insideUrban, outsideUrban, rural, nDefined,
            ownLess15000, own15000to19999, own20000to24999, own25000to29999, own30000to34999,
            own35000to39999, own40000to44999, own45000to49999, own50000to59999, own60000to74999,
            own75000to99999, own100000to124999, own125000to149999, own150000to174999,
            own175000to199999, own200000to249999, own250000to299999, own300000to399999,
            own400000to499999, own500000andUp,
            rentUnder100, rent100to149, rent150to199, rent200to249, rent250to299,
            rent300to349, rent350to399, rent400to449, rent450to499, rent500to549,
            rent550to599, rent600to649, rent650to699, rent700to749, rent750to999,
            rent1000andUp);

    public void reduce(Text key, Iterable<RawDataWritable> values, Context context)
            throws IOException, InterruptedException {

        for (RawDataWritable value : values) {
            stats.merge(value);
        }
        context.write(key, stats);
    }
}
