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


    public void reduce(Text key, Iterable<RawDataWritable> values, Context context)
            throws IOException, InterruptedException {
    	
        //------ Variable Info
        int population = 0;
        int popInsideUrban = 0;
        int popOutsideUrban = 0;
        int popMale = 0;
        int popFemale = 0;
        int ownerOccupied = 0;
        int renterOccupied = 0;

        //-------Initialized List Values
        int neverMarriedMale = 0;
        int marriedMale = 0;
        int separatedMale = 0;
        int widowedMale = 0;
        int neverMarriedFemale = 0;
        int marriedFemale = 0;
        int separatedFemale = 0;
        int widowedFemale = 0;

        int aUnder1 = 0;
        int a1and2 = 0;
        int a3and4 = 0;
        int a5 = 0;
        int a6 = 0;
        int a7to9 = 0;
        int a10and11 = 0;
        int a12and13 = 0;
        int a14 = 0;
        int a15 = 0;
        int a16 = 0;
        int a17 = 0;
        int a18 = 0;
        int a19 = 0;
        int a20 = 0;
        int a21 = 0;
        int a22to24 = 0;
        int a25to29 = 0;
        int a30to34 = 0;
        int a35to39 = 0;
        int a40to44 = 0;
        int a45to49 = 0;
        int a50to54 = 0;
        int a55to59 = 0;
        int a60and61 = 0;
        int a62to64 = 0;
        int a65to69 = 0;
        int a70to74 = 0;
        int a75to79 = 0;
        int a80to84 = 0;
        int a85andUp = 0;

        int hMUnder1 = 0;
        int hM1and2 = 0;
        int hM3and4 = 0;
        int hM5 = 0;
        int hM6 = 0;
        int hM7to9 = 0;
        int hM10and11 = 0;
        int hM12and13 = 0;
        int hM14 = 0;
        int hM15 = 0;
        int hM16 = 0;
        int hM17 = 0;
        int hM18 = 0;
        int hM19 = 0;
        int hM20 = 0;
        int hM21 = 0;
        int hM22to24 = 0;
        int hM25to29 = 0;
        int hM30to34 = 0;
        int hM35to39 = 0;
        int hM40to44 = 0;
        int hM45to49 = 0;
        int hM50to54 = 0;
        int hM55to59 = 0;
        int hM60and61 = 0;
        int hM62to64 = 0;
        int hM65to69 = 0;
        int hM70to74 = 0;
        int hM75to79 = 0;
        int hM80to84 = 0;
        int hM85andUp = 0;

        int hFUnder1 = 0;
        int hF1and2 = 0;
        int hF3and4 = 0;
        int hF5 = 0;
        int hF6 = 0;
        int hF7to9 = 0;
        int hF10and11 = 0;
        int hF12and13 = 0;
        int hF14 = 0;
        int hF15 = 0;
        int hF16 = 0;
        int hF17 = 0;
        int hF18 = 0;
        int hF19 = 0;
        int hF20 = 0;
        int hF21 = 0;
        int hF22to24 = 0;
        int hF25to29 = 0;
        int hF30to34 = 0;
        int hF35to39 = 0;
        int hF40to44 = 0;
        int hF45to49 = 0;
        int hF50to54 = 0;
        int hF55to59 = 0;
        int hF60and61 = 0;
        int hF62to64 = 0;
        int hF65to69 = 0;
        int hF70to74 = 0;
        int hF75to79 = 0;
        int hF80to84 = 0;
        int hF85andUp = 0;

        int room1 = 0;
        int room2 = 0;
        int room3 = 0;
        int room4 = 0;
        int room5 = 0;
        int room6 = 0;
        int room7 = 0;
        int room8 = 0;
        int room9 = 0;

        int insideUrban = 0;
        int outsideUrban = 0;
        int rural = 0;
        int nDefined = 0;

        int ownLess15000 = 0;
        int own15000to19999 = 0;
        int own20000to24999 = 0;
        int own25000to29999 = 0;
        int own30000to34999 = 0;
        int own35000to39999 = 0;
        int own40000to44999 = 0;
        int own45000to49999 = 0;
        int own50000to59999 = 0;
        int own60000to74999 = 0;
        int own75000to99999 = 0;
        int own100000to124999 = 0;
        int own125000to149999 = 0;
        int own150000to174999 = 0;
        int own175000to199999 = 0;
        int own200000to249999 = 0;
        int own250000to299999 = 0;
        int own300000to399999 = 0;
        int own400000to499999 = 0;
        int own500000andUp = 0;

        int rentUnder100 = 0;
        int rent100to149 = 0;
        int rent150to199 = 0;
        int rent200to249 = 0;
        int rent250to299 = 0;
        int rent300to349 = 0;
        int rent350to399 = 0;
        int rent400to449 = 0;
        int rent450to499 = 0;
        int rent500to549 = 0;
        int rent550to599 = 0;
        int rent600to649 = 0;
        int rent650to699 = 0;
        int rent700to749 = 0;
        int rent750to999 = 0;
        int rent1000andUp = 0;

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

        for (RawDataWritable value : values) {
            stats.merge(value);
        }
        context.write(key, stats);
    }
}
