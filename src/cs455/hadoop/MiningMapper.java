package cs455.hadoop;

import cs455.utils.RawDataWritable;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

import java.io.IOException;
import java.util.LinkedHashMap;

/**
 * Created by eloza on 4/12/17.
 */
public class MiningMapper extends Mapper <LongWritable, Text, Text, RawDataWritable> {

    //------ Variable Info
    private int population = 0;
    private int popInsideUrban = 0;
    private int popOutsideUrban = 0;
    private int popMale = 0;
    private int popFemale = 0 ;
    private int ownerOccupied = 0;
    private int renterOccupied = 0;


    //------ Lists of Info
    private LinkedHashMap<String, Integer> maritalStatusMale = new LinkedHashMap<>();
    private LinkedHashMap<String, Integer> maritalStatusFemale = new LinkedHashMap<>();

    private LinkedHashMap<String, Integer> ageDemographics = new LinkedHashMap<>();
    private LinkedHashMap<String, Integer> ageHispanicMale = new LinkedHashMap<>();
    private LinkedHashMap<String, Integer> ageHispanicFemale = new LinkedHashMap<>();

    private LinkedHashMap<String, Integer> rooms = new LinkedHashMap<>();
    private LinkedHashMap<String, Integer> urbanVRural = new LinkedHashMap<>();
    private LinkedHashMap<String, Integer> ownValue = new LinkedHashMap<>();
    
    //-------Initialized List Values
    private int neverMarriedMale = 0; private int marriedMale = 0; private int separatedMale = 0; private int widowedMale = 0;
    private int neverMarriedFemale = 0; private int marriedFemale = 0; private int separatedFemale = 0; private int widowedFemale = 0;
    
    private int aUnder1 = 0; private int a1and2 = 0; private int a3and4 = 0; private int a5 = 0; private int a6 = 0; 
    private int a7to9 = 0; private int a10and11 = 0; private int a12and13 = 0; private int a14 = 0; private int a15 = 0; 
    private int a16 = 0; private int a17 = 0; private int a18 = 0; private int a19 = 0; private int a20 = 0; private int a21 = 0; 
    private int a22to24 = 0; private int a25to29 = 0; private int a30to34 = 0; private int a35to39 = 0; private int a40to44 = 0; 
    private int a45to49 = 0; private int a50to54 = 0; private int a55to59 = 0; private int a60and61 = 0; private int a62to64 = 0; 
    private int a65to69 = 0; private int a70to74 = 0; private int a75to79 = 0; private int a80to84 = 0; private int a85andUp = 0;
    
    private int hMUnder1 = 0; private int hM1and2 = 0; private int hM3and4 = 0; private int hM5 = 0; private int hM6 = 0; 
    private int hM7to9 = 0; private int hM10and11 = 0; private int hM12and13 = 0; private int hM14 = 0; private int hM15 = 0;
    private int hM16 = 0; private int hM17 = 0; private int hM18 = 0; private int hM19 = 0; private int hM20 = 0; 
    private int hM21 = 0; private int hM22to24 = 0; private int hM25to29 = 0; private int hM30to34 = 0; private int hM35to39 = 0; 
    private int hM40to44 = 0; private int hM45to49 = 0; private int hM50to54 = 0; private int hM55to59 = 0; private int hM60and61 = 0; 
    private int hM62to64 = 0; private int hM65to69 = 0; private int hM70to74 = 0; private int hM75to79 = 0; private int hM80to84 = 0; 
    private int hM85andUp = 0;
    
    private int hFUnder1 = 0; private int hF1and2 = 0; private int hF3and4 = 0; private int hF5 = 0; private int hF6 = 0; 
    private int hF7to9 = 0; private int hF10and11 = 0; private int hF12and13 = 0; private int hF14 = 0; private int hF15 = 0; 
    private int hF16 = 0; private int hF17 = 0; private int hF18 = 0; private int hF19 = 0; private int hF20 = 0;
    private int hF21 = 0; private int hF22to24 = 0; private int hF25to29 = 0; private int hF30to34 = 0; private int hF35to39 = 0; 
    private int hF40to44 = 0; private int hF45to49 = 0; private int hF50to54 = 0; private int hF55to59 = 0; private int hF60and61 = 0; 
    private int hF62to64 = 0; private int hF65to69 = 0; private int hF70to74 = 0; private int hF75to79 = 0; private int hF80to84 = 0; 
    private int hF85andUp = 0;
    
    private int room1 = 0; private int room2 = 0; private int room3 = 0; private int room4 = 0; private int room5 = 0; 
    private int room6 = 0; private int room7 = 0; private int room8 = 0; private int room9 = 0;
    
    private int insideUrban = 0; private int outsideUrban = 0; private int rural = 0; private int nDefined = 0;
    
    private int ownLess15000 = 0; private int own15000to19999 = 0; private int own20000to24999 = 0; private int own25000to29999 = 0; 
    private int own30000to34999 = 0; private int own35000to39999 = 0; private int own40000to44999 = 0; private int own45000to49999 = 0; 
    private int own50000to59999 = 0; private int own60000to74999 = 0; private int own75000to99999 = 0; private int own100000to124999 = 0; 
    private int own125000to149999 = 0; private int own150000to174999 = 0; private int own175000to199999 = 0; 
    private int own200000to249999 = 0; private int own250000to299999 = 0; private int own300000to399999 = 0;
    private int own400000to499999 = 0; private int own500000andUp = 0;
    
    private int rentUnder100 = 0; private int rent100to149 = 0; private int rent150to199 = 0; private int rent200to249 = 0; 
    private int rent250to299 = 0; private int rent300to349 = 0; private int rent350to399 = 0; private int rent400to449 = 0; 
    private int rent450to499 = 0; private int rent500to549 = 0; private int rent550to599 = 0; private int rent600to649 = 0; 
    private int rent650to699 = 0; private int rent700to749 = 0; private int rent750to999 = 0;
    private int rent1000andUp = 0;

    public void map(LongWritable key, Text value, Mapper.Context context)
            throws IOException, InterruptedException {
        String line = value.toString();

        String state = line.substring((9-1),(11-1));
        String sumL = line.substring((11-1), (14-1));
        String logRegNum = line.substring((19-1), (25-1));
        String seg = line.substring((25-1), (29-1));
        String totSeg = line.substring((29-1), (33-1));

        int sumLevel = Integer.parseInt(sumL);
        int recordNumber = Integer.parseInt(logRegNum);
        int segment = Integer.parseInt(seg);
        int totalSegment = Integer.parseInt(totSeg);

        if (sumLevel == 100){
            if (segment == 1){
                population = Integer.parseInt(line.substring((301-1), (310-1))); popInsideUrban = Integer.parseInt(line.substring((328-1),(337-1)));
                popOutsideUrban = Integer.parseInt(line.substring((337 - 1 ), (346 - 1)));
                popMale = Integer.parseInt(line.substring((364-1),(373-1))); popFemale = Integer.parseInt(line.substring((373-1), (382-1)));

                aUnder1 = Integer.parseInt(line.substring((796 - 1), (805 - 1))); a1and2 = Integer.parseInt(line.substring((805 - 1), (814 - 1)));
                a3and4 = Integer.parseInt(line.substring((814 - 1), (823 - 1))); a5 = Integer.parseInt(line.substring((823 - 1), (832 - 1)));
                a6 = Integer.parseInt(line.substring((832 - 1), (841 - 1))); a7to9 = Integer.parseInt(line.substring((841- 1), (850 - 1)));
                a10and11 = Integer.parseInt(line.substring((850 - 1), (859 - 1))); a12and13 = Integer.parseInt(line.substring((859 - 1), (868 - 1)));
                a14 = Integer.parseInt(line.substring((868 - 1), (877 - 1))); a15 = Integer.parseInt(line.substring((877 - 1), (886- 1)));
                a16 = Integer.parseInt(line.substring((886 - 1), (895 - 1))); a17 = Integer.parseInt(line.substring((895 - 1), (904 - 1)));
                a18 = Integer.parseInt(line.substring((904 - 1), (913- 1))); a19 = Integer.parseInt(line.substring((913- 1), (922- 1)));
                a20 = Integer.parseInt(line.substring((922 - 1), (931 - 1))); a21 = Integer.parseInt(line.substring((931 - 1), (940 - 1)));
                a22to24 = Integer.parseInt(line.substring((940 - 1), (949 - 1))); a25to29 = Integer.parseInt(line.substring((949 - 1), (958 - 1)));
                a30to34 = Integer.parseInt(line.substring((958- 1), (967 - 1))); a35to39 = Integer.parseInt(line.substring((967- 1), (976- 1)));
                a40to44 = Integer.parseInt(line.substring((976 - 1), (985 - 1))); a45to49 = Integer.parseInt(line.substring((985 - 1), (994 - 1)));
                a50to54 = Integer.parseInt(line.substring((994 - 1), (1003 - 1))); a55to59 = Integer.parseInt(line.substring((1003 - 1), (1012- 1)));
                a60and61 = Integer.parseInt(line.substring((1012 - 1), (1021 - 1))); a62to64 = Integer.parseInt(line.substring((1021 - 1), (1030- 1)));
                a65to69 = Integer.parseInt(line.substring((1030 - 1), (1039 - 1))); a70to74 = Integer.parseInt(line.substring((1039 - 1), (1048 - 1)));
                a75to79 = Integer.parseInt(line.substring((1048 - 1), (1057- 1))); a80to84 = Integer.parseInt(line.substring((1057 - 1), (1066 - 1)));
                a85andUp = Integer.parseInt(line.substring((1066 - 1), (1075- 1)));

                hFUnder1 = Integer.parseInt(line.substring((3865 - 1), (3874- 1))); hF1and2 = Integer.parseInt(line.substring((3874 - 1), (3883- 1)));
                hF3and4 = Integer.parseInt(line.substring((3883 - 1), (3892- 1))); hF5 = Integer.parseInt(line.substring((3892- 1), (3901- 1)));
                hF6 = Integer.parseInt(line.substring((3901 - 1), (3910- 1))); hF7to9 = Integer.parseInt(line.substring((3910 - 1), (3919- 1)));
                hF10and11 = Integer.parseInt(line.substring((3919 - 1), (3928 - 1))); hF12and13 = Integer.parseInt(line.substring((3928 - 1), (3937 - 1)));
                hF14 = Integer.parseInt(line.substring((3937 - 1), (3946 - 1))); hF15 = Integer.parseInt(line.substring((3946 - 1), (3955 - 1)));
                hF16 = Integer.parseInt(line.substring((3955 - 1), (3964- 1))); hF17 = Integer.parseInt(line.substring((3964- 1), (3973- 1)));
                hF18 = Integer.parseInt(line.substring((3973 - 1), (3982 - 1))); hF19 = Integer.parseInt(line.substring((3982- 1), (3991- 1)));
                hF20 = Integer.parseInt(line.substring((3991 - 1), (4000 - 1))); hF21 = Integer.parseInt(line.substring((4000 - 1), (4009 - 1)));
                hF22to24 = Integer.parseInt(line.substring((4009 - 1), (4018 - 1))); hF25to29 = Integer.parseInt(line.substring((4018 - 1), (4027- 1)));
                hF30to34 = Integer.parseInt(line.substring((4027- 1), (4036- 1))); hF35to39 = Integer.parseInt(line.substring((4036- 1), (4045- 1)));
                hF40to44 = Integer.parseInt(line.substring((4045 - 1), (4054- 1))); hF45to49 = Integer.parseInt(line.substring((4054 - 1), (4063- 1)));
                hF50to54 = Integer.parseInt(line.substring((4063 - 1), (4072 - 1))); hF55to59 = Integer.parseInt(line.substring((4072- 1), (4081- 1)));
                hF60and61 = Integer.parseInt(line.substring((4081- 1), (4090- 1))); hF62to64 = Integer.parseInt(line.substring((4090  - 1), (4099- 1)));
                hF65to69 = Integer.parseInt(line.substring((4099 - 1), (4108 - 1))); hF70to74 = Integer.parseInt(line.substring((4108 - 1), (4117 - 1)));
                hF75to79 = Integer.parseInt(line.substring((4117 - 1), (4126 - 1))); hF80to84 = Integer.parseInt(line.substring((4126 - 1), (4135 - 1)));
                hF85andUp = Integer.parseInt(line.substring((4135 - 1), (4144 - 1)));

                hMUnder1 = Integer.parseInt(line.substring((4144 - 1), (4153- 1))); hM1and2 = Integer.parseInt(line.substring((4153 - 1), (4162- 1)));
                hM3and4 = Integer.parseInt(line.substring((4162 - 1), (4171 - 1))); hM5 = Integer.parseInt(line.substring((4171 - 1), (4180 - 1)));
                hM6 = Integer.parseInt(line.substring((4180 - 1), (4189- 1))); hM7to9 = Integer.parseInt(line.substring((4189 - 1), (4198 - 1)));
                hM10and11 = Integer.parseInt(line.substring((4198 - 1), (4207 - 1))); hM12and13 = Integer.parseInt(line.substring((4207 - 1), (4216 - 1)));
                hM14 = Integer.parseInt(line.substring((4216- 1), (4225 - 1))); hM15 = Integer.parseInt(line.substring((4225 - 1), (4234 - 1)));
                hM16 = Integer.parseInt(line.substring((4234 - 1), (4243- 1))); hM17 = Integer.parseInt(line.substring((4243 - 1), (4252 - 1)));
                hM18 = Integer.parseInt(line.substring((4252 - 1), (4261 - 1))); hM19 = Integer.parseInt(line.substring((4261- 1), (4270 - 1)));
                hM20 = Integer.parseInt(line.substring((4270 - 1), (4279 - 1))); hM21 = Integer.parseInt(line.substring((4279 - 1), (4288- 1)));
                hM22to24 = Integer.parseInt(line.substring((4288 - 1), (4297 - 1))); hM25to29 = Integer.parseInt(line.substring((4297 - 1), (4306- 1)));
                hM30to34 = Integer.parseInt(line.substring((4306 - 1), (4315 - 1))); hM35to39 = Integer.parseInt(line.substring((4315 - 1), (4324 - 1)));
                hM40to44 = Integer.parseInt(line.substring((4324- 1), (4333 - 1))); hM45to49 = Integer.parseInt(line.substring((4333 - 1), (4342 - 1)));
                hM50to54 = Integer.parseInt(line.substring((4342 - 1), (4351 - 1))); hM55to59 = Integer.parseInt(line.substring((4351 - 1), (4360 - 1)));
                hM60and61 = Integer.parseInt(line.substring((4360 - 1), (4369- 1))); hM62to64 = Integer.parseInt(line.substring((4369 - 1), (4378 - 1)));
                hM65to69 = Integer.parseInt(line.substring((4378 - 1), (4387 - 1))); hM70to74 = Integer.parseInt(line.substring((4387 - 1), (4396- 1)));
                hM75to79 = Integer.parseInt(line.substring((4396 - 1), (4405 - 1))); hM80to84 = Integer.parseInt(line.substring((4405 - 1), (4414- 1)));
                hM85andUp = Integer.parseInt(line.substring((4414 - 1), (4423- 1)));

                neverMarriedMale = Integer.parseInt(line.substring((4423 - 1), (4432 - 1))); marriedMale = Integer.parseInt(line.substring((4432 - 1), (4441- 1))); 
                separatedMale = Integer.parseInt(line.substring((4441 - 1), (4450- 1))); widowedMale = Integer.parseInt(line.substring((4450 - 1), (4459 - 1)));
                neverMarriedFemale = Integer.parseInt(line.substring((4468 - 1), (4477- 1))); marriedFemale = Integer.parseInt(line.substring((4477 - 1), (4486 - 1))); 
                separatedFemale = Integer.parseInt(line.substring((4486 - 1), (4495 - 1))); widowedFemale = Integer.parseInt(line.substring((4495 - 1), (4504- 1)));

                

            }
            else if (segment == 2){
                ownerOccupied = Integer.parseInt(line.substring((1804 - 1), (1813 - 1)));
                renterOccupied = Integer.parseInt(line.substring((1813 - 1), (1822 - 1)));

                insideUrban = Integer.parseInt(line.substring((1822 - 1), (1831 - 1))); outsideUrban = Integer.parseInt(line.substring((1831 - 1), ( 1840- 1))); 
                rural = Integer.parseInt(line.substring((1840 - 1), (1849 - 1))); nDefined = Integer.parseInt(line.substring((1849 - 1), (1858 - 1)));

                room1 = Integer.parseInt(line.substring((2389 - 1), (2398 - 1))); room2 = Integer.parseInt(line.substring((2398 - 1), (2407 - 1))); 
                room3 = Integer.parseInt(line.substring((2407 - 1), (2416 - 1))); room4 = Integer.parseInt(line.substring((2416 - 1), (2425 - 1))); 
                room5 = Integer.parseInt(line.substring((2425 - 1), (2434 - 1))); room6 = Integer.parseInt(line.substring((2434 - 1), (2443 - 1))); 
                room7 = Integer.parseInt(line.substring((2443 - 1), (2452 - 1))); room8 = Integer.parseInt(line.substring((2452 - 1), (2461 - 1)));
                room9 = Integer.parseInt(line.substring((2461 - 1), (2470 - 1)));

                ownLess15000 = Integer.parseInt(line.substring((2929 - 1), (2938 - 1))); own15000to19999 = Integer.parseInt(line.substring((2938 - 1), (2947 - 1))); 
                own20000to24999 = Integer.parseInt(line.substring((2947 - 1), (2956 - 1))); own25000to29999 = Integer.parseInt(line.substring((2956 - 1), (2965 - 1)));
                own30000to34999 = Integer.parseInt(line.substring((2965 - 1), (2974 - 1))); own35000to39999 = Integer.parseInt(line.substring((2974 - 1), (2983 - 1))); 
                own40000to44999 = Integer.parseInt(line.substring((2983 - 1), (2992 - 1))); own45000to49999 = Integer.parseInt(line.substring((2992 - 1), (3001 - 1)));
                own50000to59999 = Integer.parseInt(line.substring((3001 - 1), (3010 - 1))); own60000to74999 = Integer.parseInt(line.substring((3010 - 1), (3019 - 1))); 
                own75000to99999 = Integer.parseInt(line.substring((3019 - 1), (3028 - 1))); own100000to124999 = Integer.parseInt(line.substring((3028 - 1), (3037 - 1)));
                own125000to149999 = Integer.parseInt(line.substring((3037 - 1), (3046 - 1))); own150000to174999 = Integer.parseInt(line.substring((3046 - 1), (3055 - 1))); 
                own175000to199999 = Integer.parseInt(line.substring((3055 - 1), (3064 - 1))); own200000to249999 = Integer.parseInt(line.substring((3064 - 1), (3073 - 1))); 
                own250000to299999 = Integer.parseInt(line.substring((3073 - 1), (3082 - 1))); own300000to399999 = Integer.parseInt(line.substring((3082 - 1), ( 3091- 1)));
                own400000to499999 = Integer.parseInt(line.substring((3091 - 1), (3100 - 1))); own500000andUp = Integer.parseInt(line.substring((3100 - 1), (3109 - 1)));

                rentUnder100 = Integer.parseInt(line.substring((3451 - 1), (3460 - 1))); rent100to149 = Integer.parseInt(line.substring((3460 - 1), (3469 - 1))); 
                rent150to199 = Integer.parseInt(line.substring((3469 - 1), (3478 - 1))); rent200to249 = Integer.parseInt(line.substring((3478 - 1), (3487 - 1)));
                rent250to299 = Integer.parseInt(line.substring((3487 - 1), (3496 - 1))); rent300to349 = Integer.parseInt(line.substring((3496 - 1), (3505 - 1))); 
                rent350to399 = Integer.parseInt(line.substring((3505 - 1), (3514 - 1))); rent400to449 = Integer.parseInt(line.substring((3514 - 1), (3523 - 1)));
                rent450to499 = Integer.parseInt(line.substring((3523 - 1), (3532 - 1))); rent500to549 = Integer.parseInt(line.substring((3532 - 1), (3541 - 1))); 
                rent550to599 = Integer.parseInt(line.substring((3541 - 1), (3550 - 1))); rent600to649 = Integer.parseInt(line.substring((3550 - 1), (3559 - 1)));
                rent650to699 = Integer.parseInt(line.substring((3559 - 1), (3568 - 1))); rent700to749 = Integer.parseInt(line.substring((3568 - 1), (3577 - 1))); 
                rent750to999 = Integer.parseInt(line.substring((3577 - 1), (3586 - 1))); rent1000andUp = Integer.parseInt(line.substring((3586 - 1), (3595 - 1)));

            }
            if(segment == 1 || segment == 2) {
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
                context.write(new Text(state), stats);
            }
        }

    }
}
