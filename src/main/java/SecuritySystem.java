import java.util.Map;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

public class SecuritySystem {

        //Suspicious Visitors
        public static final MapStateDescriptor<String,SuspiciousVisitor> suspiciousVisitorMapStateDescriptor =
                new MapStateDescriptor<String,SuspiciousVisitor>("suspicious_visitors", BasicTypeInfo.STRING_TYPE_INFO, Types.POJO(SuspiciousVisitor.class));

        //Family Members
        public static final MapStateDescriptor<String,FamilyMember> familyMemberStateDescriptor =
            new MapStateDescriptor<String,FamilyMember>("family_members", BasicTypeInfo.STRING_TYPE_INFO, Types.POJO(FamilyMember.class));

        //Thief Detections
        public static final MapStateDescriptor<String,DetectedThief> thiefDetectionStateDescriptor =
            new MapStateDescriptor<String,DetectedThief>("thief_detections", BasicTypeInfo.STRING_TYPE_INFO, Types.POJO(DetectedThief.class));

        public static void main(String[] args) throws Exception{

            //Creating a stream environment
            final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

            //Broadcasting suspicious visitors
            DataStream<SuspiciousVisitor> suspiciousVisitors = env
                    .readTextFile("/home/dani/Desktop/distributed-systems/flink-final-project/Input/Suspicious-Visitors.txt")
                    .map(new MapFunction<String, SuspiciousVisitor>() {
                        @Override
                        public SuspiciousVisitor map(String value) throws Exception {
                            return new SuspiciousVisitor(value);
                        }
                    });

            BroadcastStream<SuspiciousVisitor> suspiciousVisitorBroadcast = suspiciousVisitors.broadcast(suspiciousVisitorMapStateDescriptor);

            //---------------------------------------//

            //Broadcasting family members
            DataStream<FamilyMember> familyMembers = env
                    .readTextFile("/home/dani/Desktop/distributed-systems/flink-final-project/Input/Family-Members.txt")
                    .map(new MapFunction<String, FamilyMember>() {
                        @Override
                        public FamilyMember map(String value) throws Exception {
                            return new FamilyMember(value);
                        }
                    });

            BroadcastStream<FamilyMember> familyMemberBroadcast = familyMembers.broadcast(familyMemberStateDescriptor);

            //---------------------------------------//

            //Broadcasting detected thiefs
            DataStream<DetectedThief> detectedThiefs= env
                    .readTextFile("/home/dani/Desktop/distributed-systems/flink-final-project/Input/Detected-Thiefs.txt")
                    .map(new MapFunction<String, DetectedThief>() {
                        @Override
                        public DetectedThief map(String value) throws Exception {
                            return new DetectedThief(value);
                        }
                    });

            BroadcastStream<DetectedThief> detectedThiefBroadcast = detectedThiefs.broadcast(thiefDetectionStateDescriptor);

            //Real-time cctv detections
            DataStream<Tuple2<String,String>> data = env.socketTextStream("localhost",9090)
                    .map(new MapFunction<String, Tuple2<String, String>>() {
                        @Override
                        public Tuple2<String, String> map(String s) throws Exception {
                            String[] ws = s.split(",");
                            //Chayyik lindex 3 shou houwe bhes idvisitor
                            return new Tuple2<String,String>(ws[3],s);
                        }
                    });
            ///In order to add new entry to Detected-Thiefs.txt,
            ///I will check if it's already added, if not => add it
            ///Check if it's a suspicious visitor
            ///Make sure id not in family member id

            //1) Already detected thiefs check
            DataStream<Tuple2<String,String>> detectedThiefChecks = data
                    .keyBy(0)
                    .connect(detectedThiefBroadcast)
                    .process(new DetectedThiefCheck());

            //2) Suspicious visitors check
            DataStream<Tuple2<String,String>> suspiciousVisitorsCheck = data
                    .keyBy(0)
                    .connect(suspiciousVisitorBroadcast)
                    .process(new SuspiciousVisitorCheck());


            //3) Family members check
            DataStream<Tuple2<String,String>> familyMembersCheck = data
                    .keyBy(0)
                    .connect(familyMemberBroadcast)
                    .process(new FamilyMemberCheck());


            //All detected thieves
            DataStream<Tuple2<String,String>> AllDetectedThieves =
                    detectedThiefChecks.union(suspiciousVisitorsCheck,familyMembersCheck);

            AllDetectedThieves.writeAsText("/home/dani/Desktop/distributed-systems/flink-final-project/Output/Quarantined-Visitors.txt");

            env.execute("Cctv Streaming");
        }
    public static class DetectedThiefCheck extends KeyedBroadcastProcessFunction<String,Tuple2<String,String>,DetectedThief,Tuple2<String,String>>{

        @Override
        public void processElement(Tuple2<String, String> value, KeyedBroadcastProcessFunction<String, Tuple2<String, String>, DetectedThief, Tuple2<String, String>>.ReadOnlyContext readOnlyContext, Collector<Tuple2<String, String>> collector) throws Exception {
            for(Map.Entry<String,DetectedThief> detEntry : readOnlyContext.getBroadcastState(thiefDetectionStateDescriptor).immutableEntries()){
                final String detectedThiefId = detEntry.getKey();
                final DetectedThief thief = detEntry.getValue();

                final String dId = value.f1.split(",")[4];
                if(dId.equals(detectedThiefId)){
                    collector.collect(new Tuple2<String,String>("**ALARM**","Visit " + value + "by a an already detected thief"));
                }
            }
        }

        @Override
        public void processBroadcastElement(DetectedThief detectedThief, Context context, Collector<Tuple2<String, String>> collector) throws Exception {
            context.getBroadcastState(thiefDetectionStateDescriptor).put(detectedThief.idThief,detectedThief);
        }

    }
    public static class SuspiciousVisitorCheck extends KeyedBroadcastProcessFunction<String,Tuple2<String,String>,SuspiciousVisitor,Tuple2<String,String>> {

        @Override
        public void processElement(Tuple2<String, String> value, KeyedBroadcastProcessFunction<String, Tuple2<String, String>, SuspiciousVisitor, Tuple2<String, String>>.ReadOnlyContext readOnlyContext, Collector<Tuple2<String, String>> collector) throws Exception {
            for (Map.Entry<String, SuspiciousVisitor> susEntry : readOnlyContext.getBroadcastState(suspiciousVisitorMapStateDescriptor).immutableEntries()) {
                final String susId = susEntry.getKey();
                final SuspiciousVisitor susVis = susEntry.getValue();

                final String sId = value.f1.split(",")[4];
                if (sId.equals(susId) || value.getField(5).toString().equals("N/A")) {
                    collector.collect(new Tuple2<String, String>("**ALARM**", "Visit " + value + "by a suspicious visitor"));
                }
            }
        }

        @Override
        public void processBroadcastElement(SuspiciousVisitor suspiciousVisitor, Context context, Collector<Tuple2<String, String>> collector) throws Exception {
            context.getBroadcastState(suspiciousVisitorMapStateDescriptor).put(suspiciousVisitor.idSuspicious, suspiciousVisitor);
        }
    }
    public static class FamilyMemberCheck extends KeyedBroadcastProcessFunction<String, Tuple2<String, String>, FamilyMember, Tuple2<String, String>> {

            @Override
            public void processElement(Tuple2<String, String> value, KeyedBroadcastProcessFunction<String, Tuple2<String, String>, FamilyMember, Tuple2<String, String>>.ReadOnlyContext readOnlyContext, Collector<Tuple2<String, String>> collector) throws Exception {
                for (Map.Entry<String, FamilyMember> famEntry : readOnlyContext.getBroadcastState(familyMemberStateDescriptor).immutableEntries()) {
                    final String familyMemberId = famEntry.getKey();
                    final FamilyMember mem = famEntry.getValue();

                    final String fId = value.f1.split(",")[4];
                    if (!fId.equals(familyMemberId)) {
                        collector.collect(new Tuple2<String, String>("**ALARM**", "Visit " + value + "by a family member"));
                    }
                }
            }

            @Override
            public void processBroadcastElement(FamilyMember familyMember, Context context, Collector<Tuple2<String, String>> collector) throws Exception {
                context.getBroadcastState(familyMemberStateDescriptor).put(familyMember.idFamilyMember, familyMember);
            }
        }

}

