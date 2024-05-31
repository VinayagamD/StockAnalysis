package com.vinaylogics;

import lombok.val;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.temporal.ChronoUnit;

public class StockAnalysis {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        FileSource<String> futureTradeSource = FileSource
                .forRecordStreamFormat(new TextLineInputFormat(),
                        new Path("/home/vinaylogics/FUTURES_TRADES.txt"))
                .build();
        DataStream<Tuple5<String,String,String,Double,Integer>> data= env.fromSource(futureTradeSource,
                WatermarkStrategy.noWatermarks(),
                "File Source"
        ).map((MapFunction<String, Tuple5<String, String, String, Double, Integer>>) value -> {
            String[] words = value.split(",");
                                // date,  time,     Name,     trade,                      volume
            return new Tuple5<>(words[0], words[1], "XYZ", Double.parseDouble(words[2]), Integer.parseInt(words[3]));
        }).returns(new TupleTypeInfo<>(Types.STRING, Types.STRING,Types.STRING,Types.DOUBLE,Types.INT))
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple5<String, String, String, Double, Integer>>() {
                    private final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    @Override
                    public long extractAscendingTimestamp(Tuple5<String, String, String, Double, Integer> value) {
                        try {
                            val ts = new Timestamp(sdf.parse(value.f0+" "+value.f1).getTime());
                            return ts.getTime();
                        } catch (Exception e) {
                            System.err.println("Parsing error "+e.getMessage());
                            throw new RuntimeException("Parsing Error");
                        }
                    }
                }).returns(new TupleTypeInfo<>(Types.STRING, Types.STRING,Types.STRING,Types.DOUBLE,Types.INT));

        // Alert when price change from one window to another is more than threshold
        DataStream<String> change = data.keyBy(new KeySelector<Tuple5<String, String, String, Double, Integer>, String>() {

            @Override
            public String getKey(Tuple5<String, String, String, Double, Integer> value) throws Exception {
                return value.f2;
            }
        })
                        .window(TumblingEventTimeWindows.of(Duration.of(1, ChronoUnit.MINUTES)))
                                .process(new TrackChange());


        env.execute("Stock Analysis");
    }

    private static class TrackChange extends ProcessWindowFunction<Tuple5<String,String,String,Double,Integer>, String,String, TimeWindow> {
        private transient ValueState< Double > prevWindowMaxTrade;
        private transient ValueState < Integer > prevWindowMaxVol;
        @Override
        public void process(String key, ProcessWindowFunction<Tuple5<String, String, String, Double, Integer>, String, String, TimeWindow>.Context context,
                            Iterable<Tuple5<String, String, String, Double, Integer>> input, Collector<String> out) throws Exception {

            String windowStart = "";
            String windowEnd = "";
            Double windowMaxTrade = 0.0; // 0
            Double windowMinTrade = 0.0; // 106
            Integer windowMaxVol = 0;
            Integer windowMinVol = 0; // 348746

            for (Tuple5<String, String, String, Double, Integer> element : input)
            //  06/10/2010, 08:00:00, 106.0, 348746
            //  06/10/2010, 08:00:00, 105.0, 331580
            {
                if (windowStart.isEmpty()){
                    windowStart = element.f0 + ":" + element.f1;
                    windowMinTrade = element.f3;
                    windowMinVol = element.f4;
                }

                if (element.f3 > windowMaxTrade){
                    windowMaxTrade = element.f3;
                }

                if (element.f3 < windowMinTrade){
                    windowMinTrade = element.f3;
                }

                if (element.f4 > windowMaxVol){
                    windowMaxVol = element.f4;
                }
                if (element.f4 < windowMinVol){
                    windowMinVol = element.f4;
                }

                windowEnd = element.f0 + ":" + element.f1;
            }

            Double maxTradeChange = 0.0;
            Double maxVolChange = 0.0;

            if (prevWindowMaxTrade.value() != 0){
                maxTradeChange = ((windowMaxTrade - prevWindowMaxTrade.value()) / prevWindowMaxTrade.value()) * 100;
            }
            if (prevWindowMaxVol.value() != 0){
                maxVolChange = ((windowMaxVol - prevWindowMaxVol.value()) * 1.0 / prevWindowMaxVol.value()) * 100;
            }

            out.collect(windowStart + " - " + windowEnd + ", " + windowMaxTrade + ", " + windowMinTrade + ", " + String.format("%.2f", maxTradeChange) +
                    ", " + windowMaxVol + ", " + windowMinVol + ", " + String.format("%.2f", maxVolChange));


            prevWindowMaxTrade.update(windowMaxTrade);
            prevWindowMaxVol.update(windowMaxVol);
        }

        @Override
        public void open(OpenContext openContext) throws Exception {
            prevWindowMaxTrade =
                    getRuntimeContext().getState(new ValueStateDescriptor<>("prev_max_trade", BasicTypeInfo.DOUBLE_TYPE_INFO));

            prevWindowMaxVol = getRuntimeContext().getState(new ValueStateDescriptor<>("prev_max_vol", BasicTypeInfo.INT_TYPE_INFO));
        }
    }
}
