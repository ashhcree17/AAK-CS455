package edu.csu.cs;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import scala.Tuple3;

public class App {

    public static void main(String[] args){

        boolean Q1 = false;
        boolean Q2 = false;

        String source = args[0];
        String mdsource = args[1];
        String dest = args[2];

        //Create a SparkContext to initialize
        SparkConf conf = new SparkConf().setMaster("local").setAppName("Project Q1");

        // Create a Java version of the Spark Context
        JavaSparkContext sc = new JavaSparkContext(conf);

        if (Q1)
        {
            // Load the text into a Spark RDD, which is a distributed representation of each line of text
            JavaRDD<String> textFile = sc.textFile(source);
            JavaRDD<String> metadataFile = sc.textFile(mdsource);

            JavaRDD<String> data = textFile.filter(new Function<String, Boolean>() {
                @Override
                public Boolean call(String s) throws Exception {
                    if(s == null || s.trim().length() < 1 || s.contains("avgMeasuredTime")) {
                        return false;
                    }
                    return true;
                }
            });

            JavaRDD<String> metadata = metadataFile.filter(new Function<String, Boolean>() {
                @Override
                public Boolean call(String s) throws Exception {
                    if(s == null || s.trim().length() < 1 || s.contains("DURATION_IN_SEC")) {
                        return false;
                    }
                    return true;
                }
            });


            JavaRDD<TrafficRecord> rdd_records = data.map(
                    new Function<String, TrafficRecord>() {
                        public TrafficRecord call(String line) throws Exception {
                            String[] fields = line.split(",");
                            TrafficRecord sd = new TrafficRecord(fields[8], fields[5], fields[2], fields[6]);
                            return sd;
                        }
                    });

            JavaRDD<TrafficMetadata> mdrdd_records = metadata.map(
                    new Function<String, TrafficMetadata>() {
                        public TrafficMetadata call(String line) throws Exception {
                            String[] fields = line.split(",");
                            TrafficMetadata sd = new TrafficMetadata(fields[0], fields[3], fields[6], fields[9], fields[16],fields[18],fields[20],fields[21],fields[22]);
                            return sd;
                        }
                    });


            JavaPairRDD<String, Tuple3<Double, Double, Integer>> records_JPRDD =
                    rdd_records.mapToPair(new PairFunction<TrafficRecord,String, Tuple3<Double,Double,Integer>>() {
                        public Tuple2<String, Tuple3<Double, Double, Integer>> call(TrafficRecord record) {
                            Tuple2<String, Tuple3<Double, Double, Integer>> t2 =
                                    new Tuple2<String, Tuple3<Double, Double, Integer>>(
                                            record.REPORT_ID + "," + record.TIMESTAMP,
                                            new Tuple3<Double, Double, Integer>(Double.parseDouble(record.avgSpeed), Double.parseDouble(record.vehicleCount), 1)
                                    );
                            return t2;
                        }
                    });

            JavaPairRDD<String,String> records_MDRDD = mdrdd_records.mapToPair(new PairFunction<TrafficMetadata, String, String>() {
                @Override
                public Tuple2<String, String> call(TrafficMetadata trafficMetadata) throws Exception {

                    Tuple2<String, String> ret = new Tuple2<String,String>(trafficMetadata.REPORT_ID, trafficMetadata.POINT_1_STREET + "-" + trafficMetadata.POINT_1_CITY + "-" +
                            trafficMetadata.POINT_1_COUNTRY + "-" + trafficMetadata.POINT_2_STREET + "-" + trafficMetadata.POINT_2_CITY + "-" +
                            trafficMetadata.POINT_2_COUNTRY + "-" + trafficMetadata.DISTANCE_IN_METERS + "-" + trafficMetadata.REPORT_ID);
                    return ret;
                }
            });

            JavaPairRDD<String, Tuple3<Double, Double, Integer>> final_rdd_records =
                    records_JPRDD.reduceByKey(new Function2<Tuple3<Double, Double, Integer>, Tuple3<Double, Double, Integer>, Tuple3<Double, Double, Integer>>() {
                        public Tuple3<Double, Double, Integer> call(Tuple3<Double, Double, Integer> v1,
                                                                    Tuple3<Double, Double, Integer> v2) throws Exception {
                            return new Tuple3<>(v1._1() + v2._1(), v1._2() + v2._2(), v1._3() + v2._3());
                        }
                    });

            JavaPairRDD<String, Tuple2<Double, Double>> averagePair = final_rdd_records.mapToPair(getAverageByKey);


            JavaPairRDD<Tuple2<Double, Double>,String> swapAveragePair = averagePair.mapToPair(new PairFunction<Tuple2<String, Tuple2<Double, Double>>, Tuple2<Double, Double>, String>() {
                @Override
                public Tuple2<Tuple2<Double, Double>, String> call(Tuple2<String, Tuple2<Double, Double>> stringTuple2Tuple2) throws Exception {
                    return stringTuple2Tuple2.swap();
                }
            });

            JavaPairRDD<Tuple2<Double, Double>,String> sortedSwappedAveragePair = swapAveragePair.sortByKey(new Tuple2Comparator(),false);


            JavaPairRDD<String,Tuple3<String,Double,Double>> sortedAveragePair = sortedSwappedAveragePair.mapToPair(new PairFunction<Tuple2<Tuple2<Double, Double>, String>, String, Tuple3<String, Double, Double>>() {
                @Override
                public Tuple2<String, Tuple3<String, Double, Double>> call(Tuple2<Tuple2<Double, Double>, String> tuple2StringTuple2) throws Exception {
                    Tuple2<String,Tuple2<Double, Double>> swapped = tuple2StringTuple2.swap();
                    Tuple2<String, Tuple3<String, Double, Double>> ret = new Tuple2<>(swapped._1.split(",")[0], new Tuple3<String, Double, Double>(swapped._1.split(",")[1],swapped._2()._1,swapped._2()._2));
                    return ret;
                }
            });

            JavaPairRDD<String, Tuple2<Tuple3<String, Double, Double>, String >> mergedSortedAveragePair = sortedAveragePair.join(records_MDRDD);

            mergedSortedAveragePair.foreach(p -> System.out.println(p));
            System.out.println("Total words: " + mergedSortedAveragePair.count());
            mergedSortedAveragePair.saveAsTextFile(dest);
        }

        if(Q2)
        {

        }

        //stop sc
        sc.stop();
        sc.close();


    }

    private static PairFunction<Tuple2<String, Tuple3<Double, Double, Integer>>,String,Tuple2<Double, Double>> getAverageByKey = (tuple) -> {
        Tuple3<Double, Double, Integer> val = tuple._2;
        Double speed = val._1();
        Double vehile = val._2();
        int count = val._3();
        Tuple2<String, Tuple2<Double, Double>> averagePair = new Tuple2<String, Tuple2<Double, Double>>(tuple._1, new Tuple2<Double, Double>(speed/count, vehile/count));
        return averagePair;
    };
}
