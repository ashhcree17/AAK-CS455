package cs455.spark.Q6ActivityLevelsPollutionEffect;

import scala.Tuple2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.math.BigInteger;

/*
	Q6. Do high levels of event activity increase pollution levels?
*/
public final class ActivityEffectsOnPollutionEvaluation {
	public static void main(String[] args) throws Exception {
		// The source file is the first argument
		if (args.length < 4) {
			System.err.println("Usage: ActivityEffectsOnPollution <PollutionDir> <CulturalEventDir> <OutputDir>");
			System.exit(1);
		}
		
		// create a spark session
		SparkSession spark = SparkSession
			.builder()
			.appName("ActivityEffectsOnPollution")
			.getOrCreate();

		// read in the files (if passed a directory, it will read all files in it)
        // Loads the text into a Spark RDD which is a distributed representation of each line of text
		JavaRDD<String> pollutionLines = spark.read().textFile(args[0]).javaRDD();
		JavaRDD<String> culturalEventLines = spark.read().textFile(args[1]).javaRDD();
	
		// get the needed values out of the lines
		JavaPairRDD<String, ArrayList<String>> pollutionData = 
		    pollutionLines.mapToPair(new GetPollutionForCorrelationWithEvent());
		JavaPairRDD<String, ArrayList<String>> culturalEventData = 
		    culturalEventLines.mapToPair(new GetCulturalEventForCorrelationWithPollution());
		
		// correlate the pollution with the events
		JavaPairRDD<String, Tuple2<ArrayList<String>, ArrayList<String>>> correlatedTuples
		  = culturalEventData.join(pollutionData);
		
		// reduce them to the keys we care about
		JavaPairRDD<String, ArrayList<String>> correlated = correlatedTuples
		  .mapToPair(new CombineAndRekey());
		
		// combine duplicate keys
    	JavaPairRDD<String, ArrayList<String>> reduced = correlated.reduceByKey(new ReduceDuplicateKeys());
		
		// Average the pollution levels
		JavaPairRDD<String, ArrayList<String>> averaged = reduced.mapToPair(new AveragePollutionLevels());
		averaged.saveAsTextFile(args[3]);

		// end the session
		spark.stop();
	}
	
	private static class AveragePollutionLevels implements PairFunction<
	 Tuple2<String, ArrayList<String>>, String, ArrayList<String>> {
		public Tuple2<String, ArrayList<String>> call (Tuple2<String, ArrayList<String>> data) {
			// data._2() 0-4 is the pollution data, 5 is the count
			
			//Sum the data
			ArrayList<String> averaged = new ArrayList<String>();
			int lastIndex = data._2().size() - 1;
			double count = Double.parseDouble(data._2().get(lastIndex));

			for (int i = 0; i < lastIndex; i++) {
				averaged.add("" + Double.parseDouble(data._2().get(i)) / count);
			}
			
			return new Tuple2<String, ArrayList<String>>(data._1(), averaged);
		}
	}
	
	private static class ReduceDuplicateKeys implements Function2<ArrayList<String>, 
	 ArrayList<String>, ArrayList<String>> {

		public ArrayList<String> call (ArrayList<String> data1, ArrayList<String> data2) {
			// data._2() 0-4 is the pollution data, 5 is the count
			if (data1.size() < 5) {
				System.err.println("d1: " + data1.size() + " " + Arrays.toString(data1.toArray()));
				if (data2.size() < 5) {
					System.err.println("d2: " + data2.size() + " " + Arrays.toString(data2.toArray()));
				}
				return data1;
			}
			if (data2.size() < 5) {
				System.err.println("d2: " + data2.size() + " " + Arrays.toString(data2.toArray()));
				return data1;
			}
			
			//Sum the data
			ArrayList<String> combined = new ArrayList<String>();
			BigInteger d1Value = null;
			BigInteger d2Value = null;

			for (int i = 0; i < data1.size(); i++) {
				d1Value = new BigInteger(data1.get(i));
				d2Value = new BigInteger(data2.get(i));
				combined.add("" + d1Value.add(d2Value));
			}
			
			return combined;
		}
	}
	
	private static class CombineAndRekey implements PairFunction<Tuple2
	 <String, Tuple2<ArrayList<String>, ArrayList<String>>>, String, ArrayList<String>> {
		public Tuple2<String, ArrayList<String>> call (Tuple2<String, 
		 Tuple2<ArrayList<String>, ArrayList<String>>> data) {
			// data._2()._1() is status (0)
			// data._2()._2() 0-4 is the pollution data
			
			// determine the new key (row TBD)
			String newKey = data._2()._1().get(0);
			
			// add a row for count so we can average
			ArrayList<String> newData = new ArrayList<String>(data._2()._2());
			newData.add("1");
			
			return new Tuple2<String, ArrayList<String>>(newKey, newData);
		}
	}
	
	private static class GetPollutionForCorrelationWithEvent implements 
	 PairFunction<String, String, ArrayList<String>> {
		public Tuple2<String, ArrayList<String>> call(String row) { 
			/* Format:
				ozone,particullate_matter,carbon_monoxide,sulfure_dioxide,
				nitrogen_dioxide,longitude,latitude,timestamp
				
				we want the timestamp(7), latitude(6) and longitude(5) 
				for correlating event to pollution and the different 
				levels for analysis
			*/
			
			// split the data on commas
			String[] fields = row.split(",");
			
			// get the date or however we want to correlate it
			String dateLoc = fields[6] + "," + fields[5] + "," + fields[7];
			
			//get the polution data
			ArrayList<String> data = new ArrayList<String>();
			data.add(fields[0]);
			data.add(fields[1]);
			data.add(fields[2]);
			data.add(fields[3]);
			data.add(fields[4]);
		
			//make sure all the data was good and return it
			return new Tuple2<String, ArrayList<String>>(dateLoc, data);
		}
	}
	
  private static class GetCulturalEventForCorrelationWithPollution implements 
   PairFunction<String, String, ArrayList<String>> {
		public Tuple2<String, ArrayList<String>> call(String row) { 
			/* Format (one-line):
				** Cultural Event **
				******* Cultural event column names not provided directly
				******* - deductions made from provided CSV data and TTL formatted data
				POINT(0),CATEGORY(1),TITLE(2),TICKET_URL(3),TICKET_PRICE(4),?(5),?(6),
				LONGITUDE(7),EVENT_ID(8),TITLE_HTML(9),?(10),EVENT_NAME(11),
				EVENT_DATE_TIME(12),LATITUDE(13),EVENT_URL(14),ATTENDEES_NUMBER(15),
				EVENT_TYPE(16),EVENT_IMAGE_URL(17),EVENT_GENRE(18)

			 	we want the EVENT_DATE_TIME(12), LONGITUDE(7), and 
			 	LATITUDE(13) for correlating events to pollution
			 	we want the TICKET_PRICE(4) and ATTENDEES_NUMBER(15)
			 	for pollution analysis 
			*/

			//split the data on commas
			String[] fields = row.split(",");
			// dateLoc = lat, long, datetime
			String dateLoc = fields[13] + "," + fields[7] + "," + fields[12];
			
			//get the event date time associated with the id
			ArrayList<String> data = new ArrayList<String>();
			data.add(fields[4]);
			data.add(fields[15]);
		
			return new Tuple2<String, ArrayList<String>>(dateLoc, data);
		}
	}
}
