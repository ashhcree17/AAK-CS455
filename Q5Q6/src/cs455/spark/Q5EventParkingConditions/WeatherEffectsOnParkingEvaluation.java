package cs455.spark.Q5EventParkingConditions;

import scala.Tuple2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.math.BigInteger;

import cs455.spark.Utils.WeatherParser;

/*
	Q5. What is the best/worst weather conditions to find parking for an event?
*/
public final class WeatherEffectsOnParkingEvaluation {
	public static void main(String[] args) throws Exception {
		// The source file is the first arguement
		if (args.length < 4) {
			System.err.println("Usage: WeatherEffectsOnParking <WeatherDir> <ParkingDir> <ParkingMetaDataDir> <CulturalEventDir> <OutputDir>");
			System.exit(1);
		}
		
		// create a spark session
		SparkSession spark = SparkSession
			.builder()
			.appName("WeatherEffectsOnParking")
			.getOrCreate();

		// read in the files (if passed a directory, it will read all files in it)
		JavaRDD<String> parkingLines = spark.read().textFile(args[1]).javaRDD();
		JavaRDD<String> parkingMetaDataLines = spark.read().textFile(args[2]).javaRDD();
		JavaRDD<String> culturalEventLines = spark.read().textFile(args[3]).javaRDD();
		JavaRDD<String> libraryEventLines = spark.read().textFile(args[4]).javaRDD();
	
		// get the needed values out of the lines
		JavaPairRDD<String, ArrayList<String>> weatherData = WeatherParser.parseWeather(args[0], spark);
		JavaPairRDD<String, ArrayList<String>> parkingRawData = parkingLines
											.mapToPair(new GetParkingForCorrelationWithWeather());
		JavaPairRDD<String, ArrayList<String>> parkingMetaData = parkingMetaDataLines
											.mapToPair(new GetParkingMetaForCorrelationWithWeather());
		JavaPairRDD<String, ArrayList<String>> culturalEventData = 
		    culturalEventLines.mapToPair(new GetCulturalEventForCorrelationWithPollution());
		
		//convert the parking to lat lon points
		JavaPairRDD<String, Tuple2<ArrayList<String>, ArrayList<String>>> parkingLatLonData = parkingRawData.join(parkingMetaData);
		parkingLatLonData.saveAsTextFile(args[5]);
		JavaPairRDD<String, ArrayList<String>> parkingConvertedData = parkingLatLonData.mapToPair(new ConvertParkingToLatLon());

		// correlate the weather with the events
		JavaPairRDD<String, Tuple2<ArrayList<String>, ArrayList<String>>> eventCorrelatedTuples
		  = culturalEventData.join(weatherData);

		// correlate the weather with the parking
		JavaPairRDD<String, Tuple2<ArrayList<String>, ArrayList<String>>> parkingCorrelatedTuples
		  = parkingConvertedData.join(weatherData);

		// reduce them to the keys we care about
		JavaPairRDD<String, ArrayList<String>> correlatedEvents = eventCorrelatedTuples.mapToPair(new CombineAndRekeyEvents());
		JavaPairRDD<String, ArrayList<String>> correlatedParking = parkingCorrelatedTuples.mapToPair(new CombineAndRekeyParking());
		
		// combine duplicate keys
        JavaPairRDD<String, ArrayList<String>> reducedEvents = correlatedEvents.reduceByKey(new ReduceDuplicateKeys());
        JavaPairRDD<String, ArrayList<String>> reducedParking = correlatedParking.reduceByKey(new ReduceDuplicateKeys());
		
		// Average the parking levels
		JavaPairRDD<String, ArrayList<String>> averaged = reducedParking.mapToPair(new AverageParkingLevels());
		weatherData.saveAsTextFile(args[5]+"-weather");
		reducedEvents.saveAsTextFile(args[5]+"-events");
		averaged.saveAsTextFile(args[5]+"-parking");

		// end the session
		spark.stop();
	}
	
	private static class AverageParkingLevels implements PairFunction<
	 Tuple2<String, ArrayList<String>>, String, ArrayList<String>> {
		public Tuple2<String, ArrayList<String>> call (Tuple2<String, ArrayList<String>> data) {
			/* Format (one-line):
				VEHICLECOUNT(0),UPDATETIME(1),_ID(2),TOTALSPACES(3),GARAGECODE(4),STREAMTIME(5)

			 	we want the VEHICLECOUNT(0), TOTALSPACES(3), GARAGECODE(4) for correlating 
			 	events & weather to parking
			*/

			//  data._2() 0-4 is the pollution data, 5 is the count
			
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
	
	private static class ReduceDuplicateKeys implements Function2<ArrayList<String>, ArrayList<String>, ArrayList<String>> {
		public ArrayList<String> call (ArrayList<String> data1, ArrayList<String> data2) {
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
	
	private static class CombineAndRekeyParking implements PairFunction<Tuple2<String, Tuple2<ArrayList<String>, ArrayList<String>>>, String, ArrayList<String>> {
		public Tuple2<String, ArrayList<String>> call (Tuple2<String, Tuple2<ArrayList<String>, ArrayList<String>>> data) {
			// String newKey = data._2()._2().get(0) + "," + data._2()._2().get(1) + "," + newData.get(0) + ":" + percentAvailable

			// determine the new key
			String newKey = data._2()._1().get(0);
			
			// add a row for count so we can average
			ArrayList<String> newData = new ArrayList<String>(data._2()._2());
			newData.add("1");
			
			return new Tuple2<String, ArrayList<String>>(newKey, newData);
		}
	}

	private static class CombineAndRekeyEvents implements PairFunction<Tuple2<String, Tuple2<ArrayList<String>, ArrayList<String>>>, String, ArrayList<String>> {
		public Tuple2<String, ArrayList<String>> call (Tuple2<String, Tuple2<ArrayList<String>, ArrayList<String>>> data) {
			//in data._2()._1() -- data - EVENT_ID(0), EVENT_DATE_TIME(1), ATTENDEES_NUMBER(2)
			
			// determine the new key -- IS THE EVENT ID
			String newKey = data._2()._1().get(0);
			
			// add a row for count so we can average
			ArrayList<String> newData = new ArrayList<String>(data._2()._1().get(1), data._2()._1().get(2));
			newData.add("1");
			
			return new Tuple2<String, ArrayList<String>>(newKey, newData);
		}
	}

	private static class ConvertParkingToLatLon implements PairFunction<
	  Tuple2<String, Tuple2<ArrayList<String>, ArrayList<String>>>, String, ArrayList<String>> {
		public Tuple2<String, ArrayList<String>> call (Tuple2<String, 
		 Tuple2<ArrayList<String>, ArrayList<String>>> data) {
			//in data._2()._1() -- raw data - GARAGECODE(0), VEHICLECOUNT(1), TOTALSPACES(2)
			//in data._2()._2() -- meta data - GARAGECODE(0), LATITUDE(1), LONGITUDE(2)

			// determine the new key
			ArrayList<String> newData = new ArrayList<String>(data._2()._1());

			// determine percentage of available parking (vehicle count / total spaces) 
			BigInteger percentAvailable = newData.get(1) / newData.get(2);
			// lat, long, garagecode:percentAvailable
			String newKey = data._2()._2().get(0) + "," + data._2()._2().get(1) + "," + newData.get(0) + ":" + percentAvailable;
			newData.remove(0);
			
			return new Tuple2<String, ArrayList<String>>(newKey, newData);
		}
	}

	private static class GetParkingForCorrelationWithWeather implements 
	 PairFunction<String, String, ArrayList<String>> {
		public Tuple2<String, ArrayList<String>> call(String row) { 
			/* Format (one-line):
				VEHICLECOUNT(0),UPDATETIME(1),_ID(2),TOTALSPACES(3),
				GARAGECODE(4),STREAMTIME(5)

			 	we want the VEHICLECOUNT(0), TOTALSPACES(3), GARAGECODE(4) for correlating 
			 	events & weather to parking
			*/
			
			//split the data on commas
			String[] fields = row.split(",");
			//get the id that this point corresponds to
			String key = fields[4];
			
			//get the traffic lat and long associated with the id
			ArrayList<String> data = new ArrayList<String>();
			data.add(fields[0]);
			data.add(fields[3]);
			
			return new Tuple2<String, ArrayList<String>>(key, data);
		}
	}

	private static class GetParkingMetaForCorrelationWithWeather implements 
	 PairFunction<String, String, ArrayList<String>> {
		public Tuple2<String, ArrayList<String>> call(String row) { 
			/* Format (one-line):			
				GARAGECODE(0),CITY(1),POSTALCODE(2),STREET(3),HOUSENUMBER(4),
				LATITUDE(5),LONGITUDE(6)
			
				we want the GARAGECODE(0), LATITUDE(5) and LONGITUDE(6) 
				for correlating parking to weather
			*/
			
			//split the data on commas
			String[] fields = row.split(",");
			//get the id that this point corresponds to
			String key = fields[0];
			
			//get the traffic lat and long associated with the id
			ArrayList<String> location = new ArrayList<String>();
			location.add(fields[5]);
			location.add(fields[6]);
		
			return new Tuple2<String, ArrayList<String>>(key, location);
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

			 	we want the EVENT_ID(8), EVENT_DATE_TIME(12), & ATTENDEES_NUMBER(15) 
			 	for correlating events to pollution
			*/

			//split the data on commas
			String[] fields = row.split(",");
			//get the cultural event id that this point corresponds to
			String culturalKey = fields[8];
			
			//get the event date time associated with the id
			ArrayList<String> dateTimeAndAttendees = new ArrayList<String>();
			dateTimeAndAttendees.add(fields[12]);
			dateTimeAndAttendees.add(fields[15]);
		
			return new Tuple2<String, ArrayList<String>>(key, dateTimeAndAttendees);
		}
	}
}
