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

/*
	Q5. What is the best/worst weather conditions to find parking for an event?
*/
public final class WeatherEffectsOnParkingEvaluation {
	public static void main(String[] args) throws Exception {
		// The source file is the first arguement
		if (args.length < 4) {
			System.err.println("Usage: WeatherEffectsOnParking <WeatherDir> <ParkingDir> <ParkingMetaDataDir> <CulturalEventDir> <LibraryEventDir> <OutputDir>");
			System.exit(1);
		}
		
		// create a spark session
		SparkSession spark = SparkSession
			.builder()
			.appName("WeatherEffectsOnParking")
			.getOrCreate();

		// read in the files (if passed a directory, it will read all files in it)
		JavaRDD<String> weatherLines = spark.read().textFile(args[0]).javaRDD();
		JavaRDD<String> parkingLines = spark.read().textFile(args[1]).javaRDD();
		JavaRDD<String> parkingMetaDataLines = spark.read().textFile(args[2]).javaRDD();
		JavaRDD<String> culturalEventLines = spark.read().textFile(args[3]).javaRDD();
		JavaRDD<String> libraryEventLines = spark.read().textFile(args[4]).javaRDD();
	
		// get the needed values out of the lines
		JavaPairRDD<String, ArrayList<String>> weatherData = weatherLines
											.mapToPair(new GetWeatherForCorrelationWithParking());
		JavaPairRDD<String, ArrayList<String>> parkingRawData = parkingLines
											.mapToPair(new GetParkingForCorrelationWithWeather());
		JavaPairRDD<String, ArrayList<String>> parkingMetaData = parkingMetaDataLines
											.mapToPair(new GetParkingMetaForCorrelationWithWeather());
		JavaPairRDD<String, ArrayList<String>> culturalEventData = 
		    culturalEventLines.mapToPair(new GetCulturalEventForCorrelationWithPollution());
		JavaPairRDD<String, ArrayList<String>> libraryEventData = 
		    libraryEventLines.mapToPair(new GetLibraryEventForCorrelationWithPollution());
		JavaPairRDD<String, ArrayList<String>> eventsData = culturalEventData.union(libraryEventData);

		/*
		//convert the traffic to lat lon points
		JavaPairRDD<String, Tuple2<ArrayList<String>, ArrayList<String>>> trafficLatLonData = trafficRawData.join(trafficMetaData);
		trafficLatLonData.saveAsTextFile(args[3]);
		JavaPairRDD<String, ArrayList<String>> trafficP1Data = trafficLatLonData.mapToPair(new ConvertTrafficToLatLonP1());
		JavaPairRDD<String, ArrayList<String>> trafficP2Data = trafficLatLonData.mapToPair(new ConvertTrafficToLatLonP2());
		JavaPairRDD<String, ArrayList<String>> trafficData = trafficP1Data.union(trafficP2Data);
		*/
		
		//convert the parking to lat lon points
		JavaPairRDD<String, Tuple2<ArrayList<String>, ArrayList<String>>> parkingLatLonData = parkingRawData.join(parkingMetaData);
		parkingLatLonData.saveAsTextFile(args[5]);
		JavaPairRDD<String, ArrayList<String>> parkingConvertedData = parkingLatLonData.mapToPair(new ConvertTrafficToLatLonP1());
		JavaPairRDD<String, ArrayList<String>> trafficData = parkingConvertedData.union(trafficP2Data);

		// correlate the weather with the events
		JavaPairRDD<String, Tuple2<ArrayList<String>, ArrayList<String>>> correlatedTuples
		  = eventsData.join(weatherData);

		// correlate the weather with the parking
		JavaPairRDD<String, Tuple2<ArrayList<String>, ArrayList<String>>> correlatedTuples
		  = parkingData.join(pollutionData);

		// // reduce them to the keys we care about
		// JavaPairRDD<String, ArrayList<String>> correlated = correlatedTuples.mapToPair(new CombineAndRekey());
		// correlated.saveAsTextFile(args[5]+"C");
		
		// // combine duplicate keys
  //       JavaPairRDD<String, ArrayList<String>> reduced = correlated.reduceByKey(new ReduceDuplicateKeys());
		// reduced.saveAsTextFile(args[5]+"R");
		
		// // Average the pollition levels
		// JavaPairRDD<String, ArrayList<String>> averaged = reduced.mapToPair(new AveragePollutionLevels());
		// trafficData.saveAsTextFile(args[5]+"A");

		// end the session
		spark.stop();
	}
	
	/*
		A datastream with parking data provided from the city of Aarhus. 
		There are a total of 8 parking lots providing information over a 
		period of 6 months (55.264 data points in total).
	*/
	private static class AverageParkingLevels implements PairFunction<
	 Tuple2<String, ArrayList<String>>, String, ArrayList<String>> {
		public Tuple2<String, ArrayList<String>> call (Tuple2<String, ArrayList<String>> data) {
			/* Format (one-line):
				VEHICLECOUNT(0),UPDATETIME(1),_ID(2),TOTALSPACES(3),GARAGECODE(4),STREAMTIME(5)

			 	we want the VEHICLECOUNT(0) & GARAGECODE(4) for correlating 
			 	events & weather to parking
			*/

			// ********* BELOW NEEDS TO CHANGE ************ //

			//  data._2() 0-4 is the pollution data, 5 is the count
			
			//Sum the data
			// int lastIndex = data._2().get(0);
			// double count = Double.parseDouble(data._2().get(lastIndex));
			// ArrayList<String> averaged = new ArrayList<String>();

			// for (int i = 0; i < lastIndex; i++) {
			// 	averaged.add("" + Double.parseDouble(data._2().get(i)) / count);
			// }
			// System.out.println("average: " + averaged);
			
			// return new Tuple2<String, ArrayList<String>>(data._1(), averaged);
			return null;
		}
	}
	
	private static class ReduceDuplicateKeys implements Function2<ArrayList<String>, ArrayList<String>, ArrayList<String>> {
		public ArrayList<String> call (ArrayList<String> data1, ArrayList<String> data2) {
			// data._2() 0-4 is the pollution data, 5 is the count
			
			//Sum the data
			ArrayList<String> combined = new ArrayList<String>();
			for (int i = 0; i < data1.size(); i++) {
				combined.add("" + new BigInteger(data1.get(i)) + new BigInteger(data2.get(i)));
			}
			
			System.out.println("combined: " + combined);
			return combined;
		}
	}
	
	private static class CombineAndRekey implements PairFunction<Tuple2<String, Tuple2<ArrayList<String>, ArrayList<String>>>, String, ArrayList<String>> {
		public Tuple2<String, ArrayList<String>> call (Tuple2<String, Tuple2<ArrayList<String>, ArrayList<String>>> data) {
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

	private static class ConvertParkingToLatLon implements PairFunction<
	  Tuple2<String, Tuple2<ArrayList<String>, ArrayList<String>>>, String, ArrayList<String>> {
		public Tuple2<String, ArrayList<String>> call (Tuple2<String, 
		 Tuple2<ArrayList<String>, ArrayList<String>>> data) {
			//in data._2()._1() time stamp(0) and status(1)
			//in data._2()._2() p1 lat(0) and lon(1) and p2 lat(2) and lon(3)
			// determine the new key
			ArrayList<String> newData = new ArrayList<String>(data._2()._1());
			String newKey = data._2()._2().get(0) + "," + data._2()._2().get(1) + "," + newData.get(0);
			newData.remove(0);
			
			return new Tuple2<String, ArrayList<String>>(newKey, newData);
		}
	}

	private static class GetWeatherForCorrelationWithParking implements 
	 PairFunction<String, String, ArrayList<String>> {

		public Tuple2<String, ArrayList<String>> call(String row) { 
			/* Format (separate txt files):
				wspdm := wind speed in kilometers per hour
				wdird := wind direction in degrees
				tempm := temperature in degrees Celsius
				pressurem := pressure measured in mBar
				hum := humidity (percentage)
				dewptm := dew point in degrees Celsius

				Data format:
					{"KEY1:VALUE1","KEY2:VALUE2","KEY3:VALUE3",...,"KEYN:VALUEN"}
						where KEY is the time stamp and VALUE is the measurement

				we want the KEY,VALUE for correlating parking to weather
			*/

			// ********* BELOW NEEDS TO CHANGE ************ //
			//split the data on commas
			String[] keyValuePairs = row.split(",");

			Map<String, String> keyValues = new HashMap();
			for (String str : keyValuePairs) {
				String[] tempValue = str.split(":");

				keyValues.put(tempValue[0], tempValue[1]);
			}
			
			//get the data to correlate it
			String date = keyValues.forEach(() => {

			});
			
			//get the weather data
			ArrayList<String> data = new ArrayList<String>();
			data.add(fields[0]);
		
			//make sure all the data was good and return it
			return new Tuple2<String, ArrayList<String>>(date, data);
		}
	}

	private static class GetParkingForCorrelationWithWeather implements 
	 PairFunction<String, String, ArrayList<String>> {
		public Tuple2<String, ArrayList<String>> call(String row) { 
			/* Format (one-line):
				VEHICLECOUNT(0),UPDATETIME(1),_ID(2),TOTALSPACES(3),
				GARAGECODE(4),STREAMTIME(5)

			 	we want the VEHICLECOUNT(0) & GARAGECODE(4) for correlating 
			 	events & weather to parking
			*/
			
			//split the data on commas
			String[] fields = row.split(",");
			//get the id that this point corresponds to
			String key = fields[4];
			
			//get the traffic lat and long associated with the id
			ArrayList<String> data = new ArrayList<String>();
			data.add(fields[0]);
			
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

  private static class GetLibraryEventForCorrelationWithPollution implements 
   PairFunction<String, String, ArrayList<String>> {
		public Tuple2<String, ArrayList<String>> call(String row) { 
			/* Format (one-line):
				** Library Event **
				LID(0),CITY(1),ENDTIME(2),TITLE(3),URL(4),PRICE(5),CHANGED(6),
				CONTENT(7),ZIPCODE(8),LIBRARY(9),IMAGEURL(10),TEASER(11),
				STREET(12),STATUS(13),LONGITUDE(14),STARTTIME(15),
				LATITUDE(16),_ID(17),ID(18),STREAMTIME(19)

			 	we want the ID(18) and ENDTIME(2) for correlating events to pollution
			*/

			//split the data on commas
			String[] fields = row.split(",");
			//get the library event id that this point corresponds to
			String libraryKey = fields[18];
			
			//get the endtime associated with the id
			ArrayList<String> dateTime = new ArrayList<String>();
			dateTime.add(fields[2]);
		
			return new Tuple2<String, ArrayList<String>>(key, dateTime);
		}
	}
}
