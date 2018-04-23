package cs455.spark.EffectsOnPollutionLevels;

import scala.Tuple2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;

public final class TrafficEffectsOnPollutionEvaluation
{
	public static void main(String[] args) throws Exception
	{
		// The source file is the first arguement
		if (args.length < 4) {
			System.err.println("Usage: TrafficEffectsOnPollution <PollutionDir> <TrafficDir> <TrafficMetaDataDir> <outputDir>");
			System.exit(1);
		}
		
		// create a spark session
		SparkSession spark = SparkSession
			.builder()
			.appName("TrafficEffectsOnPollution")
			.getOrCreate();

		// read in the files (if passed a dir it will read all files in it)
		JavaRDD<String> pollutionLines = spark.read().textFile(args[0]).javaRDD();
		JavaRDD<String> trafficLines = spark.read().textFile(args[1]).javaRDD();
		JavaRDD<String> trafficMetaLines = spark.read().textFile(args[2]).javaRDD();
	
		// get the needed values out of the lines
		JavaPairRDD<String, ArrayList<String>> pollutionData = pollutionLines.mapToPair(new GetPollutionForCorrelationWithTraffic());
		JavaPairRDD<String, ArrayList<String>> trafficRawData = trafficLines.mapToPair(new GetTrafficForCorrelationWithPollution());
		JavaPairRDD<String, ArrayList<String>> trafficMetaData = trafficMetaLines.mapToPair(new GetTrafficMetaDataForCorrelationWithPollution());
		pollutionData.saveAsTextFile(args[3]+"2");
		
		
		//JavaPairRDD<String, ArrayList<String>> pollutionDataOnly = pollutionData.filter(new RemoveText());
		
		//convert the traffic to lat lon points
		JavaPairRDD<String, Tuple2<ArrayList<String>, ArrayList<String>>> trafficLatLonData = trafficRawData.join(trafficMetaData);
		trafficLatLonData.saveAsTextFile(args[3]);
		JavaPairRDD<String, ArrayList<String>> trafficP1Data = trafficLatLonData.mapToPair(new ConvertTrafficToLatLonP1());
		JavaPairRDD<String, ArrayList<String>> trafficP2Data = trafficLatLonData.mapToPair(new ConvertTrafficToLatLonP2());
		JavaPairRDD<String, ArrayList<String>> trafficData = trafficP1Data.union(trafficP2Data);
		trafficData.saveAsTextFile(args[3]+"3");
		
		// correlate the pollution with the traffic
		JavaPairRDD<String, Tuple2<ArrayList<String>, ArrayList<String>>> correlatedTuples = trafficData.join(pollutionData);
		correlatedTuples.saveAsTextFile(args[3]+"T");
		
		// reduce them to the keys we care about
		JavaPairRDD<String, ArrayList<String>> correlated = correlatedTuples.mapToPair(new CombineAndRekey());
		correlated.saveAsTextFile(args[3]+"C");
		
		// combine duplicate keys
        JavaPairRDD<String, ArrayList<String>> reduced = correlated.reduceByKey(new ReduceDuplicateKeys());
		reduced.saveAsTextFile(args[3]+"R");
		
		// Average the pollition levels
		JavaPairRDD<String, ArrayList<String>> averaged = reduced.mapToPair(new AveragePollutionLevels());
		trafficData.saveAsTextFile(args[3]+"A");

		// end the session
		spark.stop();
	}
	
	/*private static class RemoveText implements Function2<String, ArrayList<String>, bool>
	{
		public bool call (String key, ArrayList<String> data)
		{
			try
			{
				Integer.parseInt(data.get(0));
				return true;
			}
			catch (NullPointerException nfe)
			{
				return false;
			}
		}
	}*/
	
	private static class AveragePollutionLevels implements PairFunction<Tuple2<String, ArrayList<String>>, String, ArrayList<String>>
	{
		public Tuple2<String, ArrayList<String>> call (Tuple2<String, ArrayList<String>> data)
		{
			// data._2() 0-4 is the pollution data, 5 is the count
			
			//Sum the data
			int lastIndex = data._2().size() - 1;
			double count = Integer.parseInt(data._2().get(lastIndex));
			ArrayList<String> averaged = new ArrayList<String>();
			for (int i = 0; i < lastIndex; i++)
			{
				averaged.set(i, "" + Integer.parseInt(data._2().get(i)) / count);
			}
			
			return new Tuple2<String, ArrayList<String>>(data._1(), averaged);
		}
	}
	
	private static class ReduceDuplicateKeys implements Function2<ArrayList<String>, ArrayList<String>, ArrayList<String>>
	{
		public ArrayList<String> call (ArrayList<String> data1, ArrayList<String> data2)
		{
			// data._2() 0-4 is the pollution data, 5 is the count
			if(data1.size() < 5)
			{
				System.err.println("d1: " + data1.size() + " " + Arrays.toString(data1.toArray()));
				if(data2.size() < 5)
				{
					System.err.println("d2: " + data2.size() + " " + Arrays.toString(data2.toArray()));
				}
				return data1;
			}
			if(data2.size() < 5)
			{
				System.err.println("d2: " + data2.size() + " " + Arrays.toString(data2.toArray()));
				return data1;
			}
			
			//Sum the data
			ArrayList<String> combined = new ArrayList<String>();
			for (int i = 0; i < data1.size(); i++)
			{
				combined.set(i, "" + Integer.parseInt(data1.get(i)) + Integer.parseInt(data2.get(i)));
			}
			
			return combined;
		}
	}
	
	private static class CombineAndRekey implements PairFunction<Tuple2<String, Tuple2<ArrayList<String>, ArrayList<String>>>, String, ArrayList<String>>
	{
		public Tuple2<String, ArrayList<String>> call (Tuple2<String, Tuple2<ArrayList<String>, ArrayList<String>>> data)
		{
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
	
	private static class ConvertTrafficToLatLonP1 implements PairFunction<Tuple2<String, Tuple2<ArrayList<String>, ArrayList<String>>>, String, ArrayList<String>>
	{
		public Tuple2<String, ArrayList<String>> call (Tuple2<String, Tuple2<ArrayList<String>, ArrayList<String>>> data)
		{
			//in data._2()._1() time stamp(0) and status(1)
			//in data._2()._2() p1 lat(0) and lon(1) and p2 lat(2) and lon(3)
			// determine the new key
			ArrayList<String> newData = new ArrayList<String>(data._2()._1());
			String newKey = data._2()._2().get(0) + "," + data._2()._2().get(1) + "," + newData.get(0);
			newData.remove(0);
			
			return new Tuple2<String, ArrayList<String>>(newKey, newData);
		}
	}
	
	private static class ConvertTrafficToLatLonP2 implements PairFunction<Tuple2<String, Tuple2<ArrayList<String>, ArrayList<String>>>, String, ArrayList<String>>
	{
		public Tuple2<String, ArrayList<String>> call (Tuple2<String, Tuple2<ArrayList<String>, ArrayList<String>>> data)
		{
			//in data._1() time stamp(0) and status(1)
			//in data._2() p1 lat(0) and lon(1) and p2 lat(2) and lon(3)
			// determine the new key
			ArrayList<String> newData = new ArrayList<String>(data._2()._1());
			String newKey = data._2()._2().get(2) + "," + data._2()._2().get(3) + "," + newData.get(0);
			newData.remove(0);
			
			return new Tuple2<String, ArrayList<String>>(newKey, newData);
		}
	}
	
	private static class GetPollutionForCorrelationWithTraffic implements PairFunction<String, String, ArrayList<String>> 
	{
		public Tuple2<String, ArrayList<String>> call(String row) 
		{ 
			//Format:
			//ozone,particullate_matter,carbon_monoxide,sulfure_dioxide,nitrogen_dioxide,longitude,latitude,timestamp
			
			//we want the timestamp(7), lat(6) and lon(5) for correlating traffic to pollution and the different levels for analysis
			
			//split the data on commas
			String[] fields = row.split(",");
			
			//get the data and location or however we want to correlate it
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
	
  	private static class GetTrafficMetaDataForCorrelationWithPollution implements PairFunction<String, String, ArrayList<String>> 
	{
		public Tuple2<String, ArrayList<String>> call(String row) 
		{ 
			//Format (one-line):			
			//POINT_1_STREET,DURATION_IN_SEC,POINT_1_NAME,POINT_1_CITY,POINT_2_NAME,POINT_2_LNG,POINT_2_STREET,NDT_IN_KMH,
			//POINT_2_POSTAL_CODE,POINT_2_COUNTRY,POINT_1_STREET_NUMBER,ORGANISATION,POINT_1_LAT,POINT_2_LAT,POINT_1_POSTAL_CODE,
			//POINT_2_STREET_NUMBER,POINT_2_CITY,extID,ROAD_TYPE,POINT_1_LNG,REPORT_ID,POINT_1_COUNTRY,DISTANCE_IN_METERS,REPORT_NAME,RBA_ID,_id
			
			//we want the REPORT_ID(20) p1 lat(12) and lon(19) and p2 lat(13) and lon(5) for correlating traffic to pollution
			
			//split the data on commas
			String[] fields = row.split(",");
			//get the id that this point corresponds to
			String key = fields[20];
			
			//get the traffic lat and long associated with the id
			ArrayList<String> location = new ArrayList<String>();
			location.add(fields[12]);
			location.add(fields[19]);
			location.add(fields[13]);
			location.add(fields[5]);
		
			return new Tuple2<String, ArrayList<String>>(key, location);
		}
	}
	
	private static class GetTrafficForCorrelationWithPollution implements PairFunction<String, String, ArrayList<String>> 
	{
		public Tuple2<String, ArrayList<String>> call(String row) 
		{ 
			//Format:
			//status,avgMeasuredTime,avgSpeed,extID,medianMeasuredTime,TIMESTAMP,vehicleCount,_id,REPORT_ID
			
			//we want the REPORT_ID(8) and time stamp(5) for correlating traffic to pollution and the status(0) for determining if it affect traffic
			
			//split the data on commas
			String[] fields = row.split(",");
			
			//get the data and location or however we want to correlate it
			//put the timestamp in here for now until after we map the traffic to locations
			//replace the T with a " " to make it match the pollution timestamp
			String key = fields[8];
			
			//get the traffic data 
			ArrayList<String> data = new ArrayList<String>();
			data.add(fields[5].replace('T', ' '));
			data.add(fields[0]);
		
			//make sure all the data was good and return it
			return new Tuple2<String, ArrayList<String>>(key, data);
		}
	}
}
