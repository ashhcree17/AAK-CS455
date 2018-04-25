package cs455.spark.EffectsOnPollutionLevels;

import scala.Tuple2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.math.BigInteger;
import java.util.Iterator;

import cs455.spark.Utils.Helper;
import cs455.spark.Utils.WeatherParser;

public final class WeatherEffectsOnPollutionEvaluation
{
	public static void main(String[] args) throws Exception
	{
		// The source file is the first arguement
		if (args.length < 3) {
			System.err.println("Usage: WeatherEffectsOnPollution <PollutionDir> <WeatherFilesDir> <outputDir>");
			System.exit(1);
		}
		
		// create a spark session
		SparkSession spark = SparkSession
			.builder()
			.appName("WeatherEffectsOnPollution")
			.getOrCreate();

		//parse the weather
		JavaPairRDD<String, ArrayList<String>> weatherData = WeatherParser.parseWeather(args[1], spark);
		
		//Get the on the hour pollution levels
		JavaRDD<String> pollutionLines = spark.read().textFile(args[0]).javaRDD();
		JavaPairRDD<String, ArrayList<String>> pollutionData = pollutionLines.mapToPair(new GetPollutionForCorrelationWithWeather());
		JavaPairRDD<String, ArrayList<String>> pollutionHourData = pollutionData.filter(new FilterOnTheHour());
		
		//correlate the pollution and weather
		JavaPairRDD<String, Tuple2<ArrayList<String>, ArrayList<String>>> correlatedTuples = weatherData.join(pollutionHourData);
		
		// Change them to the keys we care about
		JavaPairRDD<String, ArrayList<String>> correlated = correlatedTuples.mapToPair(new CombineAndRekey());
		
		//average the pollution reading on the weather keys
		JavaPairRDD<String, ArrayList<String>> weatherTotals = correlated.reduceByKey(new Helper.ReduceDuplicateKeys());
		JavaPairRDD<String, ArrayList<String>> weatherAvgs = weatherTotals.mapToPair(new Helper.AverageData());
		weatherAvgs.saveAsTextFile(args[2]+"_avgs");
		
		//determine the min and max
		JavaPairRDD<String, ArrayList<String>> weatherMins = correlated.reduceByKey(new Helper.MinOfData());
		JavaPairRDD<String, ArrayList<String>> weatherMinsNoCount = weatherMins.mapToPair(new Helper.StripCountOff());
		JavaPairRDD<String, ArrayList<String>> weatherMaxs = correlated.reduceByKey(new Helper.MaxOfData());
		JavaPairRDD<String, ArrayList<String>> weatherMaxsNoCount = weatherMaxs.mapToPair(new Helper.StripCountOff());
		weatherMinsNoCount.saveAsTextFile(args[2]+"_mins");
		weatherMaxsNoCount.saveAsTextFile(args[2]+"_maxs");
		
		//determine the std dev
		JavaPairRDD<String, Tuple2<ArrayList<String>, ArrayList<String>>> weatherWithAvgs = weatherAvgs.join(correlated);
		JavaPairRDD<String, ArrayList<String>> weatherPrepedForStdDev = weatherWithAvgs.mapToPair(new Helper.PrepForStdDevOfData());
		JavaPairRDD<String, ArrayList<String>> weatherPrepedForStdDevReduced = weatherPrepedForStdDev.reduceByKey(new Helper.ReduceDuplicateKeysDoubles());
		JavaPairRDD<String, ArrayList<String>> weatherStdDevs = weatherPrepedForStdDevReduced.mapToPair(new Helper.StdDevOfData());
		weatherStdDevs.saveAsTextFile(args[2]+"_stddevs");
		
		
		// end the session
		spark.stop();
	}
		
	private static class CombineAndRekey implements PairFunction<Tuple2<String, Tuple2<ArrayList<String>, ArrayList<String>>>, String, ArrayList<String>>
	{
		public Tuple2<String, ArrayList<String>> call (Tuple2<String, Tuple2<ArrayList<String>, ArrayList<String>>> data)
		{
			// data._2()._1() is the weather data
			// data._2()._2() 0-5 is the pollution data and count
			
			// The key is the weather data
			String newKey = data._2()._1().get(0) + "," + data._2()._1().get(1) + "," + data._2()._1().get(2);
			
			//the data is the pollution data
			return new Tuple2<String, ArrayList<String>>(newKey, new ArrayList<String>(data._2()._2()));
		}
	}
	
	private static class FilterOnTheHour implements Function<Tuple2<String,ArrayList<String>>,Boolean> 
	{
		public Boolean call(Tuple2<String, ArrayList<String>> data) 
		{
			String[] timeSplit = data._1().split("T");
			try
			{
				if (timeSplit[1].trim().endsWith("00:00"))
				{
					return true;
				}
			}
			//means its not a data row if it couldn't split on T
			catch (IndexOutOfBoundsException oibe) {}
			
			return false;	
		}
	}
	
	private static class GetPollutionForCorrelationWithWeather implements PairFunction<String, String, ArrayList<String>> 
	{
		public Tuple2<String, ArrayList<String>> call(String row) 
		{ 
			//Format:
			//ozone,particullate_matter,carbon_monoxide,sulfure_dioxide,nitrogen_dioxide,longitude,latitude,timestamp
			
			//we want the timestamp(7) for correlating weather to pollution and the different levels for analysis
			
			//split the data on commas
			String[] fields = row.split(",");
			
			//get the data and location or however we want to correlate it (change timestamp to have T to match weather)
			String date = fields[7].replace(' ', 'T');
			
			//get the polution data
			ArrayList<String> data = new ArrayList<String>();
			data.add(fields[0]);
			data.add(fields[1]);
			data.add(fields[2]);
			data.add(fields[3]);
			data.add(fields[4]);
			data.add("1"); //a count
		
			//make sure all the data was good and return it
			return new Tuple2<String, ArrayList<String>>(date, data);
		}
	}
}
