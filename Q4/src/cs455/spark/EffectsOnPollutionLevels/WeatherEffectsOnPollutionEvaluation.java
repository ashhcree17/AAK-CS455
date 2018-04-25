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

		// read in the files (if passed a dir it will read all files in it)
		//JavaRDD<String> pollutionLines = spark.read().textFile(args[0]).javaRDD();
		JavaRDD<String> weatherTempLines = spark.read().textFile(args[1]+"/*/tempm.txt").javaRDD();
		JavaRDD<String> weatherDewLines = spark.read().textFile(args[1]+"/*/dewptm.txt").javaRDD();
		JavaRDD<String> weatherWindLines = spark.read().textFile(args[1]+"/*/wspdm.txt").javaRDD();
	
		// get the needed values out of the lines
		//JavaPairRDD<String, ArrayList<String>> pollutionData = pollutionLines.mapToPair(new GetPollutionForCorrelationWithTraffic());
		JavaRDD<ArrayList<String>> weatherTempData = weatherTempLines.map(new GetWeatherData());
		JavaRDD<ArrayList<String>> weatherDewData = weatherDewLines.map(new GetWeatherData());
		JavaRDD<ArrayList<String>> weatherWindData = weatherWindLines.map(new GetWeatherData());
		weatherTempData.saveAsTextFile(args[2]+"1");
		
		//flatten the data so each entry is one time stamp
		JavaRDD<String> weatherTempFlatData = weatherTempData.flatMap(new ExpandRawWeatherData());
		JavaRDD<String> weatherDewFlatData = weatherDewData.flatMap(new ExpandRawWeatherData());
		JavaRDD<String> weatherWindFlatData = weatherWindData.flatMap(new ExpandRawWeatherData());
		weatherTempFlatData.saveAsTextFile(args[2]+"2");
		
		//rekey them based on the timestamp
		JavaPairRDD<String, String> weatherTempKeyedData = weatherTempFlatData.mapToPair(new KeyWeatherData());
		JavaPairRDD<String, String> weatherDewKeyedData = weatherDewFlatData.mapToPair(new KeyWeatherData());
		JavaPairRDD<String, String> weatherWindKeyedData = weatherWindFlatData.mapToPair(new KeyWeatherData());
		weatherTempKeyedData.saveAsTextFile(args[2]+"3");
		
		//finally combine all the weather data into one RDD
		JavaPairRDD<String, Tuple2<String, String>> weatherDewAndWindRawData = weatherDewKeyedData.join(weatherWindKeyedData);
		JavaPairRDD<String, Tuple2<String, Tuple2<String, String>>> weatherAllRawData = weatherTempKeyedData.join(weatherDewAndWindRawData);
		JavaPairRDD<String, ArrayList<String>> weatherAllData = weatherAllRawData.mapToPair(new CombineWeatherData());
		weatherDewAndWindRawData.saveAsTextFile(args[2]+"4");
		weatherAllRawData.saveAsTextFile(args[2]+"5");
		weatherAllData.saveAsTextFile(args[2]);

		// end the session
		spark.stop();
	}
		
	private static class CombineWeatherData implements PairFunction<Tuple2<String, Tuple2<String, Tuple2<String, String>>>, String, ArrayList<String>>
	{
		public Tuple2<String, ArrayList<String>> call (Tuple2<String, Tuple2<String, Tuple2<String, String>>> data)
		{
			//each layer has one weather value in the order temp, dew, wind
			ArrayList<String> newData = new ArrayList<String>();
			newData.add(data._2()._1());
			newData.add(data._2()._2()._1());
			newData.add(data._2()._2()._2());
			
			return new Tuple2<String, ArrayList<String>>(data._1(), newData);
		}
	}
	
		
	private static class KeyWeatherData implements PairFunction<String, String, String>
	{
		public Tuple2<String, String> call (String data)
		{
			// data has format:
			// timestamp: value
			
			// split the timestamp into the key
			int index = data.lastIndexOf(":");
			return new Tuple2<String, String>(data.substring(0, index).trim(), data.substring(index + 1).trim());
		}
	}
		
  	private static class ExpandRawWeatherData implements FlatMapFunction<ArrayList<String>, String>
	{
		public Iterator<String> call (ArrayList<String> data)
		{			
			//flatten the data into the timestamp as keys
			return data.iterator();
		}
	}
	
  	private static class GetWeatherData implements Function<String, ArrayList<String>> 
	{
		public ArrayList<String> call(String row) 
		{ 
			//Format:			
			//{"TimeStamp": "value",... (times x)}
			//note that each row is one day
			
			//remove the braces and quotes
			String line = row.replace("\"", "").replace("{", "").replace("}", "");
			
			//get all the pairs
			String[] pairs = line.split(",");
			
			//parse the value from each pair only keeping the on the hour measurements
			ArrayList<String> onTheHourData = new ArrayList<String>();
			for(int i = 0; i < pairs.length; i++)
			{
				//get the hour
				String[] entry = pairs[i].split(":");
				if(entry[1].trim().equals("00"))
				{
					onTheHourData.add(pairs[i]);
				}
			}
		
			return onTheHourData;
		}
	}
}
