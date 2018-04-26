package cs455.spark.Utils;

import scala.Tuple2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.util.Iterator;

public final class WeatherParser
{
	public static JavaPairRDD parseWeather(String weatherDir, SparkSession spark) throws Exception
	{		
	    //create the jsc
		SparkContext sc = spark.sparkContext();
		JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sc);
		
		// read in the files (if passed a dir it will read all files in it)
		JavaRDD<String> weatherTempLines = spark.read().textFile(weatherDir+"/*/tempm.txt").javaRDD();
		JavaRDD<String> weatherPressLines = spark.read().textFile(weatherDir+"/*/pressurem.txt").javaRDD();
		JavaRDD<String> weatherWindLines = spark.read().textFile(weatherDir+"/*/wspdm.txt").javaRDD();
	
		// get the needed values out of the lines
		JavaRDD<ArrayList<String>> weatherTempData = weatherTempLines.map(new GetWeatherData());
		JavaRDD<ArrayList<String>> weatherPressData = weatherPressLines.map(new GetWeatherData());
		JavaRDD<ArrayList<String>> weatherWindData = weatherWindLines.map(new GetWeatherData());
		
		//flatten the data so each entry is one time stamp
		JavaRDD<String> weatherTempFlatData = weatherTempData.flatMap(new ExpandRawWeatherData());
		JavaRDD<String> weatherPressFlatData = weatherPressData.flatMap(new ExpandRawWeatherData());
		JavaRDD<String> weatherWindFlatData = weatherWindData.flatMap(new ExpandRawWeatherData());
		
		//rekey them based on the timestamp
		JavaPairRDD<String, String> weatherTempKeyedData = weatherTempFlatData.mapToPair(new KeyWeatherData());
		JavaPairRDD<String, String> weatherPressKeyedData = weatherPressFlatData.mapToPair(new KeyWeatherData());
		JavaPairRDD<String, String> weatherWindKeyedData = weatherWindFlatData.mapToPair(new KeyWeatherData());
		
		//finally combine all the weather data into one RDD
		JavaPairRDD<String, Tuple2<String, String>> weatherPressAndWindRawData = weatherPressKeyedData.join(weatherWindKeyedData);
		JavaPairRDD<String, Tuple2<String, Tuple2<String, String>>> weatherAllRawData = weatherTempKeyedData.join(weatherPressAndWindRawData);
		JavaPairRDD<String, ArrayList<String>> weatherAllData = weatherAllRawData.mapToPair(new CombineWeatherData());
		
		//now convert the values to flags
		//For temperature we want to base this on the average temperature
		//for wind we want to base it on the average wind
		//for rain we want to correlate temp and dew point
		JavaRDD<ArrayList<String>> weatherAllDataValues = weatherAllData.values();
		
		ArrayList<String> weatherTotals = weatherAllDataValues.reduce(new TotalWeatherValues());
		ArrayList<String> weatherAvgs = AverageWeatherData(weatherTotals);
		List<ArrayList<String>> rddPrep = Arrays.asList(weatherAvgs);
		
		//convert it back to an RDD
		JavaRDD<ArrayList<String>> weatherAvgsRdd = jsc.parallelize(rddPrep);
		JavaPairRDD<String, ArrayList<String>> weatherAvgsRddDummyKey = weatherAvgsRdd.mapToPair(v -> new Tuple2<String, ArrayList<String>>("dummykey", v));
		
		//now associate the averages and determine the flags
		JavaPairRDD<String, Tuple2<String, ArrayList<String>>> weatherAllDataDumyKey = weatherAllData.mapToPair(v -> new Tuple2<String, Tuple2<String, ArrayList<String>>>("dummykey", v));
		JavaPairRDD<String, Tuple2<ArrayList<String>, Tuple2<String, ArrayList<String>>>> weatherAvgCombined = weatherAvgsRddDummyKey.join(weatherAllDataDumyKey);
		JavaPairRDD<String, ArrayList<String>> weatherAllFlagData = weatherAvgCombined.mapToPair(new MakeWeatherDataFlags());
		
		return weatherAllFlagData;
	}
		
	private static class MakeWeatherDataFlags implements PairFunction<Tuple2<String, Tuple2<ArrayList<String>, Tuple2<String, ArrayList<String>>>>, String, ArrayList<String>>
	{
		public Tuple2<String, ArrayList<String>> call (Tuple2<String, Tuple2<ArrayList<String>, Tuple2<String, ArrayList<String>>>> data)
		{
			//data._2()._1() is the averages
			//data._2()._2()._2() is the current measurments
			//data._2()._2()._1() is the key
			ArrayList<String> newData = new ArrayList<String>();
			
			//for temperature just toggle high on and off based on the average
			if (Double.parseDouble(data._2()._2()._2().get(0)) > Double.parseDouble(data._2()._1().get(0)))
			{
				newData.add("HIGH_TEMPERATURE");
			}
			else
			{
				newData.add("LOW_TEMPERATURE");
			}
			
			//for dewpoint check if its higher than the temperature
			if (Double.parseDouble(data._2()._2()._2().get(1)) > Double.parseDouble(data._2()._1().get(1)))
			{
				newData.add("HIGH_PRESSURE");
			}
			else
			{
				newData.add("LOW_PRESSURE");
			}
			
			//for wind just toggle high on and off based on the average
			if (Double.parseDouble(data._2()._2()._2().get(2)) > Double.parseDouble(data._2()._1().get(2)))
			{
				newData.add("HIGH_WIND");
			}
			else
			{
				newData.add("LOW_WIND");
			}
			
			return new Tuple2<String, ArrayList<String>>(data._2()._2()._1(), newData);
		}
	}
	
	private static ArrayList<String> AverageWeatherData (ArrayList<String> data)
	{
		// data 0-2 is the pollution data, 3 is the count
		
		//Sum the data
		ArrayList<String> averaged = new ArrayList<String>();
		int lastIndex = data.size() - 1;
		double count = Double.parseDouble(data.get(lastIndex));
		for (int i = 0; i < lastIndex; i++)
		{
			averaged.add("" + Double.parseDouble(data.get(i)) / count);
		}
		return averaged;
	}
	
	private static class TotalWeatherValues implements Function2<ArrayList<String>, ArrayList<String>, ArrayList<String>>
	{
		public ArrayList<String> call (ArrayList<String> data1, ArrayList<String> data2)
		{
			// data1 and data2 0-2 is the pollution data, 3 is the count

			//Sum the data
			ArrayList<String> combined = new ArrayList<String>();
			for (int i = 0; i < data1.size(); i++)
			{
				combined.add("" + (Double.parseDouble(data1.get(i)) + Double.parseDouble(data2.get(i))));
			}
			return combined;
		}
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
			newData.add("1"); //add a count
			
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
		    ArrayList<String> flattened = new ArrayList<String>(data);
			//flatten the data into the timestamp as keys
			for (int i = 0; i < flattened.size(); i++)
			{
				//mae sure it is a valid number
				try
				{
					Double.parseDouble(flattened.get(i).split(":")[3]);
				}
				catch (NumberFormatException nfe)
				{
					flattened.remove(i);
					i--;
				}
			}
			return flattened.iterator();
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
