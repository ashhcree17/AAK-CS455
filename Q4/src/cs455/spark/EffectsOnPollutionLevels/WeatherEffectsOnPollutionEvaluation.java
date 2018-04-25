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

import cs455.spark.WeatherParser.WeatherParser;

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
		weatherData.saveAsTextFile(args[2]);
		
		// end the session
		spark.stop();
	}
}
