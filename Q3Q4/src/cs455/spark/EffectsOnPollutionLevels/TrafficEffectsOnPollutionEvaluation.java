/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cs455.spark.EffectsOnPollutionLevels;

import scala.Tuple2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;

public class TrafficEffectsOnPollutionEvaluation
{
	private static final int outputCount = 20;  
	public TrafficEffectsOnPollutionEvaluation(String[] args)
	{
		// create a spark session
		SparkSession spark = SparkSession
			.builder()
			.appName("TrafficEffectsOnPollution")
			.getOrCreate();

		// read in the files (if passed a dir it will read all files in it)
		JavaRDD<String> pollutionLines = spark.read().textFile(args[0]).javaRDD();
		JavaRDD<String> trafficLines = spark.read().textFile(args[1]).javaRDD();
	
		// get the needed values out of the lines
		JavaPairRDD<String, ArrayList<String>> pollutionData = pollutionLines.mapToPair(new GetPollutionForCorrelationWithTraffic());
		JavaPairRDD<String, ArrayList<String>> trafficData = trafficLines.mapToPair(new GetTrafficForCorrelationWithPollution());

		// correlate the pollution with the traffic
		JavaPairRDD<String, Tuple2<ArrayList<String>, ArrayList<String>>> correlatedTuples = trafficData.join(pollutionData);
		JavaPairRDD<String, ArrayList<String>> correlated = correlatedTuples.mapToPair(
				tuple -> {
					ArrayList<String> combined = new ArrayList<String>(tuple._2()._1());
					combined.addAll(tuple._2()._1());
					return new Tuple2<String, ArrayList<String>>(tuple._1(), combined);
				});

		// Do some sort of analysis (TBD)

		// and output it (just a couple lines to see if everything above is woring as expected)
		List<Tuple2<String, ArrayList<String>>> output = correlated.collect();
		for (Tuple2<String, ArrayList<String>> tuple : output) 
		{
			System.out.println(tuple._1() + ": " + Arrays.toString(tuple._2().toArray()));
		}

		// end the session
		spark.stop();
	}

	private class GetPollutionForCorrelationWithTraffic implements PairFunction<String, String, ArrayList<String>> 
	{
		public Tuple2<String, ArrayList<String>> call(String row) 
		{ 
			//split the data on commas
			String[] fields = row.split(",");
			//get the data and location or however we want to correlate it (TBD columns)
			String dateLoc = fields[0] + "," + fields[1];
			//get the polution data (TBD columns)
			ArrayList<String> data = new ArrayList<String>();
			data.add(fields[0]);
			data.add(fields[1]);
			data.add(fields[2]);
		
			//make sure all the data was good and return it
			return new Tuple2<String, ArrayList<String>>(dateLoc, data);
		}
	}
  
	private class GetTrafficForCorrelationWithPollution implements PairFunction<String, String, ArrayList<String>> 
	{
		public Tuple2<String, ArrayList<String>> call(String row) 
		{ 
			//split the data on commas
			String[] fields = row.split(",");
			//get the data and location or however we want to correlate it (TBD columns)
			String dateLoc = fields[0] + "," + fields[1];
			//get the traffic data (TBD columns)
			ArrayList<String> data = new ArrayList<String>();
			data.add(fields[0]);
			data.add(fields[1]);
			data.add(fields[2]);
		
			//make sure all the data was good and return it
			return new Tuple2<String, ArrayList<String>>(dateLoc, data);
		}
	}
}
