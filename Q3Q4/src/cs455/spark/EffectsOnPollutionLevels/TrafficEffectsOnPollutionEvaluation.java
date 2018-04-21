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
		
		// reduce them to the keys we care about
		JavaPairRDD<String, ArrayList<String>> correlated = correlatedTuples.mapToPair(new CombineAndRekey());
		
		// combine duplicate keys
        JavaPairRDD<String, ArrayList<String>> reduced = correlated.reduceByKey(new ReduceDuplicateKeys());

		// Do some analysis if needed
		
		// and output it (just a couple lines to see if everything above is woring as expected)
		List<Tuple2<String, ArrayList<String>>> output = reduced.collect();
		for (Tuple2<String, ArrayList<String>> tuple : output) 
		{
			System.out.println(tuple._1() + ": " + Arrays.toString(tuple._2().toArray()));
		}

		// end the session
		spark.stop();
	}
	
	private class ReduceDuplicateKeys implements Function2<ArrayList<String>, ArrayList<String>, ArrayList<String>>
	{
		public ArrayList<String> call (ArrayList<String> data1, ArrayList<String> data2)
		{
			ArrayList<String> combined = new ArrayList<String>(data1);
			
			//combine the data in a logical way as needed (TBD actual columns)
			combined.set(1, data1.get(1) + data2.get(2));
			combined.set(2, "" + (Integer.parseInt(data1.get(2)) + Integer.parseInt(data2.get(2))));
			
			//combine the counters
			int lastIndex = combined.size() - 1;
			combined.set(lastIndex, "" + (Integer.parseInt(data1.get(lastIndex)) + Integer.parseInt(data2.get(lastIndex))));
			
			return combined;
		}
	}
	
	private class CombineAndRekey implements PairFunction<Tuple2<String, Tuple2<ArrayList<String>, ArrayList<String>>>, String, ArrayList<String>>
	{
		public Tuple2<String, ArrayList<String>> call (Tuple2<String, Tuple2<ArrayList<String>, ArrayList<String>>> data)
		{
			// determine the new key (row TBD)
			ArrayList<String> newData = new ArrayList<String>(data._2()._1());
			String newKey = newData.get(1);
			newData.remove(1);
			
			// combine the rest of the data
			newData.addAll(data._2()._2());
			
			//add a counter for averaging
			newData.add("1"); 
			
			return new Tuple2<String, ArrayList<String>>(newKey, newData);
		}
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
