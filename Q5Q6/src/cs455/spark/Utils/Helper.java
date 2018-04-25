package cs455.spark.Utils;

import scala.Tuple2;

import org.apache.spark.api.java.function.*;

import java.math.BigInteger;
import java.util.ArrayList;

public final class Helper
{	
	public static class AverageData implements PairFunction<Tuple2<String, ArrayList<String>>, String, ArrayList<String>>
	{
		public Tuple2<String, ArrayList<String>> call (Tuple2<String, ArrayList<String>> data)
		{
			// data._2() 0-(n-1) is the pollution data, n-1 is the count
			
			//Sum the data
			ArrayList<String> averaged = new ArrayList<String>();
			int lastIndex = data._2().size() - 1;
			double count = Double.parseDouble(data._2().get(lastIndex));
			for (int i = 0; i < lastIndex; i++)
			{
				averaged.add("" + Double.parseDouble(data._2().get(i)) / count);
			}
			return new Tuple2<String, ArrayList<String>>(data._1(), averaged);
		}
	}
		
	public static class MinOfData implements Function2<ArrayList<String>, ArrayList<String>, ArrayList<String>>
	{
		public ArrayList<String> call (ArrayList<String> data1, ArrayList<String> data2)
		{
			// data._2() 0-(n-1) is the pollution data, n-1 is the count
			
			//keep only the lowest the data
			ArrayList<String> min = new ArrayList<String>();
			for (int i = 0; i < data1.size(); i++)
			{
				if (Double.parseDouble(data1.get(i)) < Double.parseDouble(data2.get(i)))
				{
					min.add(data1.get(i));
				}
				else
				{
					min.add(data2.get(i));
				}
			}
			return min;
		}
	}
	
	public static class MaxOfData implements Function2<ArrayList<String>, ArrayList<String>, ArrayList<String>>
	{
		public ArrayList<String> call (ArrayList<String> data1, ArrayList<String> data2)
		{
			// data._2() 0-(n-1) is the pollution data, n-1 is the count
			
			//keep only the highest the data
			ArrayList<String> max = new ArrayList<String>();
			for (int i = 0; i < data1.size(); i++)
			{
				if (Double.parseDouble(data1.get(i)) > Double.parseDouble(data2.get(i)))
				{
					max.add(data1.get(i));
				}
				else
				{
					max.add(data2.get(i));
				}
			}
			return max;
		}
	}
	
	public static class PrepForStdDevOfData implements PairFunction<Tuple2<String, Tuple2<ArrayList<String>, ArrayList<String>>>, String, ArrayList<String>>
	{
		public Tuple2<String, ArrayList<String>> call (Tuple2<String, Tuple2<ArrayList<String>, ArrayList<String>>> data)
		{
			// data._2()._1() is averages
			// data._2()._2() is the entry 0-(n-1) is the pollution data, n-1 is the count
			
			//keep only the highest the data
			ArrayList<String> stdDevRaw = new ArrayList<String>();
			int lastIndex = data._2()._2().size() - 1;
			for (int i = 0; i < lastIndex; i++)
			{
				stdDevRaw.add("" + Math.pow(Double.parseDouble(data._2()._2().get(i)) - Double.parseDouble(data._2()._1().get(i)), 2));
			}
			stdDevRaw.add("1"); //dont forget the count!
			return new Tuple2<String, ArrayList<String>>(data._1(), stdDevRaw);
		}
	}
	
	public static class StdDevOfData implements PairFunction<Tuple2<String, ArrayList<String>>, String, ArrayList<String>>
	{
		public Tuple2<String, ArrayList<String>> call (Tuple2<String, ArrayList<String>> data)
		{
			// data._2() 0-(n-1) is the pollution data, n-1 is the count
			
			//average and square root the data
			ArrayList<String> stdDev = new ArrayList<String>();
			int lastIndex = data._2().size() - 1;
			double count = Double.parseDouble(data._2().get(lastIndex));
			for (int i = 0; i < lastIndex; i++)
			{
				stdDev.add("" + Math.sqrt(Double.parseDouble(data._2().get(i)) / count));
			}
			return new Tuple2<String, ArrayList<String>>(data._1(), stdDev);
		}
	}
	
	public static class ReduceDuplicateKeys implements Function2<ArrayList<String>, ArrayList<String>, ArrayList<String>>
	{
		public ArrayList<String> call (ArrayList<String> data1, ArrayList<String> data2)
		{
			// data._2() 0-4 is the pollution data, 5 is the count

			//Sum the data
			ArrayList<String> combined = new ArrayList<String>();
			BigInteger d1Value = null;
			BigInteger d2Value = null;
			for (int i = 0; i < data1.size(); i++)
			{
				d1Value = new BigInteger(data1.get(i));
				d2Value = new BigInteger(data2.get(i));
				combined.add("" + d1Value.add(d2Value));
			}
			return combined;
		}
	}
		
	public static class ReduceDuplicateKeysDoubles implements Function2<ArrayList<String>, ArrayList<String>, ArrayList<String>>
	{
		public ArrayList<String> call (ArrayList<String> data1, ArrayList<String> data2)
		{
			// data._2() 0-4 is the pollution data, 5 is the count

			//Sum the data
			ArrayList<String> combined = new ArrayList<String>();
			for (int i = 0; i < data1.size(); i++)
			{
				combined.add("" + (Double.parseDouble(data1.get(i)) + Double.parseDouble(data2.get(i))));
			}
			return combined;
		}
	}
		
	public static class StripCountOff implements PairFunction<Tuple2<String, ArrayList<String>>, String, ArrayList<String>>
	{
		public Tuple2<String, ArrayList<String>> call (Tuple2<String, ArrayList<String>> data)
		{
			ArrayList<String> NoCount = new ArrayList<String>(data._2());
			NoCount.remove(NoCount.size() - 1);
			return new Tuple2(data._1(), NoCount);
		}
	}
}