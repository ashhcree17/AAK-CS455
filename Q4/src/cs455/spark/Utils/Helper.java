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
}
