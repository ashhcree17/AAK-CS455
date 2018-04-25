package edu.csu.cs;

import scala.Serializable;
import scala.Tuple2;

import java.util.Comparator;

public class Tuple2Comparator implements Comparator<Tuple2<Double,Double>>, Serializable {

    @Override
    public int compare(Tuple2<Double, Double> o1, Tuple2<Double, Double> o2) {
        return Double.compare(o1._2,o2._2);
    }
}
