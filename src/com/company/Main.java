package com.company;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.rdd.PairRDDFunctions;
import scala.Tuple2;

import java.util.Arrays;

public class Main {

    public static void main(String[] args) {
        //Because Windows 10.....
        System.setProperty("hadoop.home.dir", "C:\\winutil\\");

        //Config spark
        SparkConf conf = new SparkConf().setAppName("WordCounting").setMaster("local");
        //Start spark conext
        JavaSparkContext sc = new JavaSparkContext(conf);

        /*
        Implement word counting using Spark RDD.
        All words should be turned lower case.
        Sort output by word frequency in descending order(Most common words in the beginning).
        Before counting, remove stop words. e.g. "the", "a" and " ".
        Count number of words. The number of words should be outputted separately.
         */
        //JavaRDD<String> stopWordsFile = sc.textFile("C:\\Users\\Alexander Eric\\Desktop\\Skola augsburg\\JavaSpark\\Exercie2Spark\\stopwords.txt");
        //From link in PDF.
        JavaRDD<String> inputFile =  sc.textFile("C:\\Users\\Alexander Eric\\Desktop\\Skola augsburg\\JavaSpark\\Exercie2Spark\\datatest.txt");
        JavaRDD<String> wordsFromFile = inputFile.flatMap(content -> Arrays.asList(content.split(" ")).iterator());
        String[] stopWords = new String[] {"the", "a", "are", "is"};
        JavaRDD<String> stopWordsRDD = sc.parallelize(Arrays.asList(stopWords));

        
        //Remove all stopWords from wordsFromFile
        wordsFromFile = wordsFromFile.subtract(stopWordsRDD);
        //Reduce
        JavaPairRDD<String,Integer> countData = wordsFromFile.mapToPair(t -> new Tuple2<>(t.toLowerCase(),1)).reduceByKey((x,y) -> (int) x + (int) y);
        //Now we want to sort by value. JavaRDD has sortBy function. We can chose if we want to sort by key or value.
        JavaRDD sortedByValue = countData.map(t -> t).sortBy(t-> t._2,false,countData.getNumPartitions());


        sortedByValue.saveAsTextFile("countedData");

    }
}
