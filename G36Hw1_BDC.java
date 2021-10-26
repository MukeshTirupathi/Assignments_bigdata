import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;
import scala.Tuple3;

import java.io.IOException;
import java.util.*;


public class WordCountExample {


    public static void main(String[] args) throws IOException {

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // CHECKING NUMBER OF CMD LINE PARAMETERS
        // Parameters are: num_partitions, <path_to_file>
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        if (args.length != 3) {
            throw new IllegalArgumentException("USAGE: num_partitions file_path");
        }

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // SPARK SETUP
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        SparkConf conf = new SparkConf(true).setAppName("WordCount");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // INPUT READING
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        // Read number of partitions or chunk
        int K = Integer.parseInt(args[ 0 ]);

        //Read T from CLI (we do not know:T products with largest maximum normalized rating)
        int T = Integer.parseInt(args[ 1 ]);

        // Read input file and subdivide it into K random partitions
        JavaRDD<String> review = sc.textFile(args[ 2 ]).repartition(K).cache();

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // SETTING GLOBAL VARIABLES
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        long numdocs;
        numdocs = review.count();
        System.out.println("Number of Lines = " + numdocs);
        JavaPairRDD<String, Tuple2<Float,Float> > person_ratings;
        JavaPairRDD<String , Float> normalizedRatings;
        JavaPairRDD<String , Float> maxNormRatings;

            // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
            // STANDARD WORD COUNT with reduceByKey
            // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        person_ratings = review.flatMapToPair((questionnaire) -> {    // <-- MAP PHASE (R1) -> create a userId and ratings pair for each review
            String[] Item_component = questionnaire.split(",");
            Float Rate = Float.valueOf(Item_component[ 2 ]);
            String User_Id = Item_component[ 1 ];

            ArrayList<Tuple2<String, Float>> pairs = new ArrayList<>();
            pairs.add(new Tuple2<String, Float>(User_Id, Rate));
            return pairs.iterator();

        }).mapValues(s -> new Tuple2<Float,Float>(s, 1f)).reduceByKey((x, y) -> new Tuple2<Float,Float>(x._1 + y._1 ,x._2 + y._2));    // <-- REDUCE PHASE (R1)-> compute AccSum of Ratings and number of Rating for each userID


        normalizedRatings = review.flatMap((document) -> {    // <-- MAP PHASE (R1)
            String[] Item_component = document.split(",");
            String Product_Id = Item_component[ 0 ];
            String User_Id = Item_component[ 1 ];
            Float Rate = Float.parseFloat(Item_component[ 2 ]);

            ArrayList<Tuple3<String,String, Float>> pairs = new ArrayList<>();
            pairs.add(new Tuple3<>(Product_Id, User_Id , Rate));
            return pairs.iterator();

        }).cartesian(person_ratings).flatMapToPair(pur_ur -> {

                    ArrayList<Tuple2<String,Float>> pairs = new ArrayList<>();
                    if(pur_ur._1()._2().equals(pur_ur._2._1()))
                        pairs.add(new Tuple2<>(pur_ur._1()._1(), (pur_ur._1()._3()  -  (pur_ur._2()._2()._1() / pur_ur._2()._2()._2() ))));

                    return pairs.iterator();
                });

        System.out.println(" Normalized Ratings for Products = " + normalizedRatings.collect());


        maxNormRatings = normalizedRatings.reduceByKey((x,y) -> {
            if (x > y)
                return x;
            else
                return y;
        } );

        System.out.println(" maxNormRatings   for Products = " + maxNormRatings.collect());

    }

}
