import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import scala.Tuple2;

import java.io.IOException;
import java.util.*;

import static java.lang.Double.*;


public class G36HW2 {

    // Read Dataset row and convert each element to Tuple2<Vector, Integer> Point
    public static Tuple2<Vector, Integer> stringToTuple(String str){

        String[] tokens = str.split(",");
        double[] data = new double[tokens.length];
        for (int i = 0; i < tokens.length-1; i++) {
            data[i] = Double.parseDouble(tokens[i]);
        }
        Vector point =  Vectors.dense(data);
        Integer cluster = Integer.valueOf(tokens[tokens.length-1]);
        Tuple2<Vector, Integer> pair = new Tuple2<>(point, cluster);

        return pair;
    }

    // This Method compute Silhouette Coefficient for each "inputPoint" and "Clusters" based on "distanceArray" , number of clusters "K" , expected cluster size "T"
    public static double computeSilhouette(Tuple2<Vector, Integer> inputPoint, Broadcast<ArrayList<Long>> sharedClusterSizes, ArrayList<Double> distanceArray , int K, int T ){

        double a_p = 0, b_p = MAX_VALUE;
        for (int i = 0; i < K; i++) {
            // if the selected sample cluster numbers is the same number of inputPoint, inputPoint belongs to this cluster number
            // a_p is used in order to evaluate how well assigned the inputPoint is to it’s cluster
            if (inputPoint._2().equals(i))
                a_p = distanceArray.get(inputPoint._2()) / Math.min(sharedClusterSizes.value().get(inputPoint._2()), T);
            else {
                // b_p is defined as the average dissimilarity to the closest cluster which inputPoint is not in this cluster
                b_p = Math.min(distanceArray.get(i) / Math.min(sharedClusterSizes.value().get(i).doubleValue(), T), b_p);
            }
        }

        // Return The silhouette coefficient for inputPoint
        return (b_p - a_p) / Math.max(b_p, a_p);
    }


    public static void main(String[] args) throws IOException {

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // CHECKING NUMBER OF CMD LINE PARAMETERS
        // Parameters are: num_cluster, expected size, <path_to_file>
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        // check the input arguments are exactly 3 parameters
        if (args.length != 3) {
            throw new IllegalArgumentException("USAGE: dataset_name num_cluster num_expected_size");
        }

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        //          SPARK SETUP
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        SparkConf conf = new SparkConf(true).setAppName("Silhouette ");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        //          INPUT READING
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        // The number of clusters k (an integer)
        int K = Integer.parseInt(args[1]);

        //The expected sample size per cluster t (an integer)
        int T = Integer.parseInt(args[2]);

        // Time variables
        long startTime, endTime;

        // Define variables to store cluster size, minimum probability and selected samples
        ArrayList<Long> clusterSizes = new ArrayList<>();
        ArrayList<Float> minProb = new ArrayList<>();
        List<Tuple2<Vector, Integer>> selectedSamples = new ArrayList<>();

        // Define shared Variables to store common value throughout the program
        Broadcast<ArrayList<Float>> sharedMinProb = sc.broadcast(minProb);
        Broadcast<ArrayList<Long>> sharedClusterSizes = sc.broadcast(clusterSizes);
        Broadcast<List<Tuple2<Vector, Integer>>> clusteringSample = sc.broadcast(selectedSamples);

        // Task1: Read All samples in Java RDD of pairs (point,ClusterID) and repartition them to "4" Partitions and Cache them in ram
        JavaPairRDD<Vector, Integer> fullClustering = sc.textFile(args[0]).repartition(4)
                .mapToPair(G36HW2::stringToTuple).cache();

        //Task 2,3: Compute clusterSizes and find minimum Probability for each related Cluster
        for (int i = 0; i < K; i++) {
            sharedClusterSizes.value().add(fullClustering.values().countByValue().get(i));
            sharedMinProb.value().add(Math.min(T / sharedClusterSizes.value().get(i).floatValue(), 1));
        }
        clusteringSample.value().addAll(fullClustering.filter(currentElement ->
                // we select each sample from each cluster based on "min{t/|C|, 1}"
                (sharedClusterSizes.value().get(currentElement._2()) < T) || (Math.random() < sharedMinProb.value().get(currentElement._2()))
        ).cache().collect());


        // Task 4 :Compute the Approx Silhouette for entire points in the Dataset
        startTime = System.currentTimeMillis();
        double totalSilhouetteCoefficient = fullClustering.map(currentElement -> {

            //Create 1D Array to store Distance for each number of Cluster(K) and fill them with zero.
            ArrayList<Double> distanceArray = new ArrayList<>(Collections.nCopies(K, 0.0));

            // for each x belong to main data set, it computes distance between the point (x) and selected samples(new Sub cluster points)
            // it stores values in Distance Array based on cluster number
            for (Tuple2<Vector, Integer> sample : clusteringSample.getValue())
                distanceArray.set(sample._2(), distanceArray.get(sample._2()) + Vectors.sqdist(currentElement._1(), sample._1()));

            return computeSilhouette(currentElement, sharedClusterSizes, distanceArray, K,T);

        }).cache().reduce(Double::sum);
        endTime = System.currentTimeMillis();
        double approxSilhFull = totalSilhouetteCoefficient / fullClustering.count();
        System.out.println(" \n Value of approxSilhFull = " + approxSilhFull);
        System.out.printf(" Time to compute approxSilhFull = %d ms", endTime - startTime);


        //Task 5: Compute the "exactSilhFull" just for selected sample ("clusteringSample") from main Dataset
        startTime = System.currentTimeMillis();
        double totalExactSilhSample =0;
        for (int i = 0; i < clusteringSample.value().size(); i++) {
            ArrayList<Double> distanceArray = new ArrayList<>(Collections.nCopies(K, 0.0));
            for (int j = 0; j < clusteringSample.value().size(); j++)
                distanceArray.set(clusteringSample.value().get(j)._2(),
                        distanceArray.get(clusteringSample.value().get(j)._2()) +
                                Vectors.sqdist(clusteringSample.value().get(j)._1(), clusteringSample.value().get(i)._1()));

            totalExactSilhSample += computeSilhouette(clusteringSample.value().get(i), sharedClusterSizes, distanceArray, K, T);
        }
        endTime = System.currentTimeMillis();

        double exactSilhSample = totalExactSilhSample/clusteringSample.value().size();
        System.out.println("\n Value of the exactSilhSample : " + exactSilhSample);
        System.out.printf(" Time to compute exactSilhSample = %d ms", endTime - startTime);


//        System.out.printf(" \n Dataset size= %d, Selected sample= %d , T= %d , K= %d" ,fullClustering.count(), clusteringSample.value().size(),T,K );

    }
}