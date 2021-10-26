import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import scala.Tuple2;

import java.io.IOException;
import java.util.*;

import static java.lang.Double.*;


public class G36HW3 {

    // Read Dataset row and convert each element to Tuple2<Vector, Integer> Point
    public static Vector getPoints(String str){

        String[] tokens = str.split(" ");
        double[] data = new double[tokens.length];
        for (int i = 0; i < tokens.length; i++) {
            data[i] = Double.parseDouble(tokens[i]);
        }

        return Vectors.dense(data);
    }


    // This Method compute Silhouette Coefficient for each "inputPoint" and "Clusters" based on "distanceArray" , number of clusters "K" , expected cluster size "T"
    public static double computeApproxSilhouette(Tuple2<Vector, Integer> inputPoint, Broadcast<ArrayList<Long>> sharedClusterSizes, ArrayList<Double> distanceArray , int K, int T ){

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

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // CHECKING NUMBER OF CMD LINE PARAMETERS
        // Parameters are: path, kstart, h, iter, M, L
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        // check the input arguments are exactly 6 parameters
        if (args.length != 6) {
            throw new IllegalArgumentException("USAGE: path, kstart, h, iter, M, L");
        }

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        //     SPARK Configurations based on IMPORTANT Section of the Homework
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        SparkConf conf = new SparkConf(true)
                .setAppName("Homework3")
                .set("spark.locality.wait", "0s");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        //      Define and set Variables
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        //A path to a text file containing a point set in Euclidean space.
        String path = String.valueOf(args[0]);
        //An integer kstart which is the initial number of clusters.
        int kstart = Integer.parseInt(args[1]);
        //An integer h which is the number of values of k that the program will test.
        int h = Integer.parseInt(args[2]);
        //An integer iter which is the number of iterations of Lloyd's algorithm.
        int iter = Integer.parseInt(args[3]);
        // An integer M which is the expected size of the sample used to approximate the silhouette coefficient.
        int M = Integer.parseInt(args[4]);
        //An integer L which is the number of partitions of the RDDs containing the input points and their clustering.
        int L = Integer.parseInt(args[5]);

        // Time variables
        long startTime, startTimeSilhouette, endTimeClustering,endTimeSilhouette ;

        // Define variables to store cluster size, minimum probability and selected samples
        ArrayList<Long> clusterSizes = new ArrayList<>();
        ArrayList<Float> minProb = new ArrayList<>();
        List<Tuple2<Vector, Integer>> selectedSamples = new ArrayList<>();

        startTime = System.currentTimeMillis();
        // Task1: Read All samples in Java RDD of pairs (point,ClusterID) and repartition them to "L" Partitions and Cache them in ram
        JavaRDD<Vector> inputPoints = sc.textFile(path).repartition(L)
                                         .map(G36HW3::getPoints).cache();
        System.out.printf("Time for input reading = %d ", System.currentTimeMillis()-startTime);

        // For every k between kstart and kstart+h-1
        for( int k= kstart; k<= kstart+h-1 ; k++){
            final int finalK = k;
            clusterSizes.clear();
            minProb.clear();
            selectedSamples.clear();


            // step1: LLoyd's algorithm for k-means clustering and store into an RDD currentClustering of pairs (point, cluster_index)
            startTime =System.currentTimeMillis();
            KMeansModel cluster = KMeans.train(inputPoints.rdd(), k, iter);
            JavaPairRDD<Vector, Integer> currentClustering = inputPoints.repartition(L).flatMapToPair(point-> {
                List<Tuple2<Vector, Integer>> pointPairs = new ArrayList<>() ;
                pointPairs.add(new Tuple2<>(point, cluster.predict(point)));
                return pointPairs.iterator();
            }).cache();
            endTimeClustering = System.currentTimeMillis();


            // Compute clusterSizes and find minimum Probability for each related Cluster
            for (int i = 0; i < k; i++) {
                clusterSizes.add(currentClustering.values().countByValue().get(i));
                minProb.add(Math.min((float) (M / finalK) / clusterSizes.get(i).floatValue(), 1));
            }

            // Define shared Variables to store common value throughout the program
            Broadcast<ArrayList<Float>> sharedMinProb = sc.broadcast(minProb);
            Broadcast<ArrayList<Long>> sharedClusterSizes = sc.broadcast(clusterSizes);

            selectedSamples.addAll(currentClustering.filter(current ->
                    // we select each sample from each cluster based on "min{t/|C|, 1}"
                    (sharedClusterSizes.value().get(current._2()) < (M/finalK)) || (Math.random() < sharedMinProb.value().get(current._2()))
            ).cache().collect());

            // Define shared Variables to store common value throughout the program
            Broadcast<List<Tuple2<Vector, Integer>>> clusteringSample = sc.broadcast(selectedSamples);


            // Step2: Computes the approximate average silhouette coefficient of the clustering
            startTimeSilhouette = System.currentTimeMillis();
            double totalSilhouetteCoefficient = currentClustering.map(currentPoint -> {

                //Create 1D Array to store Distance for each number of Cluster(K) and fill them with zero.
                ArrayList<Double> distanceArray = new ArrayList<>(Collections.nCopies(finalK, 0.0));

                // for each x belong to main data set, it computes distance between the point (x) and selected samples(new Sub cluster points)
                // it stores values in Distance Array based on cluster number
                for (Tuple2<Vector, Integer> sample : clusteringSample.getValue())
                    distanceArray.set(sample._2(),  distanceArray.get(sample._2()) + Vectors.sqdist(currentPoint._1(), sample._1()));

                return computeApproxSilhouette(currentPoint, sharedClusterSizes, distanceArray, finalK, (M/finalK) );

            }).cache().reduce(Double::sum);

            endTimeSilhouette = System.currentTimeMillis();
            double approxSilhFull = totalSilhouetteCoefficient / currentClustering.count();


            // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
            //      Step3: Prints the following values:
            //      (a) the value k;
            //      (b) the value of the approximate average silhouette coefficient;
            //      (c) the time spent to compute the clustering;
            //      (d) the time spent to compute the silhouette
            // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
            System.out.printf("\n\nNumber of clusters k = %d ", k );
            System.out.printf("\nSilhouette coefficient = %f ",approxSilhFull );
            System.out.printf("\nTime for clustering = %d ", endTimeClustering-startTime);
            System.out.printf("\nTime for silhouette computation = %d ", endTimeSilhouette-startTimeSilhouette );

        }


    }
}