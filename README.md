# Assignments_bigdata

Assignment of Homework 1: DEADLINE April 11, 23.59pm
The purpose of this first homework is to set up the environment for developing Spark code on your machine, and to get acquainted with Spark and with its use to implement MapReduce algorithms. In preparation for the homework, perform the following preliminary steps:

Do the machine set up following these instructions.
Download and run the WordCountExample program (Java or Python). To fully understand the program, refer to the Introduction to Programming in Spark page for details on functional programming and on the usage of the methods offered by the  Spark APIs.
ASSIGNMENT. You must develop a Spark program to identify the T best products from a review dataset of an online retailer.

DATA FORMAT: a review dataset is provided as a file with one review per row. A review consists of 4 comma-separated fields: ProductID (string), UserID (string), Rating (integer in [1,5] represented as a real), Timestamp (integer). An example of review is: B00005N7P0,AH2IFH762VY5U,5.0,1005177600
TASK: you must write a program GxxHW1.java (for Java users) or GxxHW1.py (for Python users), where xx is your two-digit group number, which receives in input, as command-line arguments, two integers K and T, and path to a file storing a review dataset, and does the following things:
Reads the input set of reviews into an RDD of strings called RawData (each review is read as a single string), and subdivides it into K partitions.
Transform the RDD RawData into an RDD of pairs (String,Float) called normalizedRatings, so that for each string of RawData representing a review (ProductID,UserID,Rating,Timestamp), NormalizedRatings contains the pair (ProductID,NormRating), where NormRating=Rating-AvgRating and AvgRating is the average rating of all reviews by the user "UserID". To accomplish this step you can safely assume that there are a few reviews for each user. Note that normalizedRatings may contain several pairs for the same product, one for each existing review for that product!
Transform the RDD normalizedRatings into an RDD of pairs (String,Float) called maxNormRatings which, for each ProductID contains exactly one pair (ProductID, MNR) where MNR is the maximum normalized rating of product "ProductID". The maximum should be computed either using the reduceByKey method or the mapPartitionsToPair/mapPartitions method. (Hint: get inspiration from the WordCountExample program).
Print the T products with largest maximum normalized rating, one product per line. (Hint: use a combination of sortByKey and take methods.)
To test your program you can use file input_20K.csv, which contains 20000 reviews. The output on this dataset is shown in file output_20K.txt. For your homework adopt the same output format.

SUBMISSION INSTRUCTIONS. Each group must submit a single file (GxxHW1.java or GxxHW1.py depending on whether you are Java or Python users, where xx is your ID group). Only one student per group must submit the files in Moodle Exam using the link provided in the Homework1 section. Make sure that your code is free from compiling/run-time errors and that you use the file/variable names in the homework description, otherwise your score will be penalized. 
