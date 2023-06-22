# SCALA TEST EXAMPLE

The code contains 4 object scala files with three different functions(square,cube and sum) and one implementation for spark.

The idea of this sample scala project is to make you aware of how to write test in scalatest and run them
## Assumptions
- Java (1.8) and JAVA_HOME set
- sbt 1.1.x
- scala 2.12
- spark 2.4

## Instructions

- Run command sbt test from the root of the project
- It should pass 1 test case, 1 failing , 1 ignored
- One of the test is ignored , follow the instructions over that function test case to enable it
- One of the test is failing, correct the assetion logic to pass the test case.
- Now run the test cases again using sbt clean test
- This time there should be 3 test passing successfully.

## Runnng Sum Program using Spark-Submit
- Download and extract Spark 2.4.0 (https://dlcdn.apache.org/spark/spark-3.2.1/spark-3.2.1-bin-hadoop3.2.tgz)
- set SPARK_HOME to the extracted folder
- Add SPARK_HOME/bin to PATH variable to make spark-submit available to be run from anywhere 

- Build the project jar 
- sbt clean package
- Run spark-submit command to check code is running
- spark-submit --class com.de.app.Sum --master local target/scala-2.12/in-de-codepair-template-scala_2.12-0.1.0-SNAPSHOT.jar
- Check sum=20 in the output

## Summary
Expect similar test cases in the actual coding round
You should be able to run these steps on your machine and Inform the Hiring team once you are able to run these steps successfully
