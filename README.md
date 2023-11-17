# BenchmarkingWayang.
To replicate the results obtained in the pdf, do the following:
1) For WordCount application, first generate the input data by replicating the harry.txt file by running the gen2.py file with the required input file size, then store the main.java file in your unix system, compile it as a JAR file with Apache Wayang as a dependency, then execute the JAR. Make sure the environment variables required for Wayang to run are set properly and all the files locations are correctly configured.
2)  For TPC-H queries, first generate the TPC-H data, and load it into a postgreSQL DataBase. Then configure the database name, postgres username and password in the SqlTest.java file. Then run the file like the main.java file above. 
