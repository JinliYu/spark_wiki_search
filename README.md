# spark_wiki_search
boolean search based on spark


### Running Spark Word Count

* scp the jar to xcn00

* spark-submit --class WordCount --master yarn --deploy-mode cluster SparkWordCount-0.0.1-SNAPSHOT.jar /data/small_names.txt /user/cs132g2/output_small_02
(each time, you need a different output name)

* hadoop fs -get /user/cs132g2/output_small_02

* hadoop fs -ls /data
