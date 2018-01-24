from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import udf
import re


stopWords = ['a', 'able', 'about', 'across', 'after', 'all', 'almost', 'also',
             'am', 'among', 'an', 'and', 'any', 'are', 'as', 'at', 'be', 'because',
             'been', 'but', 'by', 'can', 'cannot', 'could', 'dear', 'did', 'do', 'does',
             'either', 'else', 'ever', 'every', 'for', 'from', 'get', 'got', 'had',
             'has', 'have', 'he', 'her', 'hers', 'him', 'his', 'how', 'however', 'i',
             'if', 'in', 'into', 'is', 'it', 'its', 'just', 'least', 'let', 'like',
             'likely', 'may', 'me', 'might', 'most', 'must', 'my', 'neither', 'no',
             'nor', 'not', 'of', 'off', 'often', 'on', 'only', 'or', 'other', 'our',
             'own', 'rather', 'said', 'say', 'says', 'she', 'should', 'since', 'so',
             'some', 'than', 'that', 'the', 'their', 'them', 'then', 'there', 'these',
             'they', 'this', 'tis', 'to', 'too', 'twas', 'us', 'wants', 'was', 'we',
             'were', 'what', 'when', 'where', 'which', 'while', 'who', 'whom', 'why',
             'will', 'with', 'would', 'yet', 'you', 'your', 'didnt', 'doesnt', 'wasnt',
             'isnt']


def process(first, second, third):
    st = first + ' ' + second + ' ' + third
    st = st.lower()
    st = re.sub(r'<[\w\d\/]*>', " ", st)
    # Include only letters and spaces (replace others with space)
    st = re.sub(r'[^\w\s]', " ", st)
    # remove all the digits and underscores from the text
    st = re.sub(r'[\d\_]', " ", st)
    st = ' '.join([word for word in st.split() if word not in stopWords])
    return st


def main():
    app_name = "SparkApplication"
    conf = SparkConf().setAppName(app_name)
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    data = sqlContext.read.format('com.databricks.spark.xml')\
        .options(rowTag='document').load('/user/students/input/nysk.xml')
    
    #After loading data into Dataframe, namely data, your Dataframe looks like:
    #DataFrame[date: timestamp, docid: bigint, source: string, summary: string, text: string, title: string, url: string]
    #From this Dataframe, you can select some particular fields for topic modeling purpose,
    # i.e., docid, summary, text, and title

    process_udf = udf(process, StringType())
    data = data.withColumn('text', process_udf('title', 'summary', 'text'))\
        .select('docid', 'text')
    
    #After you process further, you can store your Dataframe into CSV format by using:
    data.write.format('com.databricks.spark.csv')\
        .option('header', 'true').option('delimiter', '\t')\
        .save('/user/group16/question-3')

if __name__ == '__main__':
    main()
