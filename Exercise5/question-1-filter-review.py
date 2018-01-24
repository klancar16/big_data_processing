from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import udf
from pyspark.sql import Row
from pyspark.sql.functions import desc
from pyspark.sql.types import *


def count_reviews_per_year_range(min_year, max_year):
    def _count_reviews_per_year_range(book_year):
        if book_year.isdigit() and min_year <= int(book_year) <= max_year:
            return True
        else:
            return False
    return _count_reviews_per_year_range


def main():
    app_name = "SparkApplication"
    conf = SparkConf().setAppName(app_name)
    sc = SparkContext(conf=conf)

    ratings = sc.textFile('BX-Book-Ratings.csv')
    books = sc.textFile('BX-Books.csv')
    users = sc.textFile('BX-Users-updated.csv')

    # RDD[(ISBN, Book-Title, Book-Author, Year-Of-Publication, Publisher, Set(User_ID, Locat$
    rat_head = ratings.first()
    ratings = ratings.filter(lambda line: line != rat_head).\
        map(lambda line: line.split(";")).\
        map(lambda field: (int(field[0].strip('"')),
                           (field[1].strip('"'),
                            int(field[2].strip('"')) if field[2].strip('"') != 'NULL' else -1)
                           ))

    user_head = users.first()
    users = users.filter(lambda line: line != user_head).\
        map(lambda line: line.split("|")).\
        filter(lambda line: len(line) == 3).\
        map(lambda field: (int(field[0].strip('"')),
                           (field[1].strip('"'),
                            int(field[2].strip('"')) if field[2].strip('"') != 'NULL' else -1)
                           ))
    # ('276768', (('9057868059', 4), ('maastricht, limburg, netherlands', 23))),
    ratings_users = ratings.join(users).\
        map(lambda line: (line[1][0][0], (line[0], line[1][1][0], line[1][1][1], line[1][0][1]))).\
        groupByKey().map(lambda line: (line[0], list(line[1])))

    book_head = books.first()
    books = books.filter(lambda line: line != book_head).\
        map(lambda line: line.split(";")).\
        map(lambda field: (field[0].strip('"'),
                           (field[1].strip('"'),
                            field[2].strip('"'),
                            field[3].strip('"'),
                            field[4].strip('"'),
                            )))
 
    books_joined = books.join(ratings_users).\
        flatMap(lambda line: [Row(isbn=line[0], title=line[1][0][0], author=line[1][0][1],
                                  year=line[1][0][2], publisher=line[1][0][3], review=x) for x in line[1][1]])

    udfBooksInYears = udf(count_reviews_per_year_range(1992, 1998), BooleanType())
    df_udf = books_joined.toDF()\
        .filter(udfBooksInYears('year'))\
        .groupBy('isbn').count().sort(desc('count'))

    print(df_udf.show(50))

    sc.stop()

if __name__ == '__main__':
    main()
