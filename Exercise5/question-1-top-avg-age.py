from pyspark import SparkContext, SparkConf


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

    books_joined = books.join(ratings_users)\
        .map(lambda line: (line[0], line[1][0][0], line[1][0][1], line[1][0][2], line[1][0][3], line[1][1]))

    top20 = books_joined.flatMap(lambda line: [(line[2], age) for (_, _, age, _) in line[5] if age != -1]) \
        .groupByKey()\
        .mapValues(lambda x: sum(x)/len(x))\
        .sortBy(lambda line: line[1], False).take(20)

    final_str = ''
    for author in top20:
        final_str = final_str + '{0}, {1}\n'.format(author[0], author[1])

    print(final_str)

    sc.stop()

if __name__ == '__main__':
    main()
