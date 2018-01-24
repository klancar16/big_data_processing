from mrjob.job import MRJob

AUTHOR_QUERY = 'Stephen King'

class MrJob_Books_query(MRJob):
    def mapper(self, _, line):
        line = line.strip()
        isbn, title, author, year, _, _, _, _ = [x.strip('"') for x in line.split('";"')]
        if isbn != 'ISBN' and author == AUTHOR_QUERY:
            yield author, '{0}-{1}'.format(title, year)

    def combiner(self, author, books):
        yield author, ', '.join(books)

    def reducer(self, author, books):
        yield author, ', '.join(books)

if __name__ == '__main__':
    MrJob_Books_query.run()
