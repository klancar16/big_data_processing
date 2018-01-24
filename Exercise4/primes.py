from mrjob.job import MRJob

class MrJob_Primes(MRJob):

    def mapper(self, _, line):
        line = line.strip()
        numbers = [int(x) for x in line.split() if x.isdigit()]
        for number in numbers:
            yield 1, (number, 1)

    def _reducer_combiner(self, average_list):
        avg, count = 0, 0
        for tmp, c in average_list:
            avg = (avg * count + tmp * c) / (count + c)
            count += c
        return 1, (avg, count)

    def combiner(self, x, average_list):
        yield self._reducer_combiner(average_list)

    def reducer(self, x, average_list):
        _, average_list_final = self._reducer_combiner(average_list)
        yield average_list_final

if __name__ == '__main__':
    MrJob_Primes.run()
