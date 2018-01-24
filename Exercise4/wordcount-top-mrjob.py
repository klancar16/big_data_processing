import string
from mrjob.job import MRJob

stop_words = ["a", "able", "about", "across", "after", "all", "almost", "also", "am", "among", "an", "and", "any",
              "are", "as", "at", "be", "because", "been", "but", "by", "can", "cannot", "could", "dear", "did", "do",
              "does", "either", "else", "ever", "every", "for", "from", "get", "got", "had", "has", "have", "he", "her",
              "hers", "him", "his", "how", "however", "i", "if", "in", "into", "is", "it", "its", "just", "least",
              "let", "like", "likely", "may", "me", "might", "most", "must", "my", "neither", "no", "nor", "not", "of",
              "off", "often", "on", "only", "or", "other", "our", "own", "rather", "said", "say", "says", "she",
              "should", "since", "so", "some", "than", "that", "the", "their", "them", "then", "there", "these", "they",
              "this", "tis", "to", "too", "twas", "us", "wants", "was", "we", "were", "what", "when", "where", "which",
              "while", "who", "whom", "why", "will", "with", "would", "yet", "you", "your"]


class MrJob_WordCount(MRJob):
    def mapper(self, _, line):
        tags = ['<title>', '<summary>', '<text>']
        # --- remove leading and trailing whitespace ---
        line = line.strip()
        if line.startswith(tuple(tags)):
            # --- convert upper case to lower case ---
            line = line.lower()
            # --- remove punctuation ---
            line = line.translate(line.maketrans("", "", string.punctuation))
            # line = line.translate(None, string.punctuation)
            # --- split the line into words ---
            words = line.split()

            # --- output tuples [word, 1] in tab-delimited format---
            for word in words:
                if word != "" and word not in stop_words:
                    yield word, 1

    def combiner(self, word, counts):
        yield (word, sum(counts))

    def reducer(self, word, counts):
        sum_count = sum(counts)
        if sum_count >= 50:
            yield word, sum_count


if __name__ == '__main__':
    MrJob_WordCount.run()
