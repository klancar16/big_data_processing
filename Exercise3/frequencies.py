import matplotlib.pyplot as plt
from nltk import TextCollection

if __name__ == '__main__':
    lines = [line.rstrip('\n') for line in open('data/wikitexts.txt') if line != '\n']
    text_stats = TextCollection(lines)
    words = sorted(list(set([word for line in lines for word in line.split() if len(word) >= 4])))

    all_lines = ' '.join(lines)

    final_string = ''
    for word in words:
        # w tf tfidf(w, d1) tfidf(w, d2) ... tfidf(w, d25)
        final_string = final_string + '{0} {1}'.format(word, all_lines.count(word))
        for doc in lines:
            final_string = final_string + ' {0:.3f}'.format(text_stats.tf_idf(word, doc))
        final_string = final_string + '\n'

    with open('data/frequencies.txt', 'w', encoding='UTF-8') as text_file:
        text_file.write(final_string)

    words_freq = sorted([all_lines.count(word) for word in words], reverse=True)
    plt.plot(words_freq)
    plt.savefig('data/frequencies.png')

