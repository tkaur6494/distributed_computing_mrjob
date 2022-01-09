from mrjob.job import MRJob
from mrjob.step import MRStep
from nltk.corpus import stopwords
import csv
import re


# list of stop words obatined from nltk
stop_words = set(stopwords.words('english'))


WORD_RE = re.compile(r"[\w']+")  # match words


class MRTop10WordsByMovieGenre(MRJob):

    def mapper_get_words(self, _, line):
        # using csv reader to handle the comma values within the title field
        #  source - https://stackoverflow.com/questions/43067373/split-by-comma-and-how-to-exclude-comma-from-quotes-in-split-python/43067487
        line_split = ['"{}"'.format(x) for x in list(
            csv.reader([line], delimiter=',', quotechar='"'))[0]]

        # line_split = line.split(",")
        # removing numbers from title eg Toy Story (1995) -> Toy Story
        line_split[1] = re.sub(r'[0-9+]', '', line_split[1])
        # removing all extra characters except alphabets
        line_split[1] = re.sub(r'[^a-zA-Z \n\.]', '', line_split[1])
        # removing stopwords and converting to lowercase
        line_split[1] = ' '.join(
            [word.lower() for word in line_split[1].split(" ") if word.lower() not in stop_words])
        # removing double spaces generated after removing numbers and special characters
        line_split[1] = re.sub(r' +', ' ', line_split[1]).strip()
        # removing quotes from the genre field
        line_split[2] = re.sub("\"", "", line_split[2])

        # one movie can belong to multiple genres so splitting the genre string to a list of genres

        genre_list = line_split[2].split("|")

        # for every genre generating a key value pair
        # key=(genre,word) value=1
        for genre in genre_list:
            for word in line_split[1].split(" "):
                if(word != ""):
                    # handling cases where the movie name consists of only stop words and numbers example me too (2012)
                    yield ((genre, word), 1)

    def combiner_count_words(self, genre_word, word_count):
        # using combiner to sum up all (genre,word) pairs to get total count
        yield (genre_word, sum(word_count))

    def reducer_count_words(self, word, counts):
        # separating the key (genre,word) pair to genre,(count,word) to be able to send all common genres
        # to one reducer
        genre_word_separate = list(word)
        yield genre_word_separate[0], (sum(counts), genre_word_separate[1])

    def reducer_find_max_word(self, genre, word_count_pairs):
       # sort all the word count pairs in reverse and take the top 10
        top10mostfrequent = sorted(
            list(word_count_pairs), reverse=True)[:10]
        for wcp in top10mostfrequent:
            yield genre, wcp

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_words,
                   combiner=self.combiner_count_words,
                   reducer=self.reducer_count_words
                   ),
            MRStep(reducer=self.reducer_find_max_word)
        ]


if __name__ == '__main__':
    MRTop10WordsByMovieGenre.run()
