from mrjob.job import MRJob
from mrjob.step import MRStep
from more_itertools import locate


def clean_text(array, articles):
    return filter(lambda w: w not in articles, array)


class MRWordPairDistance(MRJob):

    # given the list of documents, for each word frame calculate Jaccard similarity metric as follows
    # Jaccard(w1, w2) = A / B, where
    # A is number of (w1, w2) frames in entire frame set, where frame set is defined below
    # B is number of occurrences of w2 in entire input set

    filter_words = ["a", "an", "and", "the"]
    punctuation = [".", ",", ":"]

    def split_line(self, line):
        for p in self.punctuation:
            line = line.replace(p, "")  # trivial punctuation removal
        line = list(map(lambda w: w.lower(),
                        line.split(" ")))  # breaking line into individual words; ignore capitalization
        clean_line = list(clean_text(line, self.filter_words)
                          )  # trivial article removal

        # generate all possible word frames
        # for each unique word, we find all words to the right of it and create a such frame
        
        # note that this approach ignores last word in the line as it is the rightmost word (hence clean_line[:-1])
        words = set(clean_line)  # extract list of unique words
        frames = {
            word: list(
                map(lambda i: clean_line[i+1], locate(clean_line[:-1], lambda w: w == word)))  # locate returns indices of specific word W in the array
            # for each such index, i,  we take word clean_line[i+1] and create frame (W, clean_line[i+1])
            for word in words
        }
        return frames

    # generate all possible frames from line foreach frame emit (second_word, first_word)
    def map_second_word(self, _, line):
        for first_word, second_words in self.split_line(line).items():
            for second_word in second_words:
                yield(second_word, first_word)

    def reduce_second_word(self, second_word, first_words):
        # parameters of this reducer are as follows (word, first_words), where
        # first_words is array of all words that create (w_i, word) frames
        first_words = list(first_words)
        # all occurrences of word in the input set is simply length of first_words array
        total_second_word_count = len(first_words)
        for first_word in first_words:
             # key of this reducer's out is frame that occurred in document
             # which ensures that future reducer is invoked once for each frame generated from input set
             # value is number of occurrences of second word of the frame i.e. B in original formula
            yield((first_word, second_word), total_second_word_count)

    def map_identity(self, frame, total_second_word_count):
        yield(frame, total_second_word_count)  # identity mapper

    def reduce_metric(self, frame, counts):
        # key is frame we want to calculate Jaccard metric of
        # value is array of elements, where each element is equal to number of times second word of the frame occurs in input set (i.e. A in the original formula)
        counts = list(counts)
        # number of times specific frame (i.e. A in the original formula) occurred is equal to number of elements in the array
        frame_count = len(counts)
        # since all elements here are the same, we take any of them and calculate Jaccard similary as value of this reducer's output
        word_count = counts[0]
        yield(' '.join(frame), frame_count/word_count)

    def steps(self):
        # combine aforementioned info final algorithm
        return [
            MRStep(mapper=self.map_second_word,
                   reducer=self.reduce_second_word),
            MRStep(mapper=self.map_identity, reducer=self.reduce_metric)
        ]


if __name__ == '__main__':
    MRWordPairDistance.run()
