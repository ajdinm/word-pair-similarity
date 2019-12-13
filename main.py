from mrjob.job import MRJob
from mrjob.step import MRStep


class MRIdentity(MRJob):

    def mapper(self, _, line):
        for word in line.split(" "):
            yield(word.lower(), 1)

    def reducer(self, key, value):
        yield (key, sum(value))

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer)
        ]


if __name__ == '__main__':
    MRIdentity.run()
