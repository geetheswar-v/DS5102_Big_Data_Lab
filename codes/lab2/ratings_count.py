from mrjob.job import MRJob

class CountRatings(MRJob):
    def mapper(self, _, line):
        user_id = line.split('\t')[0]
        yield user_id, 1

    def reducer(self, user_id, counts):
        yield user_id, sum(counts)


if __name__ == '__main__':
    CountRatings.run()
