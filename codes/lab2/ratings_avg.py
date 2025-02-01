from mrjob.job import MRJob

class AverageRating(MRJob):
    
    def mapper(self, _, line):
        mid, rating = line.split('\t')[1:3]
        yield mid, (float(rating), 1)

    def reducer(self, mid, ratings):
        n, d = map(sum, zip(*ratings))
        s = round(float(n/d), 5)
        yield mid, s


if __name__ == '__main__':
    AverageRating.run()
