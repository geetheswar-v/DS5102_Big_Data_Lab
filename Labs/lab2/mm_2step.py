from mrjob.job import MRJob
from mrjob.step import MRStep
import json

class MatrixMultiply(MRJob):   
    def steps(self):
        return [
            MRStep(mapper=self.mapper, reducer=self.reducer),
            MRStep(reducer=self.reducer2)]

    def mapper(self, _, line):
        name, r, c, val = json.loads(line)
        if name == "a":
            yield c, (name, r, val)
        else:
            yield r, (name, c, val)

    def reducer(self, key, values):
        a, b = {}, {}
        for name, l, val in values:
            if name == "a":
                a[l] = val
            else:
                b[l] = val

        for i in range(5):
            for j in range(5):
                il = a.get(i, 0)
                lj = b.get(j, 0)
                yield (i, j), il*lj

    def reducer2(self, key, values):
        s = 0
        for val in values:
            s += val
        yield key, s

if __name__ == '__main__':
    MatrixMultiply.run()

