from mrjob.job import MRJob
from mrjob.step import MRStep
import json

class MatrixMultiply(MRJob):   
    def steps(self):
        return [MRStep(mapper=self.mapper, reducer=self.reducer)]

    def mapper(self, _, line):
        name, r, c, val = json.loads(line)
        if name == "a":
            for i in range(5):
                yield (r, i), ('A', c, val)
        else:
            for i in range(5):
                yield (i, c), ('B', r, val)

    def reducer(self, key, values):
        a, b = {}, {}
        for name, l, val in values:
            if name == 'A':
                a[l] = val
            else:
                b[l] = val

        s = 0
        for l in range(5):
            il = a.get(l, 0)
            lj = b.get(l, 0)
            s+= il*lj

        yield key, s

if __name__ == '__main__':
    MatrixMultiply.run()

