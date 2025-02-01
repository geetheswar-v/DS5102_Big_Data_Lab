from mrjob.job import MRJob
from mrjob.step import MRStep


class RatingsBreakdown(MRJob):

	def steps(self):

		return [MRStep(mapper=self.mapper_ratings,reducer=self.reducer_ratings)]


	def mapper_ratings(self,_,line):
		(userID,movieID,rating,timestamp) = line.split('\t')
		yield rating,1


	def reducer_ratings(self,key,values):
		yield key, sum(values)


if __name__=='__main__':
	RatingsBreakdown.run()
