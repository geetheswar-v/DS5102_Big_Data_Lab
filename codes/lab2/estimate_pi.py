from mrjob.job import MRJob


class EstimatePi(MRJob):
	def mapper(self, _, line):
		x_y = line.strip().split()
		if len(x_y) == 2:
			x, y = map(float, x_y)
			if (x**2 + y**2) <= 1:
				yield "inside_total", (1, 1)
			else:
				yield "inside_total", (0, 1)

	def reducer(self, key, values):
		inside, total = map(sum, zip(*values))

		yield "PI", {
			'pi_estimate': round(4 * (inside / total), 2),
			'points_inside': inside,
			'total_points': total
		}


if __name__ == '__main__':
	EstimatePi.run()

