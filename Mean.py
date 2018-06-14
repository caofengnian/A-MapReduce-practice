from mrjob.job import MRJob
import csv

def csv_readline(line):
	#Given a sting CSV line, return a list of strings.
	for row in csv.reader([line]):
		return row

class Part3(MRJob):
	def mapper(self, key, line):
		# calculate the sum of each line first
		# return the total of each line and the count of each line
		total = 0
		for value in csv_readline(line):
			total += float(value)
		count = len(csv_readline(line))
		yield 1, (total, count)
			
	def reducer(self, word, total_and_count):
		# calculate the overall mean
		total = 0
		count = 0
		for value in total_and_count:
			total += value[0]
			count += value[1]
		mean = 1.0*total/count
		yield 'The Mean is', mean

if __name__ == '__main__':
	Part3.run()