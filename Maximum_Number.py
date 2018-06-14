from mrjob.job import MRJob
import csv

def csv_readline(line):
	for row in csv.reader([line]):
		return row

class Part2(MRJob):
	def mapper(self, line_no, line):
		#return each line maximum
		number = []
		for value in csv_readline(line):
			number.append(int(value))
		yield 1, max(number)
			
	def reducer(self, word, maximum):
		#output overall maximum
		yield 'The Maximum is', max(maximum)

if __name__ == '__main__':
	Part2.run()