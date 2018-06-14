from mrjob.job import MRJob
import csv

def csv_readline(line):
    #Given a sting CSV line, return a list of strings.
    for row in csv.reader([line]):
        return row

class Part4(MRJob):
	def mapper(self, line_no, line):
	#map the URL which contains each link
		cell = csv_readline(line)
		for each in cell:
			yield each, cell
		
	def reducer(self, cell, line):
		url1=[]
		url2=[]

		for each in line:
			if cell == each[1]:
				url1.append(each)
			else:
				url2.append(each)

		for each in url1:#output URL
			for links in url2:
				each=str(each).replace('[',"").replace(']',"").replace("'","")
				#make the output in a tidy format
				yield "URL is:", each+', '+str(links[1])



if __name__ == '__main__':     
	Part4.run()