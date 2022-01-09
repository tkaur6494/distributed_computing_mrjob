from mrjob.job import MRJob
from mrjob.step import MRStep


class MRFrobniusNormMatrix(MRJob):
    num = 0

    def mapper_get_row(self, _, line):
        # generating key value pair based on row
        # every row has a unique id
        yield(self.num, [float(i) for i in line.split(" ")])
        self.num = self.num+1

    def reducer_row_intermediate(self, row_key, value):
        # calculating for every row the square of all elements and summing them up
        col_value_sum = [i**2 for i in list(value)[0]]
        yield (None, sum(col_value_sum))

    def reducer_final(self, _, value):
        # output of all previous reducers sent to the final reducer
        # sum of all values obtained from all rows is added and then squareroot calculated
        yield(_, sum(value)**0.5)

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_row,
                   reducer=self.reducer_row_intermediate

                   ),
            MRStep(reducer=self.reducer_final)

        ]


if __name__ == '__main__':
    MRFrobniusNormMatrix.run()   # where MRFrobniusNormMatrix is your job
