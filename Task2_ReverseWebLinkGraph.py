from mrjob.job import MRJob
from mrjob.step import MRStep


class MRReverseWebLinkGraph(MRJob):

    def mapper_get_target(self, _, line):
        # not reading the comments in the file
        if(not("#" in line)):
            source_target = [int(i)
                             for i in line.split("\t")]
            # generating (target,source) pairs
            yield (source_target[1], source_target[0])

    # concatenating all sources corresponding to a given target
    def reducer_target_list_source(self, target, source):
        yield (target, list(source))

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_target,
                   reducer=self.reducer_target_list_source
                   ),

        ]


if __name__ == '__main__':
    MRReverseWebLinkGraph.run()
