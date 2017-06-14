import collections

from nspipeline.utilities import pickle

from nspipeline import options
from nspipeline import step
from nspipeline import nsmain

class Walker(step.StepChunk):
    @classmethod
    def get_steps(cls, options):
        big_step = int(cls.big_step)
        small_step = int(cls.small_step)

        for chrom in options.reference.chroms:
            chrom_length = options.reference.chrom_lengths[chrom]

            for start in range(0, chrom_length, big_step):
                end = min(start + big_step, chrom_length)
                yield cls(options, chrom, start, end, small_step)
        
    def __init__(self, options, chrom, start, end, step):
        self.options = options
        self.chrom = chrom
        self.start = start
        self.end = end
        self.step = step

        self._prepared = False

    def prepare_walker(self):
        pass

    def __str__(self):
        return ".".join([self.__class__.__name__,
                         self.chrom,
                         str(self.start),
                         str(self.end)])
    
    def _outfiles(self):
        return {"results": "results.{}.pickle".format(str(self))}

    def run(self):
        if not self._prepared:
            self.prepare_walker()
            self._prepared = True

        outpath = self.outpaths(final=False)["results"]

        results = collections.OrderedDict()
        for cur_start in range(self.start, self.end, self.step):
            cur_end = min(cur_start + self.step, self.end)
            cur_result = self.apply(self.chrom,
                                    cur_start,
                                    cur_end)

            interval = (self.chrom, cur_start, cur_end)
            results[interval] = cur_result

        with open(outpath, "wb") as outfile:
            pickle.dump(results, outfile, protocol=-1)

    @classmethod
    def results_for_chrom(cls, options, chrom):
        steps = cls.get_steps(options)
        steps_by_chrom = collections.defaultdict(list)
        for cur_step in steps:
            steps_by_chrom[cur_step.chrom].append(cur_step)

        chrom_steps = steps_by_chrom[chrom]
        chrom_steps.sort(key=lambda x: (x.chrom, x.start, x.end))

        chrom_results = {}
        for cur_step in chrom_steps:
            inpath = cur_step.outpaths(final=True)["results"]
            with open(inpath, "rb") as infile:
                chunk_results = pickle.load(infile)
                chrom_results.update(chunk_results)

                for chunk, result in chrom_results.items():
                    yield chunk, result


def run(walker):
    stages = collections.OrderedDict()
    stages["first"] = walker

    opts = nsmain.main(options.OptionsWithReference, stages)

    walker.combine(opts)



class TestWalker(Walker):
    big_step = 1e8
    small_step = 1e7

    def apply(self, chrom, start, end):
        result = "{}:{}-{}".format(chrom, start, end)
        return result

    @staticmethod
    def summarize_chrom(chrom, intervals, chrom_results):
        print(chrom, intervals)
        
if __name__ == '__main__':
    run(TestWalker)