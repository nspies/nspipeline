import collections

try:
    import cPickle as pickle
except ImportError:
    import pickle

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

    def __str__(self):
        return ".".join([self.__class__.__name__,
                         self.chrom,
                         str(self.start),
                         str(self.end)])
    
    def _outfiles(self):
        return {"results": "results.{}.pickle".format(str(self))}

    def run(self):
        outpath = self.outpaths(final=False)["results"]

        results = {}
        for cur_start in range(self.start, self.end, self.step):
            cur_end = min(cur_start + self.step, self.end)
            cur_result = self.apply(self.chrom,
                                    cur_start,
                                    cur_end)

            interval = (self.chrom, cur_start, cur_end)
            results[interval] = cur_result

        with open(outpath, "w") as outfile:
            pickle.dump(results, outfile, protocol=-1)

    @classmethod
    def combine(cls, options):
        steps = cls.get_steps(options)
        steps_by_chrom = collections.defaultdict(list)
        for cur_step in steps:
            steps_by_chrom[cur_step.chrom].append(cur_step)

        for chrom in sorted(steps_by_chrom):
            chrom_results = {}
            for cur_step in steps_by_chrom[chrom]:
                inpath = cur_step.outpaths(final=True)["results"]
                with open(inpath) as infile:
                    chunk_results = pickle.load(infile)
                    chrom_results.update(chunk_results)

            intervals = sorted(chrom_results)
            results_in_order = [chrom_results[i] for i in intervals]
            cls.summarize_chrom(chrom, intervals, results_in_order)

class TestWalker(Walker):
    big_step = 1e8
    small_step = 1e7

    def apply(self, chrom, start, end):
        result = "{}:{}-{}".format(chrom, start, end)
        return result

    @staticmethod
    def summarize_chrom(chrom, intervals, chrom_results):
        print chrom, intervals

def run(walker):
    stages = collections.OrderedDict()
    stages["first"] = walker

    opts = nsmain.main(options.OptionsWithReference, stages)

    walker.combine(opts)

if __name__ == '__main__':
    run(TestWalker)