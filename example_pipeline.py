import collections

from nspipeline import options
from nspipeline import step
from nspipeline import nsmain

class ConstantsStep(step.StepChunk):
    @staticmethod
    def get_steps(options):
        for i in range(50):
            yield ConstantsStep(options, i)
        
    def __init__(self, options, n):
        self.options = options
        self.n = n

    def __str__(self):
        return ".".join(map(str, [self.__class__.__name__, self.n]))
    
    def _outfiles(self):
        return {"constants": "constants{}.txt".format(self.n)}

    def run(self):
        outpath = self.outpaths(final=False)["constants"]

        with open(outpath, "w") as outfile:
            outfile.write("YO! {}".format(self.n))


# need to run from separate module so imported paths can be pickled properly

# def run():
#     stages = collections.OrderedDict()
#     stages["first"] = ConstantsStep

#     nsmain.main(options.Options, stages)

# if __name__ == '__main__':
#     run()
