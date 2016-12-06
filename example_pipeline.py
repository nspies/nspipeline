import collections

from nspipeline import options
from nspipeline import step
from nspipeline import nsmain

class ConstantsStep(step.StepChunk):
    @staticmethod
    def get_steps(options):
        yield ConstantsStep(options)
        
    def __init__(self, options):
        self.options = options

    def __str__(self):
        return ".".join([self.__class__.__name__])
    
    def _outfiles(self):
        return {"constants": "constants.txt"}        

    def run(self):
        outpath = self.outpaths(final=False)["constants"]

        with open(outpath, "w") as outfile:
            outfile.write("YO!")


def run():
    stages = collections.OrderedDict()
    stages["first"] = ConstantsStep

    nsmain.main(options.Options, stages)

if __name__ == '__main__':
    run()