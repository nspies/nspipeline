import collections
import os

from nspipeline import options
from nspipeline import step
from nspipeline import main as nsmain

class ConstantsStep(step.StepChunk):
    @staticmethod
    def get_steps(options):
        yield ConstantsStep(options)
        
    def __init__(self, options):
        self.options = options

    def __str__(self):
        return ".".join([self.__class__.__name__])
        
    def outpaths(self, final):
        directory = self.results_dir if final \
                    else self.working_dir

        return {"constants": os.path.join(directory, "constants.txt")}

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