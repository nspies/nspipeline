from __future__ import print_function

import argparse
import collections
import json
import logging
import sys

from nspipeline import log
from nspipeline import options as nsoptions
from nspipeline import pipeline
from nspipeline import utilities


logging.basicConfig(format='%(message)s', level=logging.DEBUG)



def ready_output_dir(options):
    utilities.ensure_dir(options.working_dir)
    utilities.ensure_dir(options.results_dir)
    utilities.ensure_dir(options.log_dir)


def run(options, stages):
    """
    1. create output directories
    2. collect args for each stage
    3. check which stages need to run
    4. iterate through stages and submit jobs
    5. validate that we're done running
    """

    ready_output_dir(options)

    runner = pipeline.Runner(options)

    for stage_name, stage in stages.items():
        runner.run_stage(stage, stage_name)


def clean(options, stages, clean_stage_name):
    if not clean_stage_name in stages:
        print('*'*20, "ERROR", '*'*20)
        print('Error: unknown stage "{}". Stage must be one of the '.format(clean_stage_name))
        print('following (remember to include surrounding quotation marks):')
        for i, stage_name in enumerate(stages):
            print('{:>3} - "{}"'.format(i+1, stage_name))
        sys.exit(1)

        
    doclean = False
    for stage_name, stage in stages.items():
        if doclean or stage_name == clean_stage_name:
            doclean = True

            stage.clean_all_steps(options)


def load_config(config_path, options_class):
    try:
        config = json.load(open(config_path))
    except ValueError as err:
        print("Error parsing configuration file '{}': '{}'\n  Check that this is a properly formatted JSON file!".format(config_path, err))
        sys.exit(1)
    options = options_class.deserialize(config, config_path)
    return options

    
def parse_arguments(args, options_class, stages):
    parser = argparse.ArgumentParser(description="tenxtyper")
    parser.add_argument("config", help="Path to configuration.json file")
    parser.add_argument("--restart", metavar="FROM-STAGE", help="restart from this stage")
    parser.add_argument("--local", action="store_true", help="run locally using multiprocessing")
    parser.add_argument("--debug", action="store_true", help="run in debug mode")

    args = parser.parse_args()
    
    options = load_config(args.config, options_class)
    options.debug = args.debug

    print(options)

    if args.local:
        options.cluster_settings = nsoptions.ClusterSettings()

    if args.restart is not None:
        clean(options, stages, args.restart)

    log.log_command(options, sys.argv)

    return options


def main(options_class, stages):
    options = parse_arguments(sys.argv, options_class, stages)
    run(options, stages)



if __name__ == '__main__':
    main()
