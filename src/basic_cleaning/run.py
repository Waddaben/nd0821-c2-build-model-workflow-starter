#!/usr/bin/env python
"""
Performs basic cleaning and stores results on W and B
"""
import argparse
import logging
import wandb


logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def go(args):

    run = wandb.init(job_type="basic_cleaning")
    run.config.update(args)

    # Download input artifact. This will also log that this script is using this
    # particular version of the artifact
    # artifact_local_path = run.use_artifact(args.input_artifact).file()

    ######################
    # YOUR CODE HERE     #
    ######################


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="This step cleans the data")


    parser.add_argument(
        "--parameter1", 
        type=str,
        help='testing string',
        required=True
    )

    parser.add_argument(
        "--parameter2", 
        type=str,
        help='testing string',
        required=True
    )

    parser.add_argument(
        "--parameter3", 
        type=str,
        help='testing string',
        required=True
    )


    args = parser.parse_args()

    go(args)
