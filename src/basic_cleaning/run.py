#!/usr/bin/env python
"""
Performs basic cleaning and stores results on W and B
"""
import argparse
import logging
import wandb
import pandas as pd


logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def go(args):

    run = wandb.init(job_type="basic_cleaning")
    run.config.update(args)

    # Download input artifact. This will also log that this script is using this
    # particular version of the artifact
    artifact_local_path = run.use_artifact(args.input_artifact).file()
    df = pd.read_csv(artifact_local_path)

    # Remove data between certain ranges
    logging.info(f'Removing data lower than {args.min_price}, and higher than {args.max_price}')
    idx = df['price'].between(args.min_price, args.max_price)
    df = df[idx].copy()

    # fix date things
    logging.info('Fix last review string fromat to correct date format')
    df['last_review'] = pd.to_datetime(df['last_review'])

    # Save cleaned data
    df.to_csv("clean_sample.csv", index=False)

    # Upload cleaned data to wandb
    artifact = wandb.Artifact(
     args.output_artifact,
     type=args.output_type,
     description=args.output_description,
    )
    artifact.add_file("clean_sample.csv")
    run.log_artifact(artifact)
    ######################

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="This step cleans the data")


    parser.add_argument(
        "--input_artifact", 
        type=str,
        help='The artifact to be used',
        required=True
    )

    parser.add_argument(
        "--output_artifact", 
        type=str,
        help='The resultant artifact',
        required=True
    )

    parser.add_argument(
        "--output_type", 
        type=str,
        help='the type for the output artifact',
        required=True
    )

    parser.add_argument(
        "--output_description", 
        type=str,
        help='a description for the output artifact',
        required=True
    )

    parser.add_argument(
        "--min_price", 
        type=float,
        help='the minimum price to consider',
        required=True
    )

    parser.add_argument(
        "--max_price", 
        type=float,
        help='the maximum price to consider',
        required=True
    )


    args = parser.parse_args()

    go(args)
