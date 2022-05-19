import logging
logging.basicConfig(level = logging.INFO)
logger = logging.getLogger("create_data_files")

import argparse
from episode_service import create_all_stage_data_files

def main():
    '''
    calls episode_service.create_all_stage_data_files() using command 
    line arguments.
    create_all_stage_data_files reports stats about the newly created data files
    '''
    example = """
    python create_data_files.py --subsample 100
    """

    parser = argparse.ArgumentParser(description='Create local stage data files.', usage=f"--help/-h [--subsample <pos int>] [-v*]")
    parser.add_argument(
        '--subsample', default=100, 
        metavar="<subsample>",
        help='an optional subsample rate')
    parser.add_argument(
        '--verbose', default=False, 
        action=argparse.BooleanOptionalAction)

    args = vars(parser.parse_args())

    subsample_rate = args['subsample']
    verbosity_flag = args['verbose']

    logger.info(f"subsample_rate: {subsample_rate}")
    logger.info(f"verbosity_flag: {verbosity_flag}")

    create_all_stage_data_files(subsample_rate=subsample_rate, verbosity=verbosity_flag)


if __name__ == "__main__":
    main()


    logger.info("done")

