import logging
logging.basicConfig(level = logging.INFO)
logger = logging.getLogger("create_data_files")

import argparse
from episode_service import create_all_stage_data_files

from env import DATA_FILES_DIR, S3_MEDIA_ANGEL_NFT_BUCKET, S3_MANIFESTS_DIR

def main():
    '''
    calls episode_service.create_all_stage_data_files() using command 
    line arguments.
    create_all_stage_data_files reports stats about the newly created data files
    '''
    example = """
    python create_data_files.py --subsample 100
    """

    parser = argparse.ArgumentParser(
        description=f"Create shuffled google data files in '{DATA_FILES_DIR}/' for all season manifest json files found under 's3://{S3_MEDIA_ANGEL_NFT_BUCKET}/{S3_MANIFESTS_DIR}'", 
        usage=f"--help/-h [--subsample <pos int>] [--cleanup] [--verbose]")
    parser.add_argument(
        '--subsample', default=100, 
        metavar="<subsample>",
        help='an optional subsample rate')
    parser.add_argument(
        '--cleanup', default=True, 
        action=argparse.BooleanOptionalAction,
        help='option to cleanup intermediate data files')
    parser.add_argument(
        '--verbose', default=False, 
        action=argparse.BooleanOptionalAction,
        help='optional verbose flag')

    args = vars(parser.parse_args())

    subsample_rate = args['subsample']
    cleanup_flag = args['cleanup']
    verbosity_flag = args['verbose']

    logger.debug(f"subsample_rate: {subsample_rate}")
    logger.debug(f"cleanup_flag: {cleanup_flag}")
    logger.debug(f"verbosity_flag: {verbosity_flag}")

    all_stage_data_files = create_all_stage_data_files(
        subsample_rate=subsample_rate, 
        cleanup=cleanup_flag,
        verbosity=verbosity_flag)

    logger.debug("all_stage_data_files:")
    for stage, file in all_stage_data_files.items():
        logger.debug(f"stage:{stage} data_file:{file}")

if __name__ == "__main__":
    main()

    logger.debug("done")

