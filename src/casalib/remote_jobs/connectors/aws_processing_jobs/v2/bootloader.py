"""
Program that will be used as bootloader at the remote
machine
"""
import argparse
from collections import namedtuple
from itertools import zip_longest
import logging
import os
from pathlib import Path
from pprint import pformat
import shutil
import subprocess
import sys


LIBS_ORIG_FLD = '/opt/ml/processing/libs_cp'
LIBS_FLD = '/opt/ml/processing/libs'


File = namedtuple('File', ['dest_folder', 'orig_file', 'dest_file'])


def extract_tar_gz(tar_path: str | Path, target_folder: str | Path) -> None:
    ''' Extract the passed tar.gz file '''
    import tarfile

    tar_path = Path(tar_path)
    target_folder = Path(target_folder)

    target_folder.mkdir(parents=True, exist_ok=True)

    with tarfile.open(tar_path, "r:gz") as tar:
        tar.extractall(path=target_folder)


def init_machine() -> None:
    """
    Initialize the Processing Job to run the code.
    """
    # Configures the Logging
    logging.basicConfig(
        format='%(asctime)s %(levelname)s %(message)s',
        level=logging.INFO,
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('out.log'),
        ]
    )

    # Procedure - adjust the files inside the libs folder
    #   All files sent in ProcessingInput will go to a
    #       prefix with the same name.
    #
    #   We need to adjust these paths to mount the correct
    #       the folder structure
    logging.info("Starting Init")

    logging.info("Adjusting input folder")

    orig_root = Path(LIBS_ORIG_FLD)
    dest_root = Path(LIBS_FLD)

    cp_libs_fld = [
        File(
            (dest_root / correct_parent).parent,
            file_path,
            dest_root / correct_parent
        )

        # We will identify all files in the given prefix
        for root, dirs, files in os.walk(orig_root)
        for file in files

        # Creating Path object, and identify the file_path
        #   relative to the original root
        for file_path in [Path(root) / file]
        for file_path_rel in [file_path.relative_to(orig_root)]

        # Adjusting the path: file.py/file.py > file.py
        for correct_parent in [str(file_path_rel.parent)]
    ]

    def adj_path(
        dest_fld: Path, orig_path: Path, dest_path: Path
    ) -> File:
        special_names = [
            'main_program.py',
            'contents_libs_to_send.tar.gz',
        ]

        if orig_path.parent.parent.name in special_names:
            dest_fld = dest_fld.parent
            dest_path = dest_path.parent

        return File(dest_fld, orig_path, dest_path)

    cp_libs_fld = [
        adj_path(dest_folder, orig_path, dest_path)
        for dest_folder, orig_path, dest_path in cp_libs_fld
    ]

    logging.info('\n%s', (pformat(cp_libs_fld)))

    for path_fld, old_path, new_path in cp_libs_fld:
        path_fld.mkdir(parents=True, exist_ok=True)
        shutil.move(old_path, new_path)

    logging.info("Adjustments")
    logging.info('\n%s', pformat(cp_libs_fld))

    logging.info('Extracting contents_libs_to_send.tar.gz')
    extract_tar_gz(
        tar_path=dest_root / 'contents_libs_to_send.tar.gz',
        target_folder=dest_root
    )

    logging.info("Final libs folder")
    output_folder_files = [
        Path(root) / file
        for root, dirs, files in os.walk(LIBS_FLD)
        for file in files
    ]
    logging.info('\n%s', pformat(output_folder_files))

    # Installing Libs
    logging.info("Installing whl libs")
    libs_to_install = [
        f
        for f in output_folder_files
        for f_suffixes in [f.suffixes]
        if len(f_suffixes) >= 1
        if f.suffixes[-1] == '.whl'
    ]
    logging.info("Libs to install: %s", libs_to_install)
    for lib in libs_to_install:
        logging.info("Installing %s", lib)
        subprocess.run(["pip", "install", lib], check=False)

    logging.info("Installing graphic libs")
    subprocess.run([sys.executable, "-m", "pip", "install", "matplotlib"], check=False)
    subprocess.run([sys.executable, "-m", "pip", "install", "seaborn"], check=False)
    subprocess.run([sys.executable, "-m", "pip", "install", "ipython"], check=False)

    # Adding libs folder to path
    sys.path.insert(1, LIBS_FLD)

    logging.info("Ending Init")


def grouper(iterable, n, *, incomplete='fill', fillvalue=None):  # type: ignore
    "Collect data into non-overlapping fixed-length chunks or blocks"
    # grouper('ABCDEFG', 3, fillvalue='x') --> ABC DEF Gxx
    # grouper('ABCDEFG', 3, incomplete='strict') --> ABC DEF ValueError
    # grouper('ABCDEFG', 3, incomplete='ignore') --> ABC DEF
    args_iter = [iter(iterable)] * n
    if incomplete == 'fill':
        return zip_longest(*args_iter, fillvalue=fillvalue)
    if incomplete == 'strict':
        return zip(*args_iter, strict=True)
    if incomplete == 'ignore':
        return zip(*args_iter)
    raise ValueError('Expected fill, strict, or ignore')


if __name__ == '__main__':
    init_machine()

    os.chdir(LIBS_FLD)

    parser = argparse.ArgumentParser()

    # Parse arguments
    args, unknown = parser.parse_known_args()

    # Get arguments dict
    dict_params = {
        param_adj: val
        for param, val in grouper(unknown, 2)  # type: ignore
        for param_adj in [param.replace('--', '')]
    }

    logging.info(dict_params)

    import main_program   # type: ignore  # pylint: disable=import-error
    main_program.main(**dict_params)
