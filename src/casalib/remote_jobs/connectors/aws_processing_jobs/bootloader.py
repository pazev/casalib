import argparse
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


def init_machine():
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
    logging.info("Starting Init")

    cp_libs_fld = [
        (
            (dest_root / correct_parent).parent,
            file_path,
            dest_root / correct_parent
        )

        for orig_root in [Path(LIBS_ORIG_FLD)]
        for dest_root in [Path(LIBS_FLD)]

        for root, dirs, files in os.walk(orig_root)
        for file in files
        for file_path in [Path(root) / file]
        for file_path_rel in [file_path.relative_to(orig_root)]
        for correct_parent in [str(file_path_rel.parent)]
    ]

    def adj_path(dest_fld: Path, orig_path: Path, dest_path: Path):
        if orig_path.parent.parent.name == "main_program.py":
            dest_fld = dest_fld.parent
            dest_path = dest_path.parent

        return dest_fld, orig_path, dest_path

    cp_libs_fld = [
        adj_path(dest_folder, orig_path, dest_path)
        for dest_folder, orig_path, dest_path in cp_libs_fld
    ]

    logging.info("Input folder")
    logging.info(pformat(cp_libs_fld))

    for path_fld, old_path, new_path in cp_libs_fld:
        path_fld.mkdir(parents=True, exist_ok=True)
        shutil.move(old_path, new_path)

    logging.info("Input folder")
    logging.info(pformat(cp_libs_fld))

    logging.info("Final libs folder")
    output_folder_files = [
        Path(root) / file
        for root, dirs, files in os.walk(LIBS_FLD)
        for file in files
    ]
    logging.info(pformat(output_folder_files))

    # Installing Libs
    logging.info("Installing whl libs")
    libs_to_install = [
        f
        for f in output_folder_files
        if f.suffixes[-1] == '.whl'
    ]
    logging.info(f"Libs to install: {libs_to_install}")
    for lib in libs_to_install:
        logging.info(f"Installing {lib}")
        subprocess.run(["pip", "install", lib])

    logging.info("Installing graphic libs")
    subprocess.run([sys.executable, "-m", "pip", "install", "matplotlib"])
    subprocess.run([sys.executable, "-m", "pip", "install", "seaborn"])
    subprocess.run([sys.executable, "-m", "pip", "install", "ipython"])

    # Adding libs folder to path
    sys.path.insert(1, LIBS_FLD)

    logging.info("Ending Init")


def grouper(iterable, n, *, incomplete='fill', fillvalue=None):
    "Collect data into non-overlapping fixed-length chunks or blocks"
    # grouper('ABCDEFG', 3, fillvalue='x') --> ABC DEF Gxx
    # grouper('ABCDEFG', 3, incomplete='strict') --> ABC DEF ValueError
    # grouper('ABCDEFG', 3, incomplete='ignore') --> ABC DEF
    args = [iter(iterable)] * n
    if incomplete == 'fill':
        return zip_longest(*args, fillvalue=fillvalue)
    if incomplete == 'strict':
        return zip(*args, strict=True)
    if incomplete == 'ignore':
        return zip(*args)
    else:
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
        for param, val in grouper(unknown, 2)
        for param_adj in [param.replace('--', '')]
    }

    logging.info(dict_params)

    import main_program
    main_program.main(**dict_params)
