# -*- coding: utf-8 -*-
# ----------------------------------------------------------------------
# Copyright (c) 2021
#
# See the LICENSE file for details
# see the AUTHORS file for authors
# ----------------------------------------------------------------------

#--------------------
# System wide imports
# -------------------

import sys
import argparse
import os.path
import logging
import importlib
#import logging.handlers
import traceback

# -------------
# Local imports
# -------------

from . import  __version__, DEFAULT_TPS, DEFAULT_LIMIT, DEFAULT_START_DATE, DEFAULT_END_DATE, DEFAULT_PGSZ


# -----------------------
# Module global variables
# -----------------------

log = logging.getLogger("action")

# -----------------------
# Module global functions
# -----------------------

def configureLogging(options):
    if options.verbose:
        level = logging.DEBUG
    elif options.quiet:
        level = logging.WARN
    else:
        level = logging.INFO
    
    log.setLevel(level)
    # Log formatter
    #fmt = logging.Formatter('%(asctime)s - %(name)s [%(levelname)s] %(message)s')
    fmt = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
    # create console handler and set level to debug
    if not options.no_console:
        ch = logging.StreamHandler()
        ch.setFormatter(fmt)
        ch.setLevel(level)
        log.addHandler(ch)
    # Create a file handler
    if options.log_file:
        #fh = logging.handlers.WatchedFileHandler(options.log_file)
        fh = logging.FileHandler(options.log_file)
        fh.setFormatter(fmt)
        fh.setLevel(level)
        log.addHandler(fh)


def python2_warning():
    if sys.version_info[0] < 3:
        log.warning("This software des not run under Python 2 !")


def setup(options):
    python2_warning()
    
def mkdate(datestr):
    try:
        date = datetime.datetime.strptime(datestr, '%Y-%m-%d')
    except ValueError:
        date = datetime.datetime.strptime(datestr, '%Y-%m-%dT%H:%M:%S')
    return date


# =================== #
# THE ARGUMENT PARSER #
# =================== #

def createParser():
    # create the top-level parser
    name = os.path.split(os.path.dirname(sys.argv[0]))[-1]
    parser    = argparse.ArgumentParser(prog=name, description="ZOONIVERSE TOOL")

    # Global options
    parser.add_argument('--version', action='version', version='{0} {1}'.format(name, __version__))
    group = parser.add_mutually_exclusive_group()
    group.add_argument('-v', '--verbose', action='store_true', help='Verbose output.')
    group.add_argument('-q', '--quiet',   action='store_true', help='Quiet output.')
    parser.add_argument('-nk','--no-console', action='store_true', help='Do not log to console.')
    parser.add_argument('--log-file', type=str, default=None, help='Optional log file')

    
    # --------------------------
    # Create first level parsers
    # --------------------------
    subparser = parser.add_subparsers(dest='command')
    parser_observations    = subparser.add_parser('observations',   help='ACTION database commans')
    

    # ----------------
    # Project Commands
    # ----------------

    subparser = parser_observations.add_subparsers(dest='subcommand')
    
    parser_download = subparser.add_parser('download', help='Export project classifications')
    parser_download.add_argument('-t','--token',      type=str, required=True, help='ACTION database token')
    parser_download.add_argument('-f','--file',       type=str, required=True, help='Output JSON file where to save observations')
    parser_download.add_argument('-p','--project',    type=str, required=True, help='Project where to get observations from DB')
    parser_download.add_argument('-s','--start-date', type=mkdate, metavar='<YYYY-MM-DD|YYYY-MM-DDTHH:MM:SS>', default=DEFAULT_START_DATE, help='start date')
    parser_download.add_argument('-e','--end-date',   type=mkdate, metavar='<YYYY-MM-DD|YYYY-MM-DDTHH:MM:SS>', default=DEFAULT_END_DATE, help='end date')
    parser_download.add_argument('-l','--limit',      type=int, default=DEFAULT_LIMIT,  help='max number of observations to download')
    parser_download.add_argument('--page-size',       type=int, default=DEFAULT_PGSZ,  help='Page size for individual HTTP request')
   

    parser_upload = subparser.add_parser('upload', help='Export project classifications')
    parser_upload.add_argument('-t','--token', type=str, required=True, help='ACTION database token')
    parser_upload.add_argument('-f','--file',  type=str, required=True, help='Input JSON file where to upload observations')
    parser_upload.add_argument('--tps',        type=float, default=DEFAULT_TPS,  help='Transactions per second')
    parser_upload.add_argument('--page-size',  type=int, default=DEFAULT_PGSZ,  help='Page size for individual HTTP request')

    return parser
    

# ================ #
# MAIN ENTRY POINT #
# ================ #

def main():
    '''
    Utility entry point
    '''
    try:
        options = createParser().parse_args(sys.argv[1:])
        configureLogging(options)
        setup(options)
        name = os.path.split(os.path.dirname(sys.argv[0]))[-1]
        command  = f"{options.command}"
        subcommand = options.subcommand
        try:
            command = importlib.import_module(command, package=name)
        except ModuleNotFoundError: # when debugging module in git source tree ...
            command  = f".{options.command}"
            command = importlib.import_module(command, package=name)
        log.info(f"============== {name} {__version__} ==============")
        getattr(command, subcommand)(options)
    except KeyboardInterrupt as e:
        log.critical("[%s] Interrupted by user ", __name__)
    except Exception as e:
        log.critical("[%s] Fatal error => %s", __name__, str(e) )
        traceback.print_exc()
    finally:
        pass

main()

