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
#import logging.handlers
import traceback
import importlib

# -------------
# Local imports
# -------------

from . import  __version__, DEFAULT_TIMEOUT, DEFAULT_LANGUAGE

# -----------------------
# Module global variables
# -----------------------

log = logging.getLogger("zoonis")

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
	parser_project    = subparser.add_parser('project',    help='Zooniverse project commands')
	parser_subjectset = subparser.add_parser('subjectset', help='Zooniverse SubjectSet commands')
	parser_workflow   = subparser.add_parser('workflow',   help='Zooniverse Workflow commands')

	# ----------------
	# Project Commands
	# ----------------

	subparser = parser_project.add_subparsers(dest='subcommand')

	parser_find = subparser.add_parser('find', help='Find project by URL fragment')
	parser_find.add_argument('-u','--username',  type=str, required=True, help='Zooniverse username')
	parser_find.add_argument('-pw','--password', type=str, required=True, help='Zooniverse password')
	parser_find.add_argument('-p','--project',   type=str, required=True, help='Zooniverse Project slug')
	
	parser_create = subparser.add_parser('create', help='Create a new project')
	parser_create.add_argument('-u','--username',    type=str, required=True, help='Zooniverse username')
	parser_create.add_argument('-pw','--password',   type=str, required=True, help='Zooniverse password')
	parser_create.add_argument('-n','--name',        type=str, nargs='+', required=True, help='Project name')
	parser_create.add_argument('-d','--description', type=str, nargs='+', required=True, help='Project description')
	parser_create.add_argument('-p','--private',     action='store_true', default=False, help='Project private flag')
	parser_create.add_argument('-l','--language',    type=str, default=DEFAULT_LANGUAGE,  help='Primary language')

	parser_export = subparser.add_parser('export', help='Export project classifications')
	parser_export.add_argument('-u','--username',  type=str, required=True, help='Zooniverse username')
	parser_export.add_argument('-pw','--password', type=str, required=True, help='Zooniverse password')
	parser_export.add_argument('-p','--project',   type=str, required=True, help='Zooniverse Project slug')
	parser_export.add_argument('-f','--file',      type=str, required=True, help='Output file where to save export as JSON lines')
	parser_export.add_argument('-g','--generate', action='store_true',  help='Generates new report, otherwise download last report')
	parser_export.add_argument('-w','--wait',     action='store_true',  help='Wait for an in-progress export to finish, if there is one. No effect if -g is specified')
	parser_export.add_argument('-t','--timeout',  type=int, default=DEFAULT_TIMEOUT,  help='Wait timeout in seconds')


	parser_classi = subparser.add_parser('classifications', help='Export project classifications')
	parser_classi.add_argument('-u','--username',  type=str, required=True, help='Zooniverse username')
	parser_classi.add_argument('-pw','--password', type=str, required=True, help='Zooniverse password')
	parser_classi.add_argument('-p','--project',   type=str, required=True, help='Zooniverse Project slug')
	parser_classi.add_argument('-f','--file',      type=str, required=True, help='Output file where to save export as JSON lines')
	
	# -----------------
	# Workflow Commands
	# -----------------

	subparser = parser_workflow.add_subparsers(dest='subcommand')

	parser_create = subparser.add_parser('create', help='Create a new workflow')
	parser_create.add_argument('-u','--username',    type=str, required=True, help='Zooniverse username')
	parser_create.add_argument('-pw','--password',   type=str, required=True, help='Zooniverse password')
	parser_create.add_argument('-p','--project',     type=str, required=True, help='Zooniverse Project slug')
	parser_create.add_argument('-n','--name',        type=str, nargs='+', required=True, help='Workflow name')
	parser_create.add_argument('-l','--language',    type=str, default=DEFAULT_LANGUAGE,  help='Primary language')

	
	parser_wkflstat = subparser.add_parser('completion', help='Show workflow & subjectsets completion status')
	parser_wkflstat.add_argument('-u','--username',  type=str, required=True, help='Zooniverse username')
	parser_wkflstat.add_argument('-pw','--password',  type=str, required=True, help='Zooniverse password')
	parser_wkflstat.add_argument('-p','--project',   type=str, required=True, help='Zooniverse Project slug')
	parser_wkflstat.add_argument('-f','--file',      type=str, required=True, help='Output file where to save workflow status as JSON lines')

	parser_subjsets = subparser.add_parser('subjectsets', help='List Active Subject Sets for each Workflow')
	parser_subjsets.add_argument('-u','--username',  type=str, required=True, help='Zooniverse username')
	parser_subjsets.add_argument('-pw','--password', type=str, required=True, help='Zooniverse password')
	parser_subjsets.add_argument('-p','--project',   type=str, required=True, help='Zooniverse Project slug')
	parser_subjsets.add_argument('-f','--file',      type=str, required=True, help='Output file where to list subject sets as JSON lines')

	# -----------------------
	#  SubjectSet Subcommands
	# -----------------------
	
	subparser = parser_subjectset.add_subparsers(dest='subcommand')

	parser_create = subparser.add_parser('create',    help='Create a new SubjectSet for a given Project')
	parser_create.add_argument('-n','--name',      type=str, nargs='+', required=True, help='Subject Set name')
	parser_create.add_argument('-u','--username',  type=str, required=True, help='Zooniverse username')
	parser_create.add_argument('-pw','--password', type=str, required=True, help='Zooniverse password')
	parser_create.add_argument('-p','--project',   type=str, required=True, help='Zooniverse Project slug')
	parser_create.add_argument('-f','--file',      type=str, required=True, help='Input JSON file with images & metadata to upload')

	parser_sslog = subparser.add_parser('show', help='List Subject Sets for a given Project')
	parser_sslog.add_argument('-u','--username',  type=str, required=True, help='Zooniverse username')
	parser_sslog.add_argument('-pw','--password', type=str, required=True, help='Zooniverse password')
	parser_sslog.add_argument('-p','--project',   type=str, required=True, help='Zooniverse Project slug')

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
		except ModuleNotFoundError:	# when debugging module in git source tree ...
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

