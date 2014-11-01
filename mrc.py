#!/usr/bin/env python
#-*- coding: utf-8 -*-
"""
Based on URIs-to-EAD, gets uris from id.loc.gov into MaRC bib records.
"""
from argparse import ArgumentParser, RawTextHelpFormatter, RawDescriptionHelpFormatter
from lxml import html
from sys import exit
from time import sleep, strftime
import ConfigParser
import httplib
import libxml2
import logging
import os
import pickle
import pymarc
import rdflib
import re
import requests
import shelve
import subprocess

# TODOs:
# - input / output mrk or mrc or mrx: test file extension
# - output mrc, mrk and marcxml
# - remove -o, output, automatically using name of input file (but leave option for new name)
# - VIAF?
# - logging
# - timestamp to cache

today = strftime('%Y%m%d')
ID_SUBJECT_RESOLVER = "http://id.loc.gov/authorities/label/"
RSS_XML = "application/rss+xml" 
APPLICATION_XML = "application/xml"
CONFIG = "./cfg/mrc.cfg"
SHELF_FILE = "./db/cache.db"
JOB_LOG = './log/jobs.log'
LOG_FILENAME = "./log/alts.log"
LOG_FORMAT = "%(asctime)s %(filename)s %(message)s"
REPORTS = "./reports/"
OUTDIR = "./out/"
LOGDIR = "./log/"
INDIR = "./in/"

# Generate batch no. for reports e.g. 0000000001_yyyymmdd
if not os.path.isfile(JOB_LOG):
	try:
		f = open(JOB_LOG, "w")
	except IOError:
		pass

if os.stat(JOB_LOG)[6]<=1:
	run = "%010d" % (1,)
	run = "%s_%s" % (str(run), today)
else:
	with open(JOB_LOG,'r+b') as jr:
		last = jr.readlines()[-1].decode()
		run = "%010d" % (int(last.split("_")[0])+1)
		run = "%s_%s" % (str(run), today)
		
with open(JOB_LOG,'a+b') as jr:
	jr.write(run+'\n')
	
thisrun = REPORTS + 'mrc_uris_'+run+'.tsv'

#===============================================================================
# HeadingNotFoundException
#===============================================================================
class HeadingNotFoundException(Exception):
	def __init__(self, msg, heading, type, instead=None):
		super(HeadingNotFoundException, self).__init__(msg)
		"""
		@param msg: Message for logging
		@param heading: The heading we were searching when this was raised
		@param type: The type of heading (personal or corporate) 
		@param instead: The "Use instead" URI and string when heading is deprecated
		"""
		self.heading = heading
		self.type = type
		self.instead = instead

#===============================================================================
# MultipleMatchesException
#===============================================================================
class MultipleMatchesException(Exception):
	def __init__(self, msg, heading, type, items):
		super(MultipleMatchesException, self).__init__(msg)
		"""
		@param msg: Message for logging
		@param heading: The heading we were searching when this was raised
		@param type: The type of heading (personal or corporate)  
		@param items: A list of 2-tuple (uri, label) possibilities
		"""
		self.heading = heading
		self.type = type
		self.items = items

#===============================================================================
# UnexpectedResponseException
#===============================================================================
# we throw when we get an enexpected (unhandled) HTTP response
class UnexpectedResponseException(Exception): pass

#===============================================================================
# Heading
#===============================================================================
class Heading(object):
	def __init__(self):
		self.value = ""
		"""Heading label (string) normalized from the source data"""
		self.type = ""
		"""'corporate', 'personal', or 'subject'"""
		self.found = ""
		"""boolean, True if one or more URIs was found"""
		self.alternatives = ""
		""""A list of 2-tuple (uri, label) possibilities"""

#===============================================================================
# setup
#===============================================================================
def setup():
	"""
	@note: Create log, out, report, and db dirs, if they don't exist.
	"""
	if not os.path.isdir(LOGDIR):
		os.mkdir(LOGDIR)
	if not os.path.isdir(OUTDIR):
		os.mkdir(OUTDIR)
	if not os.path.isdir(INDIR):
		os.mkdir(INDIR)
	if not os.path.isdir(REPORTS):
		os.mkdir(REPORTS)

#===============================================================================
# _normalize_heading
#===============================================================================
def _normalize_heading(heading):
	"""
	@param heading: A heading from the source data.
	@return: A normalized version of the heading.
	 
	@note: 	Other users may need to modify or extend this function. This
	version, in order:
	 1. collapses whitespace
	 2. strips spaces that trail or follow hyphens ("-")
	 3. strips double hyphens when following punctuation ("[\.,]--")
	 4. strips trailing stops (".")
	"""
	collapsed = " ".join(heading.split()).replace(" -", "-").replace("- ", "-").replace(",--",", ").replace("--("," (").replace(' d. ',' -').replace('.--','--')
	abbrev = re.search("[\.\s][A-Z][a-z]?\.$",collapsed)
	a = ' '
	if abbrev is not None:
		a = abbrev.group(0)
	if (collapsed.endswith(".") or collapsed.endswith(",")) and not collapsed.endswith("etc.") and not collapsed.endswith(a):
		stripped = collapsed[:-1]
	else:
		stripped = collapsed
	return stripped
		
#===============================================================================
# query_lc
#===============================================================================
def query_lc(subject):
	"""
	@param subject: a name or subject heading
	@type subject: string
	
	@raise HeadingNotFoundException: when the heading isn't found
	
	@raise UnexpectedResponseException: when the initial response from LC is not 
		a 302 or 404 (404 should raise a HeadingNotFoundException)
	
	"""
	to_get = ID_SUBJECT_RESOLVER + subject
	headers = {"Accept":"application/xml"}
	resp = requests.get(to_get, headers=headers, allow_redirects=True)
	if resp.status_code == 200:
		uri = resp.headers["x-uri"]
		try: 
			label = resp.headers["x-preflabel"]
		except: # x-preflabel is not returned for deprecated headings
			msg = "Not found (lc; deprecated): " + subject + os.linesep
			tree = html.fromstring(resp.text)
			see = tree.xpath("//h3/text()= 'Use Instead'") # this info isn't in the header, so grabbing from html
			seeother = ''
			if see:
				other = tree.xpath("//h3[text() = 'Use Instead']/following-sibling::ul//div/a")[0]
				seeother = (other.attrib['href'], other.text)
			raise HeadingNotFoundException(msg, subject, 'subject',seeother) # put the see other url and value into the db
		return uri, label
	elif resp.status_code == 404:
		msg = "Not found (lc): " + subject + os.linesep
		raise HeadingNotFoundException(msg, subject, 'subject')
	else: # resp.status_code != 404 and status != 200:
		msg = " Response for \"" + subject + "\" was "
		msg += resp.status_code + os.linesep
		raise UnexpectedResponseException(msg)

#===============================================================================
# update_headings
#===============================================================================
def _update_headings(bib, scheme, h, ctxt, shelf, tag, annotate=False, verbose=False, mrx=False, log=False, ignore_cache=False):
	
	uri = ""

	try:
		heading_type = ""
		heading = _normalize_heading(h)
			
		# Check the shelf right off
		if ignore_cache==False and heading in shelf:
			cached = shelf[heading]
			if cached.found == True and len(cached.alternatives) == 1:
				## we only get here if no exceptions above 
				if verbose:	os.sys.stdout.write("[Cache] Found: " + heading + "\n") 
				uri = cached.alternatives[0][0]
				if 'authorities/classification' not in uri:
					if (scheme == 'nam' and 'authorities/names' in uri) or (scheme == 'sub' and 'authorities/subjects' in uri): 
						pymarc.Field.add_subfield(ctxt,"0",uri)
			elif len(cached.alternatives) > 1:
				msg = "[Cache] Multiple matches for " + heading + "\n"
				raise MultipleMatchesException(msg, heading, heading_type, cached.alternatives)
			else: # 0 
				msg = "[Cache] Not found: " + heading + "\n"
				raise HeadingNotFoundException(msg, heading, heading_type)
		else:
			uri, auth = query_lc(heading)
			## we only get here if no exceptions above 
			if verbose:	os.sys.stdout.write("Found (lc): " + heading + "\n")
			if 'authorities/classification' not in uri:
					if (scheme == 'nam' and 'authorities/names' in uri) or (scheme == 'sub' and 'authorities/subjects' in uri): 
						pymarc.Field.add_subfield(ctxt,"0",uri)
			# we put the heading we found in the db
			record = Heading()
			record.value = heading
			record.type = type
			record.found = True
			record.alternatives = [(uri, auth)]
			shelf[heading] = record
			
			sleep(1) # A courtesy to the services.
			
	except UnexpectedResponseException, e:
		os.sys.stderr.write(str(e))
	
	except HeadingNotFoundException, e:
		if verbose:
			os.sys.stderr.write(str(e))
		if not heading in shelf:
			# We still want to put this in the db
			record = Heading()
			record.type = e.type
			record.found = False
			if e.instead != None:
				record.value = '(DEPRECATED) ' + e.heading
				record.alternatives = [e.instead]
			else:
				record.value = e.heading
				record.alternatives = []
			shelf[heading] = record
	
	except MultipleMatchesException, m:
		if verbose:
			os.sys.stderr.write(str(m)) 
		if annotate:
			content = os.linesep + "Possible URIs:" + os.linesep
			for alt in m.items:
				# we need to replace "--" in headings in comments so that the 
				# the doc stays well-formed
				content += alt[0].replace("--", "-\-") + " : " + \
				alt[1].replace("--", "-\-") + os.linesep 
			comment = libxml2.newComment(content)
			node.addNextSibling(comment)
		if log: 
			# NOTE: using known-label service, only one uri is ever returned. See: http://id.loc.gov/techcenter/searching.html.
			logging.basicConfig(filename=LOG_FILENAME,level=logging.INFO,format=LOG_FORMAT)
			content = os.linesep + "Possible URIs:" + os.linesep
			for alt in m.items:
				content += alt[0].replace("--", "-\-") + " : " + \
				alt[1].replace("--", "-\-") + os.linesep 
			logging.info(content)
		if not heading in shelf:
			# We still want to put this in the db
			record = Heading()
			record.value = m.heading
			record.type = m.type
			record.found = True
			record.alternatives = m.items
			shelf[heading] = record

	except LookupError, e:
		record = Heading()
		record.value = heading
		record.type = heading_type
		record.found = False
		record.alternatives = []
		shelf[heading] = record
		e.message = "Error: " + e.message 
		raise e
		
	# write bib and heading to report
	bh = str(bib)+'\t'+str(tag)+'\t'+str(heading)+'\t'+str(uri)+'\n'
	with open(thisrun,'a+b') as br:
		if bh not in br:
			br.write(bh)

class CLI(object):
	EX_OK = 0
	"""All good"""

	EX_SOMETHING_ELSE = 9 
	"""Something unanticipated went wrong"""
		
	EX_WRONG_USAGE = 64
	"""The command was used incorrectly, e.g., with the wrong number of 
	arguments, a bad flag, a bad syntax in a parameter, or whatever.""" 

	EX_DATA_ERR = 65
	"""The input data was incorrect in some way."""
		
	EX_NO_INPUT = 66
	"""Input file (not a system file) did not exist or was not readable."""
	
	EX_SOFTWARE = 70
	"""An internal software (not OS) error has been detected."""
	
	EX_CANT_CREATE = 73
	"""User specified output file cannot be created."""
	
	EX_IOERR = 74
	"""An error occurred while doing I/O on some file."""
		
	def __init__(self):
		
		setup()
					
		# start by assuming something will go wrong:
		status = CLI.EX_SOMETHING_ELSE
		
		desc = "Adds id.loc.gov URIs to subject and " + \
				"name headings when established forms can be found. Works with EAD or MaRCXML files."
		
		# note: defaults in config file can be overridden by args on commandline
		# argparse...
		epi = """Exit statuses:
		 0 = All good
		 9 = Something unanticipated went wrong
		64 = The command was used incorrectly, e.g., with the wrong number of arguments, a bad flag, a bad syntax in a parameter, or whatever.
		65 = The input data was incorrect in some way.
		66 = Input file (not a system file) did not exist or was not readable.
		70 = An internal software (not OS) error has been detected.
		73 = User specified output file cannot be created.
		74 = An error occurred while doing I/O on some file.
		"""
		
		rHelp = "The input file."
		
		mHelp = "The input file is MaRCXML rather than EAD. Found URIs are put into $0."
	
		oHelp = "Path to the output file. Writes to stdout if no option " + \
			"is supplied."
		
		nHelp = "Try to find URIs for names."
		
		sHelp = "Try to find URIs for subjects."
	
		aHelp = "Annotate the record. When multiple matches are found XML " + \
			"comments containing the matches and their URIs will be added " + \
			"to the record."
			
		vHelp = "Print messages to stdout (one-hit headings) and stderr " + \
			"(zero or more than one hit headings)."
			
		cHelp = "Does just what it says.\n"	
		
		lHelp = "Log alternatives.\n"
		
		cfgHelp = "Specify the config file. Defaults can be overridden. " + \
			"At minimum, run e.g.: python addauths.py myfile.marc.xml"
					
		conf_parser = ArgumentParser(add_help=False, description=desc)
		conf_parser.add_argument("-c", "--conf_file", default=CONFIG, required=False, dest="conf_file", help=cfgHelp)
		args, remaining_argv = conf_parser.parse_known_args()
		defaults = {
			"marc" : False,
			"outpath": None,
			"names" : False,
			"subjects" : False,
			"annotate" : False,
			"verbose" : False,
			"ignore_cache" : False,
			"record": None,
			"log" : False
		}
		# if -c or --conf_file, override the defaults above
		if args.conf_file:
			config = ConfigParser.SafeConfigParser()
			config.read([args.conf_file])
			cfgdict = dict(config.items('Paths')) # Paths section of config file
			booldict = dict(config.items('Booleans')) # Booleans section of config file
			for k,v in booldict.iteritems():
				# need to get the booleans as booleans, not as 'strings'
				boo = config.getboolean('Booleans',k)
				cfgdict[k]=boo
			defaults = cfgdict

		parser = ArgumentParser(parents=[conf_parser],description=desc,formatter_class=RawDescriptionHelpFormatter,epilog=epi)
		parser.set_defaults(**defaults)
		parser.add_argument("-m", "--marc", required=False, dest="mrx", action="store_true", help=mHelp)
		parser.add_argument("-o", "--output", required=False, dest="outpath", help=oHelp)
		parser.add_argument("-n", "--names", required=False, dest="names", action="store_true", help=nHelp)
		parser.add_argument("-s", "--subjects", required=False, dest="subjects", action="store_true", help=sHelp)
		parser.add_argument("-a", "--annotate", required=False, dest="annotate", action="store_true", help=aHelp)
		parser.add_argument("-v", "--verbose", required=False, dest="verbose", action="store_true", help=vHelp)
		parser.add_argument("-C", "--ignore-cache",required=False, dest="ignore_cache", action="store_true", help=cHelp)
		parser.add_argument("-l", "--log",required=False, dest="log", action="store_true", help=lHelp)
		parser.add_argument("-f", "--file",required=True, dest="record", help=rHelp)
		args = parser.parse_args(remaining_argv)

		# TODO args to log (along with batch no.) -pmg		
		print(args)

		#=======================================================================
		# Checks on our args and options. We can exit before we do any work.
		#=======================================================================
		if not os.path.exists(args.record):
			os.sys.stderr.write("File " + args.record + " does not exist\n")
			exit(CLI.EX_NO_INPUT)
			
		if args.record == None:
			os.sys.stderr.write("No input file supplied. See --help for usage\n")
			exit(CLI.EX_WRONG_USAGE)
	
		if not args.names and not args.subjects:
			msg = "Supply -n and or -s to link headings. Use --help " + \
			"for more details.\n"
			os.sys.stderr.write(msg)
			exit(CLI.EX_WRONG_USAGE)
			
		if args.mrx == True:
			marc_path = args.record
			# a quick and dirty test...
			reader = pymarc.marcxml.parse_xml_to_array(marc_path)
			if not reader:
				msg = "-m flag used but input file isn't MaRCXML.\n"
				os.sys.stderr.write(msg)
				exit(CLI.EX_WRONG_USAGE)
	
		if args.outpath:
			outdir = os.path.dirname(args.outpath)
			if not os.path.exists(outdir):
				msg = "Directory " + outdir + " does not exist\n"
				os.sys.stderr.write(msg)
				exit(CLI.EX_CANT_CREATE)
			if not os.access(outdir, os.W_OK):
				msg = "Output directory " + outdir + " not writable\n"
				os.sys.stderr.write(msg) 
				exit(CLI.EX_CANT_CREATE)

		#=======================================================================
		# The work...
		#=======================================================================
		shelf = shelve.open(SHELF_FILE, protocol=pickle.HIGHEST_PROTOCOL)
		ctxt = None
		mrx_subs = []
		h = ""
		mrxheader = """<?xml version="1.0" encoding="UTF-8" ?>
<collection xmlns="http://www.loc.gov/MARC21/slim" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.loc.gov/MARC21/slim http://www.loc.gov/standards/marcxml/schema/MARC21slim.xsd">"""
		try:
			reader = pymarc.marcxml.parse_xml_to_array(args.record)
			#writer = codecs.open("test_out.marc.xml", 'w', 'utf-8')
			options = {'annotate':args.annotate, 'verbose':args.verbose, 'mrx':args.mrx, 'log':args.log, 'ignore_cache':args.ignore_cache}
			fh = open(OUTDIR+'tmp.xml', 'wb+')
			fh.write(mrxheader)
			for rec in reader:
				f001 = rec.get_fields('001')
				for b in f001:
					bbid = b.value()
				if args.names:
					#=======================
					# NAMES
					#=======================
					# get names data from these subfields
					namesubf = ['a','c','d','q']
					names = ['100','110','130','700','710','730']
					for n in rec.get_fields(*names):
						for s in n.get_subfields(*namesubf):
							s = s.encode('utf8')
							mrx_subs.append(s)
						h = "--".join(mrx_subs)
						tag = n.tag
						_update_headings(bbid, 'nam', h, n, shelf, tag, **options)
						mrx_subs = []
				if args.subjects:
					#=======================
					# SUBJECTS
					#=======================
					# get subjects data from these subfields (all but 0,2,3,6,8)
					subs = ['600','610','611','630','650','651']
					subsubf = ['a', 'b', 'c', 'd', 'f', 'g', 'h', 'j', 'k', 'l', 
					'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'x', 'y', 'z', '4'] 
					for f in rec.get_fields(*subs):
						for s in f.get_subfields(*subsubf):
							s = s.encode('utf8')
							mrx_subs.append(s)
						h = "--".join(mrx_subs)
						tag = f.tag
						_update_headings(bbid, 'sub', h, f, shelf, tag, **options)
						mrx_subs = []
				out = "%s" % (pymarc.record_to_xml(rec))
				fh.write(out)
			fh.write("</collection>")
			fh.close()
			
			if args.outpath == None:
				os.sys.stdout.write(out)

			# if we got here...
			status = CLI.EX_OK

		#=======================================================================
		# Problems while doing "the work" are handled w/ Exceptions
		#=======================================================================
		except libxml2.parserError, e: # TODO: pymarc exceptions
			os.sys.stderr.write(str(e.message) + "\n")
			status = CLI.EX_DATA_ERR

		except IOError, e:
			os.sys.stderr.write(str(e.message) + "\n")
			status = CLI.EX_IOERR

		except LookupError, e:
			os.sys.stderr.write(str(e.message))
			status = CLI.EX_SOFTWARE
					
		except Exception, e:
			os.sys.stderr.write(str(e.message) + "\n")
			status = CLI.EX_SOMETHING_ELSE
		
		finally:
			# clean up!
			if args.outpath != None:
				subprocess.Popen(['xmllint','--format','-o', args.outpath, OUTDIR+'tmp.xml'])
			shelf.close()
			exit(status)
			
if __name__ == "__main__": CLI()
