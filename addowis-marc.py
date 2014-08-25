#!/usr/bin/env python
#-*- coding: utf-8 -*-
"""
Get WorldCat work ids and put them into a new element in bib records.
Starting out with 787$o.
"""
from sys import exit
from time import sleep
import libxml2
import os
import pickle
import pymarc
import requests
import shelve
import subprocess

# TODOs:
# - argparse
# - log
# - exceptions
# - input and output mrk, mrc, mrx

infile = "test_out.marc.xml"
outfile = "owi_test_out.marc.xml"

XID_RESOLVER = "http://xisbn.worldcat.org/webservices/xid/oclcnum/%s"
WORK_ID = "http://worldcat.org/entity/work/id/"
SHELF_FILE = "owi_cache.db"

def check_shelf(ocn):
	shelf = shelve.open(SHELF_FILE, protocol=pickle.HIGHEST_PROTOCOL)
	if ocn in shelf:
		workid = shelf[ocn]
		os.sys.stdout.write("[Cache] Found: " + ocn + "\n") 
	else:
		workid = query_oclc(ocn)
		shelf[ocn] = workid
		os.sys.stdout.write('put %s %s into db' % (ocn,workid))
	shelf.close()
	return workid

def query_oclc(xid):
	to_get = XID_RESOLVER % xid
	to_get += "?method=getMetadata&format=xml&fl=*"
	headers = {"Accept":"application/xml"}
	resp = requests.get(to_get, headers=headers, allow_redirects=True)
	if resp.status_code == 200:
		doc = libxml2.parseDoc(resp.text.encode("UTF-8", errors="ignore"))
		ctxt = doc.xpathNewContext()
		count = ctxt.xpathEval("//@owi")[0].content
		x = count.replace('owi','')
		return WORK_ID + x
	elif resp.status_code == 404:
		msg = "Not found (in oclc): " + xid + os.linesep
		#raise HeadingNotFoundException(msg, subject, 'subject')
	elif resp.status_code == 500:
		msg = "Server error " + xid
		sleep(1) # try again in a sec.
	else: # resp.status_code != 404 and status != 200 and status != 500:
		msg = " Response for %s was " % xid
		msg += "%s%s" % (resp.status_code, os.linesep)
		#raise UnexpectedResponseException(msg)
		print(msg)
	sleep(1)
	
	
if __name__ == "__main__":
	mrxheader = """<?xml version="1.0" encoding="UTF-8" ?>
<collection xmlns="http://www.loc.gov/MARC21/slim" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.loc.gov/MARC21/slim http://www.loc.gov/standards/marcxml/schema/MARC21slim.xsd">"""
	fh = open('owi_tmp.xml', 'w+')
	fh.write(mrxheader)
	reader = pymarc.marcxml.parse_xml_to_array(infile)
	for rec in reader:
		for n in rec.get_fields('035'):
			for s in n.get_subfields('a'):
				if 'OCoLC' in s:
					num = s.replace('(OCoLC)','')
					workid = check_shelf(str(num))
		if workid != None:
			field = pymarc.Field(
				tag = '787', 
				indicators = ['0',''],
				subfields = [
					'o', str(workid)
				])
			rec.add_field(field)
		workid = ""
		out = "%s" % (pymarc.record_to_xml(rec))
		fh.write(out)
	fh.write("</collection>")
	fh.close()
	subprocess.Popen(['xmllint','--format','-o', outfile,'owi_tmp.xml'])
