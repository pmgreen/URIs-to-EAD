#!/usr/bin/env python
#-*- coding: utf-8 -*-
"""
A simple, one-off, experimental script to get OCLC Work IDs (OWIs) into a batch of bib records. Here, they went into 787$o.
Uses pymarc, libxml2 and xmllint.
NOTE: There's a quota of 1,000 queries per day by default (this isn't immediately obvious). Check the following:
http://oclc.org/developer/develop/linked-data/worldcat-entities/worldcat-work-entity.en.html
http://www.oclc.org/developer/develop/web-services/xid-api.en.html
"""
from time import sleep
import libxml2
import os
import pickle
import pymarc
import requests
import shelve
import subprocess
import sys

XID_RESOLVER = "http://xisbn.worldcat.org/webservices/xid/oclcnum/%s"
WORK_ID = "http://worldcat.org/entity/work/id/"
SHELF_FILE = "./owi.db"

infile = "./input.marc.xml"
outfile = "./output_w_owis.marc.xml"

def check_shelf(ocn):
	shelf = shelve.open(SHELF_FILE, protocol=pickle.HIGHEST_PROTOCOL)
	if ocn in shelf:
		workid = shelf[ocn]
		os.sys.stdout.write("[Cache] Found: " + ocn + "\n") 
	else:
		workid = query_oclc(ocn)
		if workid != None and workid != '':
			shelf[ocn] = workid
			os.sys.stdout.write('put %s %s into db\n' % (ocn,workid))
	shelf.close()
	return workid

def query_oclc(xid):
	'''
	See the following for parameters:
	http://www.oclc.org/developer/develop/web-services/xid-api/xstandardNumber-resource.en.html
	'''
	to_get = XID_RESOLVER % xid
	to_get += "?method=getMetadata&format=xml&fl=*" # could also try &fl=owi
	print(to_get) # uncomment to get the full request URI
	headers = {"Accept":"application/xml"}
	resp = requests.get(to_get, headers=headers, allow_redirects=True)
	if resp.status_code == 200:
		doc = libxml2.parseDoc(resp.text.encode("UTF-8", errors="ignore"))
		ctxt = doc.xpathNewContext()
		if ctxt.xpathEval("//@stat[.='overlimit']"):
			print("over limit with %s" % xid)
			sys.exit() 
		else: 
			try:
				owi = ctxt.xpathEval("//@owi")[0].content
				cleanowi = owi.replace('owi','')
				return WORK_ID + cleanowi
			except:
				print("no owi found")
				
	elif resp.status_code == 404:
		msg = "Not found: %s%s" % (xid, os.linesep)
	elif resp.status_code == 500:
		msg = "Server error (%s)" % xid
	else: # resp.status_code isn't 200, 404 or 500:
		msg = " Response for %s was " % xid
		msg += "%s%s" % (resp.status_code, os.linesep)
	print(msg)
	
	sleep(1)
	
	
if __name__ == "__main__":
	mrxheader = """<?xml version="1.0" encoding="UTF-8" ?>
<collection xmlns="http://www.loc.gov/MARC21/slim" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.loc.gov/MARC21/slim http://www.loc.gov/standards/marcxml/schema/MARC21slim.xsd">"""
	fh = open('out/owi_tmp.xml', 'w+')
	fh.write(mrxheader)
	reader = pymarc.marcxml.parse_xml_to_array(infile)
	for rec in reader:
		for n in rec.get_fields('035'):
			for s in n.get_subfields('a'):
				if 'OCoLC' in s:
					num = s.replace('(OCoLC)','')
					workid = check_shelf(str(num))
		if workid != None and workid != '':
			field = pymarc.Field(
				tag = '787', 
				indicators = ['0',' '],
				subfields = [
					'o', str(workid)
				])
			rec.add_field(field)
		workid = ""
		out = "%s" % (pymarc.record_to_xml(rec))
		fh.write(out)
	fh.write("</collection>")
	fh.close()
	# format output for readability
	subprocess.Popen(['xmllint','--format','-o', outfile,'owi_tmp.xml'])
