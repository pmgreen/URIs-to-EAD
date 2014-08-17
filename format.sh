#!/bin/bash

# Checks well-formedness and if well-formed, formats (does not validate!) the
# metadata records in a directory tree. Pass the path to the tree as the 
# first and only arg to this script.
# /path/to/tree

# Arguments
ROOT=$1

# Dependencies
FIND=/usr/bin/find
XMLLINT=/usr/bin/xmllint
MV=/bin/mv

# Configuration
export XMLLINT_INDENT="   "

# Constants
TMP_XML=/tmp/formatted.xml

# Canned expression
FIND_EXPR="$FIND $ROOT -type f"
XMLLINT_WF_EXPR="$XMLLINT --noout "
XMLLINT_FORMAT_EXPR="$XMLLINT --format --output $TMP_XML"

for record in $($FIND_EXPR); do
	$XMLLINT_WF_EXPR $record

	if [ $? == "0" ]; then 
		$XMLLINT_FORMAT_EXPR $record
		if [ $? == "0" ]; then
			$MV $TMP_XML $record
		else
			echo "Could not format $record; returned status $?"
			echo "See http://xmlsoft.org/xmllint.html for Error Return Codes"
		fi
	else
		echo "Well formed check for $record returned status $?"
		echo "See http://xmlsoft.org/xmllint.html for Error Return Codes"
	fi
done
