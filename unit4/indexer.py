"""
Indexer part 2 for CS 3308: Information Retrieval

This rather long section of code provides an example of the implementation of an indexer. This code implements some, but not all, of the requirements for the Unit 4 development assignment. One key element that it does not implement is blocking of the data. Your indexer must have the capability
to process very large collections. This code maintains the entire index in memory and then writes the entire index to disk at the end.

Your indexer must process no more than 50,000 terms at a time in memory (as was specified in the Unit 2 assignment), write them to disk, and then be able to process the next 50,000 terms. To simplify the code, the 50,000 term limit is approximate and you may want to write to disk only at the end of the current document being indexed.

In this part 2 indexer assignment, there is important new functionality required to be added to your part 1 assignment.

First you must define a list of between 50 and 100 stop words and develop the functionality within your program to ignore any term that matches a stop
word. If you recall, stop words are common words that have little discriminating value in the index and so we do not include them to improve the processing efficiency of our inverted index for searches

Second your indexer must perform more editing on the terms including the following: Ignore any term that begins with a punctionary character
Ignore any term that is a number
Ignore any term that is 2 characters or shorter in length

Third your indexer must compute and store the tf-idf 'vector' calculated for each term stored in the dictionary
Finally your indexer must implement the data model outlined as part of the assignment """
import sys, os, re
import math
import sqlite3
import time

# the database is a simple dictionnary
database = {}

# regular expression for: extract words, extract ID from path, check for hexa value
chars = re.compile(r'\W+')
pattid = re.compile(r'(\d{3})/(\d{3})/(\d{3})')

# the higher ID
tokens = 0
documents = 0
terms = 0

#
# We will create a term object for each unique instance of a term
#
class Term():
    termid = 0
    termfreq = 0
    docs = 0
    docids = {}

# split on any chars
def splitchars(line):
    return chars.split(line)

# process the tokens of the source code
def parsetoken(line):
    global documents
    global tokens
    global terms

    # this replaces any tab characters with a space character in the line
    # read from the file
    line = line.replace('\t', ' ')
    line = line.strip()

    #
    # This routine splits the contents of the line into tokens
    l = splitchars(line)

    # for each token in the line process
    for elmt in l:
        # This statement removes the newline character if found
        elmt = elmt.replace('\n', '')

        # This statement converts all letters to lower case
        lowerElmt = elmt.lower().strip()

        #
        # Increment the counter of the number of tokens processed. This value will
        # provide the total size of the corpus in terms of the number of terms in the
        # entire collection
        #
        tokens += 1

        # if the term doesn't currently exist in the term dictionary
        # then add the term
        if not (lowerElmt in database.keys()):
            terms += 1
            database[lowerElmt] = Term()
            database[lowerElmt].termid = terms
            database[lowerElmt].docids = dict()
            database[lowerElmt].docs = 0

        # if the document is not currently in the postings
        # list for the term then add it
        #
        if not (documents in database[lowerElmt].docids.keys()):
            database[lowerElmt].docs += 1
            database[lowerElmt].docids[documents] = 0

        # Increment the counter that tracks the term frequency
        database[lowerElmt].docids[documents] += 1

    return l

#
# Open and read the file line by line, parsing for tokens and processing. All of the tokenizing

# is done in the parsetoken() function. You should design your indexer program keeping the tokenizing
# as a separate function that will process a string as you will be able to reuse code for
# future assignments
#
def process(filename):
    try:
        file = open(filename, 'r')
    except IOError:
        print
        "Error in file %s" % filename
        return False
    else:
        for l in file.readlines():
            parsetoken(l)
    file.close()

#
# This function will scan through the specified directory structure selecting
# every file for processing by the tokenizer function
# Notices how this is a recursive function in that it calls itself to process
# sub-directories.
#
def walkdir(cur, dirname):
    global documents

    all = {}
    all = [f for f in os.listdir(dirname) if os.path.isdir(os.path.join(dirname, f)) or os.path.isfile(os.path.join(dirname, f))]
    for f in all:
        if os.path.isdir(dirname + '/' + f):
            walkdir(cur, dirname + '/' + f)
        else:
            documents += 1
            cur.execute("insert into DocumentDictionary values (?, ?)", (dirname + '/' + f, documents))
            process(dirname + '/' + f)

    return True


"""
==========================================================================================
>>> main

This section is the 'main' or starting point of the indexer program. The python interpreter will find this 'main' routine and execute it first.
==========================================================================================

"""
if __name__ == "__main__":
    #
    # Capture the start time of the routine so that we can determine the total running
    # time required to process the corpus
    #
    t2 = time.localtime()
    print('Start Time: %.2d:%.2d' % (t2.tm_hour, t2.tm_min))

    #
    # The corpus of documents must be extracted from the zip file and placed into the C:\corpus
    # directory or another directory that you choose. If you use another directory make sure that
    # you point folder to the appropriate directory.
    #
    folder = "cacm"

    #
    # Create a sqlite database to hold the inverted index. The isolation_level statment turns

    # on autocommit which means that changes made in the database are committed automatically
    #
    con = sqlite3.connect("cacm.db")
    con.isolation_level = None
    cur = con.cursor()

    #
    # In the following section three tables and their associated indexes will be created.
    # Before we create the table or index we will attempt to drop any existing tables in
    # case they exist
    #

    # Document Dictionary Table
    cur.execute("drop table if exists DocumentDictionary")
    cur.execute("drop index if exists idxDocumentDictionary")
    cur.execute("create table if not exists DocumentDictionary (DocumentName text, DocId int)")
    cur.execute("create index if not exists idxDocumentDictionary on DocumentDictionary (DocId)")

    # Term Dictionary Table
    cur.execute("drop table if exists TermDictionary")
    cur.execute("drop index if exists idxTermDictionary")
    cur.execute("create table if not exists TermDictionary (Term text, TermId int)")
    cur.execute("create index if not exists idxTermDictionary on TermDictionary (TermId)")

    # Postings Table
    cur.execute("drop table if exists Posting")
    cur.execute("drop index if exists idxPosting1")
    cur.execute("drop index if exists idxPosting2")
    cur.execute("create table if not exists Posting (TermId int, DocId int, tfidf real, docfreq int, termfreq int)")
    cur.execute("create index if not exists idxPosting1 on Posting (TermId)")
    cur.execute("create index if not exists idxPosting2 on Posting (Docid)")

    #
    # The walkdir method essentially executes the indexer. The walkdir method will
    # read the corpus directory, Scan all files, parse tokens, and create the inverted index.
    #
    walkdir(cur, folder)

    t2 = time.localtime()
    print('Indexing Complete, write to disk: %.2d:%.2d' % (t2.tm_hour, t2.tm_min))

    #
    # Create the inverted index tables.
    #
    # Insert a row into the TermDictionary for each unique term along with a termid which is
    # a integer assigned to each term by incrementing an integer
    #
    # Insert a row into the posting table for each unique combination of Docid and termid
    #
    #

    #
    # Commit changes to the database and close the connection
    #
    con.commit()
    con.close()

    #
    # Print processing statistics
    #
    print("Documents %i" % documents)
    print("Terms %i" % terms)
    print("Tokens %i" % tokens)
    t2 = time.localtime()
    print('End Time: %.2d:%.2d' % (t2.tm_hour, t2.tm_min))
