"""
Indexer part 2 for CS 3308: Information Retrieval

This rather long section of code provides an example of the implementation of an indexer. This code implements some, but not all, of the requirements for the Unit 4 development assignment. One key element that it does not implement is blocking of the data. Your indexer must have the capability
to process very large collections. This code maintains the entire index in memory and then writes the entire index to disk at the end.

Your indexer must process no more than 50,000 terms at a time in memory (as was specified in the Unit 2 assignment), write them to disk, and then be able to process the next 50,000 terms. To simplify the code, the 50,000 term limit is approximate and you may want to write to disk only at the end of the current document being indexed.

In this part 2 indexer assignment, there is important new functionality required to be added to your part 1 assignment.

First you must define a list of between 50 and 100 stop words and develop the functionality within your program to ignore any term that matches a stop
word. If you recall, stop words are common words that have little discriminating value in the index and so we do not include them to improve the processing efficiency of our inverted index for searches

Second your indexer must perform more editing on the terms including the following: 
Ignore any term that begins with a punctionary character
Ignore any term that is a number
Ignore any term that is 2 characters or shorter in length

Third your indexer must compute and store the tf-idf 'vector' calculated for each term stored in the dictionary
Finally your indexer must implement the data model outlined as part of the assignment 
"""

import sys, os, re
import math
import sqlite3
import time
import io
from porterstemmer import PorterStemmer

enStopwords = {'and', 'ours', 'as', 'am', "mightn't", 'about', 'why', 'most', "you've", 'doing', 'under', 'didn', 'weren', "she's", 'yourselves', "mustn't", 'just', 'won', 'such', 'hadn', 'are', "you'd", 'mustn', "weren't", 'our', 'if', "don't", 'm', 'so', 'here', 'then', 'shan', 'during', 'it', 'were', 'the', 'with', "wasn't", 'yourself', 'off', 'been', 'or', 'for', 'own', 'too', 'nor', 'be', 'y', 'my', "you'll", 'from', 't', 'will', 'll', 'above', 'i', 'o', 'other', 'in', 'how', 'than', 'they', 'has', 'further', 'she', 'having', 'each', 'is', 'me', 'was', 'you', 're', 'until', 'on', 'we', 'by', 'between', 's', 'which', "haven't", 'at', 'ourselves', 'no', 'theirs', "shan't", 'out', 'his', 'this', 'of', 'ma', 'her', "didn't", 'their', 'can', "should've", 'couldn', 'myself', 'more', "that'll", 'there', 'them', 'your', 'ain', "needn't", 'against', 'a', 'wouldn', 'through', 'now', 'him', "isn't", 'whom', 'being', 'did', 'once', "hasn't", 'don', 'herself', 'only', 've', 'all', 'into', 'not', 'while', 'to', 'isn', 'when', "shouldn't", 'very', 'shouldn', 'because', 'needn', 'mightn', 'those', "wouldn't", 'both', 'an', 'below', 'again', "aren't", 'doesn', 'have', 'but', 'do', "won't", 'its', 'themselves', 'that', 'these', 'haven', 'before', 'does', 'hers', 'where', "couldn't", "hadn't", 'he', 'd', 'over', 'up', "doesn't", 'who', 'hasn', 'down', 'any', 'aren', "it's", "you're", 'same', 'himself', 'what', 'few', 'wasn', 'after', 'had', 'should', 'yours', 'some', 'itself'}
# the database is a simple dictionnary
database = {}

# regular expression for: extract words, extract ID from path, check for hexa value
chars = re.compile(r'\W+')
atLeast3Chars = re.compile(r'\w{3,}')
notDigit = re.compile(r'\D*')
pattid = re.compile(r'(\d{3})/(\d{3})/(\d{3})')

# the higher ID
tokens = 0
documents = 0
terms = 0
stopWordsFound = 0
stemmer = PorterStemmer()

#
# We will create a term object for each unique instance of a term
#
class Term():
    termid = 0
    termfreq = 0
    docs = 0
    docids = {}

    # The code added:
    # ===================================================================
    # Calculate the inverse document frequency
    # ===================================================================
    def idf(self, N):
        # dft - the number of documents that contain t
        return math.log10(N / self.docs)

# split on any chars
def splitchars(line):
    return chars.split(line)

# process the tokens of the source code
def parsetoken(line):
    global documents
    global tokens
    global terms
    global stopWordsFound
    global stemmer

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
        

        isAlphaNumeric = chars.match(lowerElmt) is None
        isAtLeast3chars = atLeast3Chars.match(lowerElmt) is not None
        isAstopWord = elmt in enStopwords
        if isAstopWord:
            stopWordsFound += 1

        # The code added:
        # ===================================================================
        # The following condition implements requirement from the assignment:
        # Ignore any term that begins with a punctionary character
        # Ignore any term that is a number
        # Ignore any term that is 2 characters or shorter in length
        # Ignore stopword
        # ===================================================================
        if isAtLeast3chars and isAlphaNumeric and not lowerElmt.isdigit() and not isAstopWord:
            # The code added:
            # ===================================================================
            # Implement a porter stemmer to stem the tokens processed by your indexer routine. 
            # ===================================================================
            lowerElmt = stemmer.stem(lowerElmt, 0, len(lowerElmt) - 1)

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
        with io.open(file.name, 'r', encoding='windows-1252') as unicode_file:
            for l in unicode_file.readlines():
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
    cursor = con.cursor()

    #
    # In the following section three tables and their associated indexes will be created.
    # Before we create the table or index we will attempt to drop any existing tables in
    # case they exist
    #

    # Document Dictionary Table
    cursor.execute("drop table if exists DocumentDictionary")
    cursor.execute("drop index if exists idxDocumentDictionary")
    cursor.execute("create table if not exists DocumentDictionary (DocumentName text, DocId int)")
    cursor.execute("create index if not exists idxDocumentDictionary on DocumentDictionary (DocId)")

    # Term Dictionary Table
    cursor.execute("drop table if exists TermDictionary")
    cursor.execute("drop index if exists idxTermDictionary")
    cursor.execute("create table if not exists TermDictionary (Term text, TermId int)")
    cursor.execute("create index if not exists idxTermDictionary on TermDictionary (TermId)")

    # Postings Table
    cursor.execute("drop table if exists Posting")
    cursor.execute("drop index if exists idxPosting1")
    cursor.execute("drop index if exists idxPosting2")
    cursor.execute("create table if not exists Posting (TermId int, DocId int, tfidf real, docfreq int, termfreq int)")
    cursor.execute("create index if not exists idxPosting1 on Posting (TermId)")
    cursor.execute("create index if not exists idxPosting2 on Posting (Docid)")

    #
    # The walkdir method essentially executes the indexer. The walkdir method will
    # read the corpus directory, Scan all files, parse tokens, and create the inverted index.
    #
    walkdir(cursor, folder)

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
    for token, term in database.items():
        termid = term.termid
        cursor.execute("insert into TermDictionary values (?, ?)", (token, termid))
        for documentId, docFrequency in term.docids.items():
            cursor.execute("insert into Posting values (?, ?, ?, ?, ?)", (termid, documentId, term.idf(documents), docFrequency, term.termfreq))

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
    print("Stop Words Found %i" % stopWordsFound)
    t2 = time.localtime()
    print('End Time: %.2d:%.2d' % (t2.tm_hour, t2.tm_min))
