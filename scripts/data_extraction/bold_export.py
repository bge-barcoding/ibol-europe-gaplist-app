#!usr/bin/python

# Title:         bold_export.py
# Description:   Retrieve, filter and combine (public) sequence data

# Custom Databases
#   The Custom Databases script uses a scientific notation of
#   species, including synonyms and expected species, stored
#   in the Dutch Species Register (NSR). Its export is used to
#   harvest matching species with sequence data from the
#   Barcode of Life Data (BOLD) database.
#
#   Software prerequisites: (see readme for versions and details).
# ====================================================================

# Import packages
import ast

from taxon_parser import TaxonParser, UnparsableNameException
from zipfile import ZipFile
from os.path import basename
import urllib3
import csv
import os
import argparse
import pandas as pd
import re
import io

# Set global variables
http = urllib3.PoolManager()
csv.field_size_limit(100000000)
par_path = os.path.abspath(os.path.join(os.pardir))
pd.options.mode.chained_assignment = None
output_header = False

# User arguments
parser = argparse.ArgumentParser()
parser.add_argument('-indir', default=par_path+"../../data/input_files/",
                    help="Input folder: NSR export directory")
parser.add_argument('-infile1', default="NSR_taxonomy.csv",
                    help="Input file 1: NSR taxonomy export")
parser.add_argument('-infile2', default="NSR_synonyms.csv",
                    help="Input file 2: NSR synonyms export")
parser.add_argument('-outdir1', default=par_path+"../../data/exports/",
                    help="Output folder 1: BOLD export directory")
parser.add_argument('-outdir2', default=par_path+"../../data/exports/",
                    help="Output folder 2: Result data directory")
parser.add_argument('-outfile1', default="bold_match.tsv",
                    help="Output file 1: Matching records")
parser.add_argument('-outfile2', default="bold_mismatch.tsv",
                    help="Output file 2: Non-matching records")
args = parser.parse_args()


def nsrTaxonomy():
    """ Captures and writes all binomial taxonomy to a CSV file.

    Loads a NSR Taxonomy export CSV file into a Pandas Dataframe, utf-8
    encoded. All taxonomic names for species-level records are sent to
    taxonParser() to parse them into their elementary components.
    Indexes and writes returned species to a CSV file.

    Arguments:
        taxonomyFile: NSR Taxonomy export read as Pandas Dataframe
        taxonomy: Dataframe with capture group on species-level records
        taxonList: List item containing parsed taxonomic components
        parser: Object containing the name parts and original taxon
        index: Numerical counter
        binomial: Binomial nomenclature of taxon
        authorship: Authorship of taxon
    Return:
        taxonList: List of all extracted taxonomy (Species + Authority)
    """
    # Input file
    taxonomyFile = pd.read_csv(args.indir+"/"+args.infile1, header=2,
                               sep="\t", encoding="utf8")

    # Parse taxonomic names into their elementary components
    taxonomy = taxonomyFile.loc[taxonomyFile['rank'] == 'soort']
    taxonList = []
    for taxon in taxonomy['scientific_name']:
        parser = taxonParser(taxon)
        if not parser or parser is False:
            pass
        else:
            taxonList.append(parser)
    # Write taxonomy to file
    index = 0

    return taxonList
# nsr_species.csv met kopjes species_id (index beginnend bij 0),
# species_name binominal, identifaction_reference auteur en jaar.
# Gehaald uit NSR_taxonomy.csv (handmatige download?)
# Geparsed met TaxonParser()
# Eerst "Ablaxia megachlora (Walker, 1835)"
# nsr id wordt niet gebruikt?
# naar inlezen file!!

def nsrSynonyms():
    """ Captures and writes all binomial synonyms to a CSV file.

    Loads a NSR Synonyms export CSV file into a Pandas Dataframe, utf-8
    encoded. All synonyms with respective taxons of scientific notation
    are sent to taxonParser() to parse them into their elementary
    components. Returned synonyms and taxons are paired in a dictionary,
    and written to a CSV file.

    Arguments:
        synonymsFile: NSR Synonyms export read as Pandas Dataframe
        synonyms: Dataframe with capture group on scientific notations
        synonymDict: Dictionary containing parsed taxonomic components
        synonym: Object containing the name parts and original synonym
        taxon: Object containing the name parts and original taxon
    Return:
        synonymList: List of all synonyms (binomial name + authority)
        synonymDict: Dictionary pairing synonym with respective taxon
    """
    # Input file
    synonymsFile = pd.read_csv(args.indir+"/"+args.infile2, header=2,
                               sep="\t", encoding="utf8")

    # Parse taxonomic names into their elementary components
    synonyms = synonymsFile.loc[synonymsFile['language'] == 'Scientific']
    synonymDict = {}
    for synonym, taxon in zip(synonyms['synonym'], synonyms['taxon']):
        synonym = taxonParser(synonym)
        taxon = taxonParser(taxon)
        if not taxon or synonym is False or taxon is False:
            pass
        else:
            synonymDict[synonym] = taxon
    # Write dictionary to file
    with io.open("../../data/exports/nsr_export_synoynyms.csv", "w+",
                 encoding="utf-8") as outfile:
        outfile.write("synonym_name,identification_reference,"
                      "taxon_name,taxon_author\n")
        for key, value in synonymDict.items():
            outfile.write('"%s","%s","%s","%s"'
                          % (' '.join(str(key).split()[:2]),
                             ' '.join(str(key).split()[2:]),
                             ' '.join(str(value).split()[:2]),
                             ' '.join(str(value).split()[2:])))
            outfile.write("\n")
    return [*synonymDict], synonymDict
#NSR_synonyms.csv (handmatige download?)
#Alleen scientific naam, niet de Nederlandse
# Checkt niet tweede kolom (of het synonym is of invalid name van taxon of bv misspelling)
# Kan juist handig zijn dat ie alles meepakt
# Kopjes: synonym name, synonym auteur, taxon naam, taxon auteur
# Veranderd in .rmd script  (?) naar synonym_id,"species_id,synonym_name,identification_reference
#return list van synonym (is het eigelijk de taxon die wordt gereturned met [*dict]?

def nsrGenera(synonymList):
    """ Extracts the unique genera from both taxonomy and synonym lists.

    Combines the scientific names of obtained taxonomy and synonyms to
    one list, filtering out empty lines. Selects all unique genera.

    Arguments:
        species: Scientific notation of all species and known synonyms
    Return:
        generaList: List of all unique genera
    """
    taxon = pd.read_csv("../../data/insert_files/nsr_species.csv")
    taxonList = list(taxon["species_name"] + " " +
                     taxon["identification_reference"])
    species = list(filter(None, sorted(taxonList + synonymList)))
    generaList = [i.split()[0] for i in species]
    generaList = list(dict.fromkeys(generaList))

    return taxonList, generaList


def taxonParser(taxon):
    """ Parse any taxonomic name into its elementary components.

    Used library is a pure Python adaptation of the GBIF Java
    name-parser library. Taxonomic names are parsed into their
    elementary components. Genus, specific epithet, and authors are
    concatenated for all binomial names and returned. For any name
    that can not be parsed, an UnparsableNameException is thrown.

    Arguments:
        parser: Object to parse
        parsed_name: Object containing the name parts and original taxon
    Return:
        scientific_name: Concatenation of binomial name and authorship
    """
    parser = TaxonParser(taxon)
    scientific_name = ""

    try:
        parsed_name = parser.parse()
        if parsed_name.isBinomial() is True:
            scientific_name = str(parsed_name.genus) + " " +\
                              str(parsed_name.specificEpithet)
            if str(parsed_name.combinationAuthorship) != "<unknown>":
                scientific_name += " " + str(parsed_name.combinationAuthorship)
            elif str(parsed_name.basionymAuthorship) != "<unknown>":
                scientific_name += " " + str(parsed_name.basionymAuthorship)
            else:
                scientific_name = False
        else:
            pass
    except UnparsableNameException:
        pass

    return scientific_name

def boldExtract(genera):
    """ Obtains public sequence data for a list of genera.

    Downloads records using BOLD's Public Data Portal API. Base URL for
    data retrieval is appended to each genus from the NSR genera list.
    Genera are retrieved one genus at a time and saved as TSV file.

    Arguments:
        base_url: String, default URL for data retrieval using BOLD's API
        source_urls: List of all URL's to be retrieved
        counter: Integer to keep track of genus in source list
        url: String, url correlating to the selected genus
        r: HTTPResponse, variable for retrieving web-url's
        name: String containing name of current genus
    """
    # Prepare Web Service Endpoint for BOLD's Public Data Portal API
    # Appending BOLD's base URL to each genera from the NSR list
    base_url = 'http://v4.boldsystems.org/index.php/API_Public/combined?taxon='
    subset_size = 500
    final = [genera[i * subset_size:(i + 1) * subset_size] for i in
             range((len(genera) + subset_size - 1) // subset_size)]
    for subset_genera in final:
        source_urls = base_url + "|".join(subset_genera)[:-1:]\
                      +'&geo=Netherlands&format=tsv'
        # Download sequence data from BOLD using list of url's
        print('Sequence data retrieval...')
        r = http.request('GET', source_urls)
        data = io.BytesIO(r.data)
        pd.read_csv(data, sep="\t", error_bad_lines=False,
                    encoding='iso-8859-1').to_csv(args.outdir1+
                                                  "/"+"bold_all.tsv",
                                                  sep="\t", mode='a',
                                                  index=False)


def boldNSR(species, synonyms, syn_dict):
    """ Match obtained sequence records to the reference species names.

    Iterates over every output file from BOLD and compares sequence data
    to the list of species from the NSR. Subgenera will be filtered out
    creating a file with as many accepeted names as possible. Mismatches
    against the NSR are copied to a seperate list.

    Arguments:
        file: String, current filename (genus) from the BOLD downloads
        filename: String, decoding the filename from the filesystem encoding
        tsvreader: File (genus) read in a tab delimited matter
        line: Rows of the current file (genus)
    """
    # Loop over each genus(file) downloaded from BOLD
    print('Comparing sequence data to list of species...')
    # Open genera file from zip
    tsvreader = csv.DictReader(open(args.outdir1+"/"+"bold_all.tsv"),
                               dialect='excel-tab')
    # Read each record
    for line in tsvreader:
       # Filter on Geographical site
        bold_identification = re.sub('[()]', '',
                                     str(line['identification_reference']))
        bold_name = line['species_name'] + " " + bold_identification
        # Compare BOLD with NSR species names
        if (bold_name in species):
            boldOutput(args.outfile1, line)
             # Check for synonyms, apply accepted name
        elif (bold_name in synonyms):
            for synonym, taxon in syn_dict.items():
                synonym = ' '.join(synonym.split()[:2])
                if synonym == line['species_name']:
                    taxon = ' '.join(taxon.split()[:2])
                    line['species_name'] = taxon
                    boldOutput(args.outfile1, line)
                    # Write missmatches to seperate file
        else:
            boldOutput(args.outfile2, line)


def boldOutput(file, line):
    """ Writes matching/non-matching records to respesctive file.

    Opens respective output file and appends the record. Ensures each
    file contains a header row.

    Arguments:
        output_header: List of record fields to be emitted
        f: Outputfile, either match or mismatch depending on parameter
    """
    # Write header to output files (executes only one time per run)
    global output_header
    if output_header is False:
        for temp in (args.outfile1, args.outfile2):
            with io.open(args.outdir2+"/"+temp, mode="a", encoding="utf-8") as f:
                for key, value in line.items():
                    f.write('%s\t' % (key))
                f.write("\n")
        output_header = True

    # Write sequence data, for each record
    with io.open(args.outdir2+"/"+file, mode="a", encoding="utf-8") as f:
        for key, value in line.items():
            f.write('%s\t' % (value))
        f.write("\n")



def main():
    """ Main logic. Powers each function with their respective input. """
    # Create clean output files
    open(args.outdir2+"/"+args.outfile1, 'w+').close()
    open(args.outdir2+"/"+args.outfile2, 'w+').close()
    open(args.outdir1 + "/" + "bold_all.tsv", 'w+').close()

    # Run functions

    synonymList, synonymDict = nsrSynonyms()
    taxonList, generaList = nsrGenera(synonymList)
    boldExtract(generaList)
    boldNSR(taxonList, synonymList, synonymDict)
    print("Done")


main()

#rmd naar python:
## NSR synonyms?

#marker en database csv handmatig gemaakt.