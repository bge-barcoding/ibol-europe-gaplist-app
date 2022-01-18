import http
import io
import json
import os
import glob
import urllib3
from Bio import Entrez, SeqIO, SearchIO
from Bio.Blast import NCBIWWW
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from scripts.classes.nsr_species import NsrSpecies
from scripts.classes.species_marker import SpeciesMarker
from bs4 import BeautifulSoup
PATH = fileDir = os.path.join(os.path.dirname(
    os.path.realpath('__file__')), '../data/')
import os
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import sessionmaker
from scripts.classes.database import Database
from scripts.classes.marker import Marker
from scripts.classes.imports import *
from scripts.classes.species_marker import SpeciesMarker
from scripts.classes.nsr_species import NsrSpecies
from scripts.classes.tree_ncbi import TreeNcbi
from scripts.classes.nsr_synonym import NsrSynonym

from Bio.Blast import NCBIXML
import time
from ete3 import NCBITaxa
def dump():
    # bold = pd.read_csv(PATH + 'exports/bold_match.tsv', sep="\t")
    # text_file = open("metadata.txt", "a+")
    #
    # #print(bold['genbank_accession'])
    # #print(bold.loc[bold['processid']=='NLLEA826-12']['genbank_accession'])
    # for col in bold:
    #   #print(len(bold[col].unique()) , col)
    #   print(bold[bold.duplicated([col], keep=False)].shape, col)
    # #print(bold.columns)
    # pd.set_option('display.max_columns', None)
    # print(bold[bold.duplicated(['processid', 'nucleotides'], keep=False)].shape)
    # print(bold[bold.duplicated(['processid'], keep=False)])
    # Entrez.email = "A.N.Other@example.com"  # Always tell NCBI who you are
    # handle = Entrez.efetch(db="nucleotide", id="KX048728", rettype="gb", retmode="text")
    # #print(handle.read())
    # http = urllib3.PoolManager()
    # # Get sequences in fasta format (NEEDS processid NOT sequenceID)
    # # for i in bold["processid"]:
    # #     r = http.request('GET', 'http://v3.boldsystems.org/index.php/API_Public/sequence?ids=' + str(i), preload_content=False)
    # #     r.auto_close = False
    # #     for line in io.TextIOWrapper(r):
    # #         print(line)
    # r = http.request('GET', 'http://v3.boldsystems.org/index.php/API_Public/sequence?ids=RMNH.INS.228912', preload_content=False)
    # r.auto_close = False
    # for line in io.TextIOWrapper(r):
    #     print(line)
    species_marker = pd.read_csv(PATH + 'insert_files/species_marker.csv', sep=",")
    duplicates = species_marker[species_marker.duplicated(['sequence_id'], keep=False)]
    # for i in duplicates['sequence_id']:
    #     print(duplicates.loc[duplicates['sequence_id'] == i])
    bold = species_marker.loc[species_marker['database_id'] == 1]
    # for i in bold['sequence_id']:
    #     print(bold.loc[bold['sequence_id'] == i])
    bold_duplicates = bold[bold.duplicated(['sequence_id'], keep=False)]
    print(bold_duplicates)
    # print(species_marker.loc[species_marker['database_id'] == 0])
    #print(species_marker[species_marker.duplicated(['sequence_id', 'marker_id'], keep=False)])
    # for i in bold_duplicates['sequence_id']:
    #     print(bold_duplicates.loc[bold_duplicates['sequence_id'] == i])



def internal():
    files = glob.glob(PATH + "/exports/*.fasta")
    species_pd = pd.read_csv(PATH + "/insert_files/nsr_species.csv")
    synonym_pd = pd.read_csv(PATH + "/insert_files/nsr_synonym.csv")
    unique = []
    for file in files:
        print(file.split("\\")[-1])
        fasta_sequences = SeqIO.parse(
            open(PATH + "exports/" + file.split("\\")[-1]), 'fasta')
        for sequence in fasta_sequences:
            unique.append(sequence.id.split("_")[0])
            genus = sequence.id.split("_")[1]
            species = sequence.id.split("_")[2]
            genus_species = species_pd['species_name'].str.split(r"\s", n=1,
                                                              expand=True)
            genus_match = genus_species[
                genus_species[0].str.contains('^%s.*' % genus) == True]
            species_match = genus_match[
                genus_match[1].str.contains('^%s.*' % species) == True]
            if len(species_match) != 0:
                print(genus + " " + species)
                print(species_match)
            if len(species_match) == 0:
                genus_species = synonym_pd['synonym_name'].str.split(r"\s", n=1,
                                                                     expand=True)
                genus_match = genus_species[
                    genus_species[0].str.contains('^%s.*' % genus) == True]
                species_match = genus_match[
                    genus_match[1].str.contains('^%s.*' % species) == True]
                if len(species_match) != 0:
                    print(genus + " " + species)
                    print(species_match)



def internal_blast():
    files = glob.glob(PATH + "/exports/*.fasta")
    species_pd = pd.read_csv(PATH + "/insert_files/nsr_species.csv")
    synonym_pd = pd.read_csv(PATH + "/insert_files/nsr_synonym.csv")
    fasta_sequences = SeqIO.parse(
         open(PATH + "exports/test.fasta"), 'fasta')
    print(len(species_pd['species_name'].unique()))
    print(len(species_pd))
    #record = SeqIO.read(files[0], format="fasta")
    for seq in fasta_sequences:
        result_handles = NCBIWWW.qblast("blastn", "nt", seq.format("fasta"), hitlist_size=2)
        #print(result_handles)
        #print(result_handles.read())
        soup = BeautifulSoup(result_handles.read(), 'xml')

        print(seq.id, "\t", soup.find('Hit_accession').text)



def internal_bold():
    base_url = 'http://v3.boldsystems.org/index.php/Ids_xml?db=COX1_SPECIES_PUBLIC&sequence='
    http = urllib3.PoolManager()
    fasta_sequences = SeqIO.parse(
        open(PATH + "exports/test.fasta"), 'fasta')
    for record in fasta_sequences:
        response = http.request('GET', str(base_url + record.seq))
        data = str(io.BytesIO(response.data).read(), 'utf-8')
        soup = BeautifulSoup(data, 'xml')
        print(str(base_url + record.seq))
        print(record.id)
        print(record.id, "\t", soup.find('taxonomicidentification').text)


def species_marker():
    species_pd = pd.read_csv(PATH + "/insert_files_banker/tree_nsr.csv")
    print(pd.concat(
        g for _, g in species_pd.groupby("tax_id") if len(g) > 1))
    print(len(species_pd))
    print('\n')
    species_pd = pd.read_csv(PATH + "/insert_files/tree_nsr.csv")
    print(pd.concat(
        g for _, g in species_pd.groupby("tax_id") if len(g) > 1))
    print(species_pd)
    pd.set_option('display.max_columns', None)
    print(species_pd[species_pd.duplicated(['tax_id'], keep=False)])


def report():
    df_bold = pd.read_csv(PATH + "exports/bold_match.tsv", sep="\t", usecols=[
        "species_name", "markercode", "sequenceID", "identification_reference",
        'bin_uri', 'recordID', 'sampleid', 'processid', "nucleotides"])

    # Rename columns to correspond to database
    df_bold = df_bold.rename(columns={"markercode": "marker_name",
                                      "sequenceID": "sequence_id"})

    # Change sequence_id from float to string
    df_bold["sequence_id"] = df_bold['sequence_id'].astype('Int64').astype(
        'str')
    pd.set_option('display.max_columns', None)
    print('Aantal entries in df', len(df_bold))
    print('Unieke nucleotides', len(df_bold['nucleotides'].unique()),
          "NaN nucleotides", df_bold['nucleotides'].isna().sum())
    print('Unieke recordIDs', len(df_bold['recordID'].unique()),
          'Geen NaN values')
    print('Unieke sampleids', len(df_bold['sampleid'].unique()),
          'Geen NaN values')
    print('Unieke sequence_ids', len(df_bold['sequence_id'].unique()),
          "NaN sequence_ids", df_bold['sequence_id'].isna().sum())
    print(df_bold['sampleid'].value_counts())
    df = df_bold[df_bold.duplicated(subset=['processid', 'sequence_id'], keep=False)]
    print(df)

def wfbi_backbone():
    df = pd.read_excel(PATH + "input_files/WFBI_taxonomy.xlsx",
                       usecols=['ID', "Taxon_name", "Rank", "Name_status",
                                "Classification"], nrows=500)
    pd.set_option('display.max_columns', None)
    df = df[df['Rank'].isin(['sp.'])]
    df = df[df['Name_status'].isin(['Legitimate'])]
    lijst = df["Classification"].tolist()
    for i in lijst:
        if len(i.split(",")) <= 5:
            print(i)


    # niet consistent! Niet ncbi. of het nsr backbone is kan pas checken na export
    # class of clade? regn = kingdom?
    # use case 10 = [species, subfamily, species?, ordo, subclass, class, subdivision, division, sub kingdom, kingdom] etc
    #
    # 10(opnieuw zonder species)
    # subfamily, species?, ordo, subclass, class, subdivision, division, sub kingdom, kingdom
    #
    # 9 (no subfamily)
    # genus, family, order, subclass, class, subdivision, division, sub kingdom, kingdom
    #
    # 8(no subclass)
    # genus, family, ordo, class, subdivision, division, sub kingdom, kingdom
    #
    # 7
    # genus, subclass, class, subdivision, division, sub kingdom, kingdom
    #
    # 5
    # genus, class, division, sub kingdom, kingdom
    #
    # 4
    # genus, division, sub kingdom, kingdom
    # Fungi, Ascomycota, Ascomycetes, Abrothallus = genus, class, division, kingdom
    #
    # 3
    # genus, division, kingdom
    # verder checken, van links rechts vullen tot waar het hetzelfde is? als ze apart gaan zoals ncbi, anders zoals tree_nsr
    #
    # 2
    # genus, kingdom


#report()
# engine = create_engine(
#         'postgresql://postgres:password@localhost:5432/barcodes', echo=False)
#
# # Create session
# Session = sessionmaker(engine)
# session = Session()
# species_markers = session.query(SpeciesMarker).all()
#
# for class_instance in session.query(SpeciesMarker).all():
#     print(class_instance.sequence_id)
# df = pd.read_csv(PATH + "exports/bold_match.tsv", sep="\t", usecols=[
#         "species_name", "markercode", "sequenceID", "identification_reference",
#         'bin_uri', 'recordID', 'sampleid', 'processid', "nucleotides"])
# pd.set_option('display.max_columns', None)
#
# http = urllib3.PoolManager()
#
# base_url = 'http://v4.boldsystems.org/index.php/API_Public/combined?ids=RMNH.INS.228912&format=tsv'
# r = http.request('GET', base_url)
# data = io.BytesIO(r.data)
# print(pd.read_csv(data, sep="\t", error_bad_lines=False,
#                     encoding='iso-8859-1'))
#
#
# session.commit()
# Load data/exports/bold_match.csv (only needed columns are selected)
    # PATH is ../data/
def bold_metadata():
    bfd_species = pd.read_csv(PATH + "insert_files/nsr_species.csv")
    bfd_marker = pd.read_csv(PATH + "insert_files/marker.csv")
    df_bold = pd.read_csv(PATH + "exports/bold_match.tsv", sep="\t", usecols=[
        "species_name", "markercode", "sequenceID", "identification_reference",
    'nucleotides', 'bin_uri', 'genbank_accession']).dropna(subset=['nucleotides'])

    # Rename columns to correspond to database
    df_bold = df_bold.rename(columns={"markercode": "marker_name",
                                      "sequenceID": "sequence_id"})

    # Change sequence_id from float to string
    df_bold["sequence_id"] = df_bold['sequence_id'].astype('Int64').astype(
        'str')

    # Make a header for fasta file
    df_bold["fasta_header"] = ">" + df_bold["sequence_id"].astype(str) + "_" + \
                              df_bold["marker_name"]

    # Put header and sequences in fasta format
    fasta_out = df_bold['fasta_header'] + "\n" + df_bold["nucleotides"]

    # Save BOLD sequences and their respective header in a fastafile
    np.savetxt(PATH + '/reference_db/reference_db.fasta', fasta_out.values,
               fmt="%s")

    # Merge bold df with nsr_species df on species_name and
    # identification_reference
    df_joined = pd.merge(bfd_species, df_bold, how='right',
                         on=["species_name", "identification_reference"])

    # Merge dataframe with marker on marker_name
    df_joined = pd.merge(df_joined, bfd_marker, on="marker_name")

    # Drop unessecary columns and NA rows
    df_bold_species = df_joined.drop(
        ['identification_reference', 'species_name', "marker_name",
         'nucleotides', 'fasta_header'], axis=1)\
        .dropna()

    # Create new column database_id and give all bold data id 1 (corresponds to
    # database_name BOLD in database.csv)
    df_bold_species.insert(0, "database_id", 1)

    # Change species_id from float to string
    df_bold_species['species_id'] = df_bold_species['species_id']\
        .astype('float').astype("Int64")
    df_bold_species = df_bold_species.sort_values(["species_id"],
                                   ignore_index=True)
    # Return merged and formatted dataframe
    print(df_bold_species['genbank_accession'])




from Bio import Entrez

def entrez():
    species_pd = pd.read_csv(PATH + "/insert_files_banker/nsr_species.csv")
    species_list = species_pd['species_name'].tolist()
    Entrez.email = ""
    ids = []
    #species = "Erodium carvifolium"
    #species = species.replace(" ", "+").strip()
    for i in species_list:
        search = Entrez.esearch(term=i, db="taxonomy", retmode="xml")
        record = Entrez.read(search)
        print("...")
        if record["Count"] != "0":
            ids.append(record["IdList"][0])
    print(ids)


def ncbi_tree():
    ncbi = NCBITaxa()
    species_pd = pd.read_csv(PATH + "/insert_files_banker/nsr_species.csv")
    species_list = species_pd['species_name'].tolist()
    # {9443: u'Primates', 9606: u'Homo sapiens'}

    name2taxid = ncbi.get_name_translator(species_list)
    clean_d = {k: str(v).strip("[]") for k, v in name2taxid.items()}
    df = pd.DataFrame(clean_d.items(), columns=["name", "tax_id"])
    output = pd.DataFrame()
    pd.set_option('display.max_columns', None)
    for i in df["tax_id"]:
        lineage = ncbi.get_lineage(i)[1::]
        values = list(ncbi.get_taxid_translator(lineage).values())
        keys = list(ncbi.get_rank(lineage).values())
        placeholder = {k: v for k, v in zip(keys, values)}
        placeholder["tax_id"] = i
        output = output.append(placeholder, ignore_index=True)
    df_joined = pd.merge(output, species_pd, how="inner",
                         left_on=["species"],
                         right_on=["species_name"])
    print(df_joined.drop(columns=['species_name',
                                  'identification_reference']))  # rearrange columns (neet to know the order) a
    # and drop ranks that are unrelevent


start = time.time()
ncbi_tree()
end = time.time()
print(end - start)
