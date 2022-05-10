import http
import io
import json
import os
import glob
import re

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
    species_marker = pd.read_csv(PATH + 'insert_files/species_marker.csv',
                                 sep=",")
    duplicates = species_marker[
        species_marker.duplicated(['sequence_id'], keep=False)]
    # for i in duplicates['sequence_id']:
    #     print(duplicates.loc[duplicates['sequence_id'] == i])
    bold = species_marker.loc[species_marker['database_id'] == 1]
    # for i in bold['sequence_id']:
    #     print(bold.loc[bold['sequence_id'] == i])
    bold_duplicates = bold[bold.duplicated(['sequence_id'], keep=False)]
    print(bold_duplicates)
    # print(species_marker.loc[species_marker['database_id'] == 0])
    # print(species_marker[species_marker.duplicated(['sequence_id', 'marker_id'], keep=False)])
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
    # record = SeqIO.read(files[0], format="fasta")
    for seq in fasta_sequences:
        result_handles = NCBIWWW.qblast("blastn", "nt", seq.format("fasta"),
                                        hitlist_size=2)
        # print(result_handles)
        # print(result_handles.read())
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
    df = df_bold[
        df_bold.duplicated(subset=['processid', 'sequence_id'], keep=False)]
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


# report()
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
        'nucleotides', 'bin_uri', 'genbank_accession']).dropna(
        subset=['nucleotides'])

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
         'nucleotides', 'fasta_header'], axis=1) \
        .dropna()

    # Create new column database_id and give all bold data id 1 (corresponds to
    # database_name BOLD in database.csv)
    df_bold_species.insert(0, "database_id", 1)

    # Change species_id from float to string
    df_bold_species['species_id'] = df_bold_species['species_id'] \
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
    # species = "Erodium carvifolium"
    # species = species.replace(" ", "+").strip()
    for i in species_list:
        search = Entrez.esearch(term=i, db="taxonomy", retmode="xml")
        record = Entrez.read(search)
        print("...")
        if record["Count"] != "0":
            ids.append(record["IdList"][0])
    print(ids)


def ncbi_tree():
    ncbi = NCBITaxa()
    species_df = pd.read_csv(PATH + "/insert_files/nsr_species.csv")
    species_list = species_df['species_name'].tolist()

    # Get tax id for every species name
    name2taxid = ncbi.get_name_translator(species_list)

    # Rermove [] from tax id string
    clean_dict = {k: str(v).strip("[]") for k, v in name2taxid.items()}

    # Make dataframe from species_name with corresponding tax_id
    df_tax = pd.DataFrame(clean_dict.items(), columns=["species_name",
                                                       "tax_id"]).head(
        20)
    df_tree_ncbi = pd.DataFrame()

    # Get lineage from every species and put into dataframe
    for id in df_tax["tax_id"]:
        lineage = ncbi.get_lineage(id)[1::]  # omit root taxid
        rank = list(ncbi.get_taxid_translator(lineage).values())
        name = list(ncbi.get_rank(lineage).values())
        ordered_dict = {k: v for k, v in zip(name, rank)}
        ordered_dict["tax_id"] = id
        df_tree_ncbi = df_tree_ncbi.append(ordered_dict, ignore_index=True)

    # Join tree dataframe with nsr_species on species names
    df_joined_rank = pd.merge(df_tree_ncbi, species_df, how="inner",
                         left_on=["species"],
                         right_on=["species_name"])

    # Drop columns
    df_joined_rank = df_joined_rank.drop(columns=['species_name',
                                        'identification_reference',
                                        'species_id',
                                        'tax_id'])  # rearrange columns (need to know the order)
    # Get ranks
    columns = df_joined_rank.columns

    # Put ranks in dataframe with an id
    df_rank = pd.DataFrame(columns=["rank", "r_id"])
    df_rank['rank'] = columns
    df_rank["r_id"] = df_rank.index

    # Make dataframe per taxname with the rank id
    df_name = pd.DataFrame(columns=['name_id', 'r_id', 'name'])
    for rank in df_joined_rank:
        for tax_name in list(set(df_joined_rank[rank].dropna())):
            df_name = df_name.append({'r_id':
                                          int(df_rank.loc[df_rank['rank']
                                                          == rank][
                                                  'r_id'].values),
                                      'name': tax_name}, ignore_index=True)

    # Give dataframe a name_id
    df_name['name_id'] = df_name.index

    # Join tree_ncbi dataframe with nsr_species dataframe on species names
    df_joined_tree = pd.merge(df_tree_ncbi, species_df, how="inner",
                           left_on=["species"],
                           right_on=["species_name"])

    # Make dataframe with name_id and species_id
    # For all taxonomic levels there is a species_id and name_id combination
    df_species_names = pd.DataFrame(columns=['name_id', 'species_id'])
    for rank in df_joined_rank.columns:
        joined = pd.merge(df_name, df_joined_tree[[rank, 'species_id']],
                          left_on='name', right_on=rank)
        df_species_names = pd.concat([df_species_names,
                                     joined[['species_id', 'name_id']]])
    df_species_names = df_species_names.drop_duplicates()

    # df_species_names.to_csv(PATH + "exports/ncbi_species_name.csv", sep=',',
    #                        index=False)
    # df_rank.to_csv(PATH + "exports/ncbi_rank.csv", sep=',', index=False)
    # df_name.to_csv(PATH + "exports/ncbi_name.csv", sep=',', index=False)

    # tree = ncbi.get_topology(lineage, intermediate_nodes=True)
    # lineage = re.split('(?=[A-Z][a-z]+,)', tree.get_ascii(attributes=["sci_name", 'rank']).strip().replace("-", ""))
    # print(lineage)
    # df = pd.DataFrame(columns=["rank", 'name'])
    # for i in lineage:
    #      df = df.append({'rank': i.split(", ")[1].strip('-'),
    #             'name': i.split(", ")[0].strip('-')}, ignore_index=True)
    # df = df.drop_duplicates(['rank', 'name'])
    # print(df)



# from treemaker import TreeMaker
#
# # zelfde proberen maar dan niet met tree maar met wat ik eerst deed
# # species en genus er niet goed in + append werkt niet
# start = time.time()
# tree_nsr = pd.read_csv(PATH + "/insert_files/tree_nsr.csv")
# tree_nsr = tree_nsr.drop(columns=['species_id', 'tax_id', 'identification_reference'])
# tree_nsr = tree_nsr.transpose().values.tolist()[0:-1]
# lijst = []
# index = 0
#
# for i in tree_nsr[0:3]:
#     index += 1
#     titel = "n" + str(index)
#     i = list(set(i))
#     newlist = [x for x in i if pd.isnull(x) == False]
#     print(newlist)
#     string = ', '.join(newlist)
#     lijst.append(list([titel, string]))
# t = TreeMaker()
#
# taxa = [
#     ('A1', 'family a, subgroup 1'),
#     ('A2', 'family a, subgroup 2'),
#     ('B1a', 'family b, subgroup 1'),
#     ('B1b', 'family b, subgroup 1'),
#     ('B2', 'family b, subgroup 2'),
# ]
#
# t = TreeMaker()
# t.add_from(taxa)
#
#
# print(t.write())
node_to_children = {}
tree_nsr = pd.read_csv(PATH + "/insert_files/tree_nsr.csv")
df = tree_nsr.drop(columns=['species_id', 'tax_id', 'identification_reference'])

#iterate over dataframe row-wise. Assuming that every row stands for one complete branch of the tree
for row in df.itertuples():
    #remove index at position 0 and elements that contain no child ("")
    row_list = [element for element in row[1:] if element != ""]
    for i in range(len(row_list)-1):
        if row_list[i] in node_to_children.keys():
            #parent entry already existing
            if row_list[i+1] in node_to_children[row_list[i]].keys():
                #entry itself already existing --> next
                continue
            else:
                #entry not existing --> update dict and add the connection
                node_to_children[row_list[i]].update({row_list[i+1]:0})
        else:
            #add the branching point
            node_to_children[row_list[i]] = {row_list[i+1]:0}

from ete3 import Tree
# op = str(node_to_children).replace("{", "(").replace('}', ")").replace(":", "").replace("0", "").replace(' ', "")
# print(op)
# tree = Tree(op)
# print(tree)
# end = time.time()
# print(end - start)
tree_nsr = pd.read_csv(PATH + "/insert_files/tree_nsr.csv")
df = tree_nsr.drop(columns=['species_id', 'tax_id', 'identification_reference'])