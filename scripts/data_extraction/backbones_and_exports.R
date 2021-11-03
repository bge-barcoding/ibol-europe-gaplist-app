
library(rmarkdown)
library(data.table)
library(taxizedb)
library(myTAI)
library(tidyr)
library(shiny)
library(DT)
library(plyr)
library(dplyr)
library(stringr)
library(d3Tree)
library(billboarder)
library(nbaR)

REPO_HOME <- paste(dirname(getwd()), sep = "")
getwd()

## Load species and synonyms from the NSR files, respectively
nsrSpecies.file <- sprintf('%s/data/insert_files/nsr_species.csv', REPO_HOME)
nsrSpecies <- read.csv(nsrSpecies.file, encoding="UTF-8")


nsrSynonyms.file <- sprintf('%s/data/insert_files/nsr_synonyms.csv', REPO_HOME)
nsrSynonyms <- read.csv(nsrSynonyms.file, encoding="UTF-8")

## Select species for taxon data retrieval
nsrIn <- tolower(nsrSpecies[, "species_name"])
head(nsrIn, 3)
## nsrIn

## Query NSR taxon records (duration: ~40m)
## Method: nbaR 'taxon_query'
## Parameters: NSR source code and list of selected species
queryParams <- list("sourceSystem.code"="NSR")
nsrSpecimens <- data.frame()
for(x in 1:length(nsrIn)){
  queryParams[["acceptedName.scientificNameGroup"]] <- nsrIn[x]
  nsrSpecimens <- bind_rows(nsrSpecimens, taxon_query(queryParams))
}
head(nsrSpecimens,3)
## nsrSpecimen 13 columns 

## Access and extract required taxonomic fields from retrieved taxon records
nsrBackbone = data.frame()
for(x in 1:nrow(nsrSpecimens)){
  tryCatch({
    c.id=nsrSpecimens$sourceSystemId[x]
    c.kingdom=nsrSpecimens$defaultClassification$kingdom[x]
    c.phylum=nsrSpecimens$defaultClassification$phylum[x]
    c.class=nsrSpecimens$defaultClassification$className[x]
    c.order=nsrSpecimens$defaultClassification$order[x]
    c.family=nsrSpecimens$defaultClassification$family[x]
    c.genus=nsrSpecimens$defaultClassification$genus[x]
    c.species=nsrSpecimens$acceptedName$specificEpithet[x]
    c.authority=nsrSpecimens$acceptedName$authorshipVerbatim[x]
    
    row <- data.frame(cbind(tax_id=c.id,kingdom=c.kingdom,phylum=c.phylum,class=c.class,
                            order=c.order,family=c.family,genus=c.genus,
                            species=paste(c.genus,c.species),identification_reference=c.authority))
    nsrBackbone <- bind_rows(nsrBackbone, row)
  }, error=function(e){cat("ERROR :",conditionMessage(e), "\n")})
}
## Remove parenthesis around the authority for consistency across data sets
nsrBackbone$identification_reference = gsub("[()]", "", nsrBackbone$identification_reference)

head(nsrBackbone,3)
## nsrBackbone, !! remove @NSR here or in nsrSpecimens, 9 columns

## Match taxonomic data to respective species ID, by species name
nsrBackbone <- left_join(nsrBackbone, nsrSpecies[1:2], by=c("species"="species_name")) %>%
  .[,c(1,10,2,3,4,5,6,7,8,9)]
nsrBackbone <- unique(nsrBackbone)
head(nsrBackbone,3)
## nsrBackbone after binding them to their species id by name (nsrSpecies), 10 columns (tax_id got added)

## Write NSR data to file
write.csv(nsrBackbone, file=sprintf('%s/data/in_files/tree_nsr.csv', REPO_HOME),
          row.names=FALSE, fileEncoding="utf-8")

## Clean up query variables
rm(x, row, queryParams, list = ls()[grep("^c.", ls())])

## Select species for taxon data retrieval
## Ensuring to select only binomial names
natIn <- unique(gsub("([A-Za-z]+).*", "\\1", nsrSpecies$species_name))
## Query Naturalis specimen records (duration: ~20m)
## Method: nbaR 'specimen_query'
## Parameters: CRS source code and list of selected species

queryParams <- list("sourceSystem.code"="CRS") #removed queryParams <- list(queryparams) before <- list(sourceS.. etc)
natSpecimens <- data.frame()
for(x in 1:length(natIn)){
  queryParams[["identifications.defaultClassification.genus"]] <- natIn[x]
  specimen_query(queryParams)
  natSpecimens <- bind_rows(natSpecimens, specimen_query(queryParams))
}
head(natSpecimens,3)
## natSpecimens (13 rows, with @CRS!!)

## Access and extract required data fields from retrieved specimen records
natSpecies = data.frame()
for(x in 1:nrow(natSpecimens)){
  tryCatch({
    c.genus=natSpecimens$identifications[[x]]$defaultClassification$genus
    c.species=natSpecimens$identifications[[x]]$defaultClassification$specificEpithet
    c.count=natSpecimens$numberOfSpecimen[[x]]
    c.id=natSpecimens$sourceSystemId[[x]]
    row <- data.frame(cbind(species=paste(c.genus,c.species),counts=c.count,sequenceID=c.id))
    natSpecies <- bind_rows(natSpecies, row)
  }, error=function(e){cat("ERROR :",conditionMessage(e), "\n")})
}
## Remove NAs
natSpecies$counts[is.na(natSpecies$counts)] <- 0
## Update data type and sum unique species record counts
natSpecies$counts <- as.numeric(as.character(natSpecies$counts))
natSpecies <- aggregate(natSpecies$counts,
                        by=list(natSpecies$species,natSpecies$sequenceID),sum)
## Rename columns
colnames(natSpecies) <- c("species", "sequenceID", "counts")
## Adopt respective taxonomic unit for synonymous names
natSpecies <- left_join(natSpecies, nsrSynonyms[2:3], by=c("species"="synonym_name"))
natSpecies <- left_join(natSpecies, nsrSpecies[1:2], by=c("species_id"))
natSpecies$species_name <- coalesce(natSpecies$species_name, natSpecies$species)
natSpecies <- natSpecies[,c(5,2,3)]
## Match species against the NSR
natCoverage <- merge(natSpecies, nsrSpecies[c(2:3)], by="species_name")

head(natSpecies,3)
## natSpecies (withdata from natSpecimens) 3 columns (species_name<(@s in name?), sequence_id and counts(?))

head(natCoverage,3)
## same as natSpecies but with identification_Reference from nsrspecies?
write.csv(natCoverage, file=sprintf('%s/data/data/exports/naturlis.csv', REPO_HOME),
          row.names=FALSE, fileEncoding="utf-8", na="")
## Clean up query variables
rm(x, row, queryParams, list = ls()[grep("^c.", ls())])

## Download taxonomic database (options: ncbi, itis, gbif, col, wfo)
## Used data source: NCBI
db_download_ncbi()

## Load species names from the NSR data set
taxidIn <- nsrSpecies[, "species_name"]

## Match species names to NCBI taxon IDs
taxidOut <- taxizedb::name2taxid(taxidIn, db="ncbi", out_type="summary")
head(taxidOut,3)
## NCBI taxon ID's

## Isolate taxon IDs from output, save as vector
treeIn <- as.vector(taxidOut$id)
head(treeIn,3)
## Only id's

## Retrieve taxonomic hierarchy for each taxon ID
treeOut <- taxizedb::classification(treeIn, db="ncbi", row=1, verbose=FALSE)
head(treeOut,3)
## Parse out the taxonomy levels/factors that you require
taxdata = data.frame()
for(x in 1:length(treeOut)){
  tryCatch({
    for(i in 1:length(treeOut[[x]][[1]])) {
      c.tax_id=filter(treeOut[[x]])$id[[i]]
      c.parent_tax_id=NULL
      c.rank=filter(treeOut[[x]])$rank[[i]]
      c.name=filter(treeOut[[x]])$name[[i]]
      
      tryCatch({
        c.parent_tax_id=filter(treeOut[[x]])$id[[i-1]]
      }, error=function(e){c.parent_tax_id=NULL})
      
      row <- data.frame(cbind(tax_id=c.tax_id,parent_tax_id=c.parent_tax_id,
                              rank=c.rank, name=c.name))
      
      # Check if record exists
      if (nrow(merge(row,taxdata))==0) {
        taxdata <- bind_rows(taxdata, row)
      }
    }
  }, error=function(e){cat("ERROR :",conditionMessage(e), "\n")})
}

head(taxdata,3)
##taxdata 4 columns tax id rank name and parent tax id

## Match hierarchies to NSR species names by taxon IDs
## (Accounting for use of synonymous names within NCBI)
ncbiBackbone <- left_join(taxdata, taxidOut, by=c("tax_id"="id"),
                          suffix=c("","_nsr")) %>%
  left_join(., nsrSpecies[1:2], by=c("name_nsr" = "species_name")) %>%
  .[, c(1,6,4,2,3)] # Reaarange columns, drop 'name_nsr' column
ncbiBackbone <- unique(ncbiBackbone)

## Write NCBI data to file
write.csv(ncbiBackbone, file=sprintf('%s/data/in_files/tree_ncbi.csv', REPO_HOME),
          row.names=FALSE, fileEncoding="utf-8", na="")


## Clean up query variables
rm(x, i, row, list = ls()[grep("^c.", ls())])