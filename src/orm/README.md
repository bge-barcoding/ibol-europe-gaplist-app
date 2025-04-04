# DNA Barcode Tracking System

This system tracks DNA barcodes produced by the BGE project under iBOL Europe. It provides a framework for capturing a 
static species list, their taxonomic hierarchy, and synonyms, mapping specimens and their barcodes (currently focusing 
on COI-5P markers) from BOLD workbench projects, and integrating public data from BOLD to identify gaps and drive 
targeted specimen collection.

## Database Structure

The system uses SQLAlchemy ORM to define and interact with the following database tables:

### Core Tables

- **NsrSpecies**: Stores canonical species information with occurrence status
- **NsrNode**: Implements a nested set model for taxonomic hierarchy 
- **NsrSynonym**: Maps alternative species names (synonyms) to their canonical forms
- **Specimen**: Stores physical specimen information linked to species
- **Marker**: Defines types of genetic markers being tracked (e.g., COI-5P)
- **Barcode**: Stores the actual barcode data with links to specimens and markers

### Auxiliary Modules

- **Common**: Contains shared constants, utility functions, and base classes

## File Listing

| File | Description |
|------|-------------|
| [`nsr_species.py`](./nsr_species.py) | Defines the NsrSpecies model for storing canonical species names and their occurrence status |
| [`nsr_node.py`](./nsr_node.py) | Implements a nested set model for the taxonomic hierarchy with extensive utility methods |
| [`nsr_synonym.py`](./nsr_synonym.py) | Maps alternative species names to their canonical forms |
| [`specimen.py`](./specimen.py) | Defines the Specimen model, representing physical specimens linked to species |
| [`marker.py`](./marker.py) | Defines the Marker model for types of genetic markers being sequenced |
| [`barcode.py`](./barcode.py) | Stores barcode data with links to specimens and markers |
| [`common.py`](./common.py) | Contains shared constants, enums, and utility functions |

## Usage

This system serves several purposes:

1. **Taxonomy Management**: Capturing and maintaining consistent taxonomic information
2. **Specimen Tracking**: Tracking physical specimens and their metadata
3. **Barcode Repository**: Storing DNA barcode data from various sources
4. **Collection Planning**: Analyzing coverage to identify taxonomic gaps for targeted specimen collection

## Data Flow

1. A static species list with taxonomy is loaded into `nsr_species`, `nsr_node`, and `nsr_synonym` tables
2. Specimen data from BOLD workbench projects is mapped to the species list and stored in the `specimen` table
3. Barcode data from both BGE-produced specimens and existing public BOLD data are linked to specimens
4. Analysis tools use this data to guide further specimen collection efforts

## Key Features

- **Hierarchical Taxonomy**: Full taxonomic hierarchy from kingdom to species using nested set model
- **Synonym Management**: Comprehensive handling of taxonomic synonyms
- **Fast Lookups**: Specialized utility functions for optimized database access
- **Flexible Data Sources**: Support for multiple barcode data sources (BOLD, NCBI, etc.)
- **Data Validation**: Built-in validation for critical fields
