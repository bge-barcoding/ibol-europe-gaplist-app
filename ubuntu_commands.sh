# Create virtual env (not needed to run for user?)
virtualenv -p /usr/bin/python2.7 venv

# activate environment
source venv/bin/activate

# install requirements (not needed to run for user?)
pip install sqlalchemy



# make reference db with fasta file
makeblastdb -in reference_db.fasta -dbtype nucl


# blast query with fasta file
blastn -query test.fasta -db reference_db.fasta -best_hit_overhang 0.1
# best overhang gives less but good hits