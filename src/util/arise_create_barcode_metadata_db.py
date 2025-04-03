import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from orm.common import Base
import argparse
import logging
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from orm.nsr_node import NsrNode
from orm.nsr_species import NsrSpecies
from orm.barcode import Barcode
from orm.marker import Marker
from orm.specimen import Specimen

main_logger = logging.getLogger('main')

if __name__ == '__main__':
    # process command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('-outfile', default="arise-barcode-metadata.db", help="Output file: SQLite DB")
    args = parser.parse_args()

    # create connection/engine to database file
    outfile = args.outfile
    engine = create_engine(f'sqlite:///{outfile}', echo=False)
    main_logger.info('create new database file: %s' % {outfile})

    # make session
    Session = sessionmaker(engine)
    session = Session()

    # create tables if they do not exist
    Base.metadata.create_all(engine)

    # commit transaction
    session.commit()
