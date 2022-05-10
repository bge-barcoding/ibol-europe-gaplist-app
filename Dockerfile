FROM ubuntu:22.04

RUN apt-get update && apt-get install -y \
	cpanminus \
	sqlite

RUN cpanm -n Bio::Phylo::Forest::DBTree
