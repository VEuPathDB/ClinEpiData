#!usr/bin/perl

use strict;

use XML::Simple;

my $ontologyMappingFile = $ARGV[0];

unless(-e $ontologyMappingFile) {
    die "OntologyMappingFile does not exist:  $ontologyMappingFile";
}

my $xml = XMLin($ontologyMappingFile);

foreach my $name (keys %{$xml->{ontologyTerm}}) {
    my $sourceId = $xml->{ontologyTerm}->{$name}->{source_id};
    print "$sourceId\t$name\n";
}
