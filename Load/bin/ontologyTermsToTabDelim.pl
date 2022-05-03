#!/usr/bin/perl

use strict;

use XML::Simple;

use Data::Dumper;

my $ontologyMappingFile = $ARGV[0];
my $outputDir = $ARGV[1];


open(TERMS, ">$outputDir/ontology_terms.txt") or die "Cannot open file $outputDir/ontology_terms.txt for writing";
open(RELS, ">$outputDir/ontology_relationships.txt") or die "Cannot open file $outputDir/ontology_relationships.txt for writing";

unless(-e $ontologyMappingFile) {
    die "OntologyMappingFile does not exist:  $ontologyMappingFile";
}

my $xml = XMLin($ontologyMappingFile);

my $entityType;
foreach my $name (keys %{$xml->{ontologyTerm}}) {
    next if($name =~ /INTERNAL/);
    my $sourceId = $xml->{ontologyTerm}->{$name}->{source_id};

    if($xml->{ontologyTerm}->{$name}->{type} eq 'materialType') {
        $entityType = $sourceId;
    }
}

foreach my $name (keys %{$xml->{ontologyTerm}}) {
    next if($name =~ /INTERNAL/);
    my $sourceId = $xml->{ontologyTerm}->{$name}->{source_id};
    print TERMS "$sourceId\t$name\n";

    unless($xml->{ontologyTerm}->{$name}->{type} eq 'materialType') {
        print RELS "$sourceId\tsubClassOf\t$entityType\n";
    }
}

close TERMS;
close RELS;
