#!/usr/bin/perl

use strict;
use warnings;

use lib $ENV{GUS_HOME} . "/lib/perl";

use ClinEpiData::Load::MetadataHelper;
use JSON;

my ($owlFile,$filters) = @ARGV;
# my $metadataHelper = ClinEpiData::Load::MetadataHelper->new($type, \@metadataFiles, $rowExcludeFile, $colExcludeFile, $parentMergedFile, $parentType, $ontologyMappingXmlFile, $ancillaryInputFile, $packageName);
my $packageName = "ClinEpiData::Load::MetadataReader";
my $metadataHelper = ClinEpiData::Load::MetadataHelper->new(undef, undef, undef, undef, undef, undef, undef, undef, $packageName);

my ($treeObjRoot, $nodeLookup) = $metadataHelper->makeTreeObjFromOntology($owlFile, $filters);

my $treeHashRef = $treeObjRoot->transformToHashRef(1);
my $json_text = to_json($treeHashRef,{utf8=>1, pretty=>1});

print "$json_text\n";


