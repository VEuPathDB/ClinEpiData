#!/usr/bin/env perl
use strict;
use warnings;

use lib $ENV{GUS_HOME} . "/lib/perl";
use ClinEpiData::Load::Utilities::OntologyMapping;
use Getopt::Long;

my ($inputFile,$owlFile,$functionsFile,%funcToAdd,$sortByIRI,$hhobs);
unless(0 < @ARGV){
	my $scr = basename($0);
	print join(" ", $scr, 
  'o|owlFile',
  'f|functionsFile','s|sortByIRI') . "\n";
	exit;
}

GetOptions(
  'i|inputFile=s' => \$inputFile,
  'o|owlFile=s' => \$owlFile,
  'f|functionsFile=s' => \$functionsFile,
  's|sortByIRI!' => \$sortByIRI,
  'h|householdObservationProtocol!' => \$hhobs
);

my $om = ClinEpiData::Load::Utilities::OntologyMapping->new();

if($owlFile) {
  $om->run($owlFile,$functionsFile,$sortByIRI);
}
elsif($inputFile) {
  $om->setTerms($om->getTermsFromSourceFile($inputFile,$functionsFile));
  $om->printXml();
}


