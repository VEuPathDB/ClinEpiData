#!/usr/bin/env perl
use strict;
use warnings;

use lib $ENV{GUS_HOME} . "/lib/perl";
use ClinEpiData::Load::Utilities::OntologyMapping;
use Getopt::Long qw/:config no_ignore_case/;

my (@inputFiles,@protocols,$owlFile,$functionsFile,%funcToAdd,$sortByIRI,$varPrefix,$noEntities);
unless(0 < @ARGV){
	my $scr = basename($0);
	print join(" ", $scr, 
  'o|owlFile',
  'f|functionsFile','s|sortByIRI') . "\n";
	exit;
}

GetOptions(
  'i|inputFile=s' => \@inputFiles,
  'p|protocol=s' => \@protocols,
  'o|owlFile=s' => \$owlFile,
  'f|functionsFile=s' => \$functionsFile,
  's|sortByIRI!' => \$sortByIRI,
  'V|varPrefix=s' => \$varPrefix,
  'N|noEntities!' => \$noEntities,
);

my $om = ClinEpiData::Load::Utilities::OntologyMapping->new();

if($owlFile) {
  $om->run($owlFile,$functionsFile,$sortByIRI,$varPrefix,$noEntities);
}
elsif(@inputFiles) {
  #$om->setTerms($om->getTermsFromSourceFile(\@inputFiles,$functionsFile));
  $om->getOntologyXmlFromFiles(\@inputFiles,\@protocols);
  $om->printXml();
}


