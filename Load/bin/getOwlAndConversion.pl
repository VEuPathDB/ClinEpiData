#!/usr/bin/env perl
use strict;
use warnings;

use XML::Simple qw/XMLin/;
use Data::Dumper;
use File::Basename qw/dirname/;

my $ontopath = $ENV{'PROJECT_HOME'} . '/ApiCommonData/Load/ontology';

my $f = XMLin(sprintf("%s/ClinEpiDatasets/Datasets/lib/xml/datasets/ClinEpiDB.xml", $ENV{'PROJECT_HOME'}));

printf("%s\n", join(",", "datasetName", "owl", "conversion" ));
foreach my $ds ( @{$f->{dataset}} ){
  next unless $ds->{class} eq 'ISASimpleNF';

  my ($subproj, $group, $study,$onto) = (
    $ds->{prop}->{subProjectName}->{content},
    $ds->{prop}->{groupName}->{content},
    $ds->{prop}->{studyName}->{content},
    $ds->{prop}->{webDisplayOntologyName}->{content},
  );
  my $dsname = join("_", "ISASimple", $subproj, $group, $study, "RSRC");

  my $suffix = lc($subproj);
  my $csvname = $onto;
  $csvname =~ s/${suffix}_//;
  my $subpath = join("/", $ontopath, $subproj);
  my $owlpath = sprintf("$subpath/*/%s.owl", $onto);
  my ($owlfile) = glob( $owlpath );
  my ($csvpath, $csvfile) = ("NO OWL", "NO OWL");
  if($owlfile){
    my $ins = "*/" x 1;
    $csvpath = sprintf("%s/$ins*conversion.csv", dirname($owlfile));
    ($csvfile) = glob( $csvpath );
  }
  $owlfile =~ s/$ENV{PROJECT_HOME}\/// if $owlfile;
  $csvfile =~ s/$ENV{PROJECT_HOME}\/// if $csvfile;
  printf("%s\n", join(",", $dsname, $owlfile || "NOT FOUND" ,$csvfile || "NOT FOUND" ));
}
