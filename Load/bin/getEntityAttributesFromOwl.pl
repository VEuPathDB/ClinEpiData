#!/usr/bin/env perl
#
# Get class attributes from owl file
# Output JSON
use strict;
use warnings;

use lib $ENV{GUS_HOME} . "/lib/perl";

use ApiCommonData::Load::OwlReader;
use ClinEpiData::Load::Utilities::File qw/csv2arrayhash/;
use File::Basename;
use Switch;
use Scalar::Util qw/blessed looks_like_number/;
use JSON qw/to_json/;
use Data::Dumper;
use Getopt::Long qw/:config no_ignore_case/;

my $exitState = 0;
my ($expand, $fromOwl);
GetOptions( 'x|expandedOutput!' => \$expand );
my ($inFile) = @ARGV;

unless(-e $inFile){
  $inFile = join("/", $ENV{GUS_HOME}, "ontology/release/production", $inFile);
  $inFile .= ".owl" unless $inFile =~ /\.owl$/i;
}
if($inFile =~ /\.owl$/){ $fromOwl = 1 }

my %outputHashes;

my %keep = (
category => 0,
codebookDescription => 0,
plural => 0,
ordinal_values =>0,
codebookValues => 0,
dataFile => 0,
dataSet => 0,
defaultBinWidth => 1,
defaultDisplayRangeMax => 1,
defaultDisplayRangeMin => 1,
definition => 0,
displayOrder => 1,
hidden => 1,
is_featured => 1,
is_temporal => 1,
label => 0,
mergeKey => 1,
repeated => 1,
replaces => 1,
termType => 1,
unitIRI => 1,
unitLabel => 1,
variable => 1,
displayType => 1, ## derived annotation property
forceStringType => 1,
scale => 1,
);

# things that map to SRes.OntologySynonym columns
my %expandMap = (
  label => 'ontology_synonym',
  codebookDescription => 'definition',
  plural => 'plural',
  ordinal_values => 'ordinal_values',
);

# The column headers from INPUT to retain
my @expandHeaders = qw/label codebookDescription plural ordinal_values/;

my %expandVals;

my $forcedDisplayType = {
'EUPATH_0043203' => 'geoaggregator',
'EUPATH_0043204' => 'geoaggregator',
'EUPATH_0043205' => 'geoaggregator',
'EUPATH_0043206' => 'geoaggregator',
'EUPATH_0043207' => 'geoaggregator',
'EUPATH_0043208' => 'geoaggregator',
'OBI_0001620' => 'latitude',
'OBI_0001621'  => 'longitude'
};


# if a value passes ANY test, it passes validation
my %validationTest = (
defaultBinWidth => [qw/nonzero empty/],
defaultDisplayRangeMax => [qw/numeric "year" "month" "week" "day" empty/],
defaultDisplayRangeMin => [qw/numeric "year" "month" "week" "day" empty/],
displayOrder => [qw/numeric empty/],
hidden => [qw/"everywhere" "variableTree" "download" empty/],
is_featured => [qw/"yes" empty/],
is_temporal => [qw/"yes" empty/],
mergeKey => [qw/"yes" empty/],
repeated => [qw/"yes" empty/],
# termType => [qw/category "multifilter" "value" "variable" "variable,derived" "value,derived"/],
displayType => [qw/"multifilter" "geoaggregator" "latitude" "longitude"/], ## derived from termType
);

print STDERR "Reading $inFile\n";
if($fromOwl){
  my $owl = ApiCommonData::Load::OwlReader->new($inFile);
  my $it = $owl->execute('get_entity_attributes');
  while (my $row = $it->next) {
      my $termId = $row->{sid}->as_hash->{literal};
      my $attribName = $row->{ label }->as_hash->{literal};
      my $attribValue = $row->{ value }->as_hash->{literal};
    ##printf("%s\t%s\t%s\n", $termId, $attribName, $attribValue);
    next unless $keep{$attribName};
    next unless ($attribValue ne "");
    $outputHashes{$termId} ||= {};
    $outputHashes{$termId}->{$attribName} ||= [];
    push(@{$outputHashes{$termId}->{$attribName}},$attribValue);
  }
}
else { # tabular/conversion file
  my $csv = csv2arrayhash($inFile);
  foreach my $row (@$csv) {
    my $termId = basename($row->{IRI});
    foreach my $attribName (keys %$row){
      next unless($keep{$attribName});
      my @values = map { s/^\s*|\s*$//g; $_ } split(/\|/, $row->{$attribName});
      next unless (@values);
      $outputHashes{$termId} ||= {};
      $outputHashes{$termId}->{$attribName} ||= [];
      $outputHashes{$termId}->{$attribName} = \@values;
    }
    ## expanded cols
    foreach my $attribName (@expandHeaders){
      my $alias = $expandMap{$attribName};
      next unless $row->{$attribName};
      $expandVals{$termId}->{$attribName} = defined($row->{$attribName}) ? $row->{$attribName} : "";
      if($alias && !$expandVals{$termId}->{$attribName}){
        $expandVals{$termId}->{$attribName} ||= defined($row->{$alias}) ? $row->{$alias} : "";
      }
    }
  }
}

## post-processing in this while loop
while( my( $termId, $props ) = each %outputHashes ){
  my $termType = defined($props->{termType}->[0]) ? $props->{termType}->[0] : '';
  delete($outputHashes{$termId}->{termType});
  if( defined($forcedDisplayType->{$termId})  ){
    $outputHashes{$termId}->{displayType} = [$forcedDisplayType->{$termId}];
  }
  if( $termType eq 'multifilter' ){
    $outputHashes{$termId}->{displayType} = ['multifilter'];
  }
}

## do validation
while( my ($termId, $termHash) = each %outputHashes){
  while (my ($propName, $values) = each %$termHash){
    next unless $validationTest{$propName};
    if( 1 > @$values ){ delete $outputHashes{$termId}->{$propName}; next }
    foreach my $value (@$values){
      my $score = scalar @{$validationTest{$propName}}; # value assumed to be valid
      ## if $score reaches zero, all tests failed
      ## otherwise, it passed at least one test
      foreach my $test ( @{$validationTest{$propName}} ){ 
        switch ($test){
          case /^".+"$/ { my ($word) = ($test =~ /"(.+)"/); $score-- unless $value eq $word }
          case 'numeric' { $score-- unless looks_like_number($value) }
          case 'nonzero' { $score-- unless $value != '0' }
          case 'date' { $score-- unless $value =~ /^\d{4}-\d{2}-\d{2}$/ }
          case 'empty' { $score-- unless $value =~ /^$/ }
        }
      }
      if($score <= 0){
        $exitState++;
        printf STDERR ("FAIL: %s %s [%s] TESTS:%s\n",
          $termId, $propName, $value, join(", ", @{$validationTest{$propName}}))
      }
    }
  }
}

my $max = 0;

my @headers = qw/SOURCE_ID annotation_properties/;
if($expand){ push(@headers, map { $expandMap{$_} } @expandHeaders) }

printf("%s\n", join("\t", @headers));
foreach my $termId (sort keys %outputHashes){
  my $json = to_json($outputHashes{$termId});
  next if $json eq '{}';
  if(length($json) > $max){ $max = length($json) }
  my @row = ($termId, $json);
  if($expand){
    foreach my $attribName (@expandHeaders){
      push(@row, $expandVals{$termId}->{$attribName} || "" );
    }
  }
  
  printf("%s\n", join("\t", @row));
}

print STDERR ("Longest string was $max chars\n");

exit $exitState;

