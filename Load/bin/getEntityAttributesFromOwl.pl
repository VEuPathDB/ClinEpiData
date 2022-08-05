#!/usr/bin/env perl
#
# Get class attributes from owl file
# Output JSON
use strict;
use warnings;

use lib $ENV{GUS_HOME} . "/lib/perl";

use ApiCommonData::Load::OwlReader;
use File::Basename;
use Switch;
use Scalar::Util qw/blessed looks_like_number/;
use JSON qw/to_json/;
use Data::Dumper;

my ($owlFile) = @ARGV;

unless(-e $owlFile){
  $owlFile = join("/", $ENV{GUS_HOME}, "ontology/release/production", $owlFile);
  $owlFile .= ".owl" unless $owlFile =~ /\.owl$/i;
}
print STDERR "Reading $owlFile\n";

my $owl = ApiCommonData::Load::OwlReader->new($owlFile);

my %outputHashes;

my %keep = (
category => 0,
codebookDescription => 0,
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
        printf STDERR ("FAIL: %s %s [%s] TESTS:%s\n",
          $termId, $propName, $value, join(", ", @{$validationTest{$propName}}))
      }
    }
  }
}

my $max = 0;

printf("SOURCE_ID\tannotation_properties\n");
foreach my $termId (sort keys %outputHashes){
  my $json = to_json($outputHashes{$termId});
  next if $json eq '{}';
  if(length($json) > $max){ $max = length($json) }
  printf("%s\t%s\n", $termId, $json);
}

print STDERR ("Longest string was $max chars\n");


