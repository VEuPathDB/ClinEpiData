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

my ($owlFile) = @ARGV;

unless(-e $owlFile){
  $owlFile = join("/", $ENV{PROJECT_HOME}, "ApiCommonData/Load/ontology/release/production", $owlFile);
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
);

# if a value passes ANY test, it passes validation
my %validationTest = (
defaultBinWidth => [qw/nonzero empty/],
defaultDisplayRangeMax => [qw/numeric date empty/],
defaultDisplayRangeMin => [qw/numeric date empty/],
displayOrder => [qw/numeric empty/],
# hidden => [qw/"yes" empty/],
is_featured => [qw/"yes" empty/],
is_temporal => [qw/"yes" empty/],
mergeKey => [qw/"yes" empty/],
repeated => [qw/"yes" empty/],
# termType => [qw/category "multifilter" "value" "variable" "variable,derived" "value,derived"/],
displayType => [qw/"hidden" "multifilter"/], ## derived from hidden and termType
);

my $it = $owl->execute('get_entity_attributes');
while (my $row = $it->next) {
    my $termId = $row->{sid}->as_hash->{literal};
    my $attribName = $row->{ label }->as_hash->{literal};
    my $attribValue = $row->{ value }->as_hash->{literal};
  ##printf("%s\t%s\t%s\n", $termId, $attribName, $attribValue);
  next unless $keep{$attribName};
  $outputHashes{$termId} ||= {};
  $outputHashes{$termId}->{$attribName} ||= [];
  push(@{$outputHashes{$termId}->{$attribName}},$attribValue);
}

## post-processing in this while loop
while( my( $termId, $props ) = each %outputHashes ){
  my $hidden = defined($props->{hidden}->[0]) ? lc($props->{hidden}->[0]) : '';
  my $termType = defined($props->{termType}->[0]) ? lc($props->{termType}->[0]) : '';
  delete($outputHashes{$termId}->{termType});
  delete($outputHashes{$termId}->{hidden});
  next unless $hidden || $termType;
  # printf STDERR ("HIDDEN = $hidden, TERM_TYPE = $termType\n");
  if( $hidden eq 'yes' ){
    $outputHashes{$termId}->{displayType} = ['hidden'];
  }
  elsif( $termType eq 'multifilter' ){
    $outputHashes{$termId}->{displayType} = ['multifilter'];
  }
  # always delete hidden and termType
}

## do validation
while( my ($termId, $termHash) = each %outputHashes){
  while (my ($propName, $values) = each %$termHash){
    next unless $validationTest{$propName};
    foreach my $value (@$values){
      my $score = scalar @{$validationTest{$propName}}; # value assumed to be valid
      ## if $score reaches zero, all tests failed
      ## otherwise, it passed at least one test
      foreach my $test ( @{$validationTest{$propName}} ){ 
        switch ($test){
          case /^"(.+)"$/ { $score-- unless $value =~ /$1/ }
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
  


sub pp {
  my ($val) = @_;
  $val =~ s/^"|"$//g;
  return $val;
}



