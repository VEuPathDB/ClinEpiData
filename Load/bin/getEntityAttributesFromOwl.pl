#!/usr/bin/env perl
#
# Get class attributes from owl file
# Output JSON
use strict;
use warnings;

use lib $ENV{GUS_HOME} . "/lib/perl";

use ApiCommonData::Load::OwlReader;

use File::Basename;

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

my $it = $owl->execute('get_entity_attributes');
while (my $row = $it->next) {
  my $termId = pp(basename($row->{entity}->as_sparql));
  my $attribName = pp($row->{ label }->as_sparql);
  my $attribValue = pp($row->{ value }->as_sparql);
  ##printf("%s\t%s\t%s\n", $termId, $attribName, $attribValue);
  next unless $keep{$attribName};
  $outputHashes{$termId} ||= {};
  $outputHashes{$termId}->{$attribName} ||= [];
  push(@{$outputHashes{$termId}->{$attribName}},$attribValue);
}

## post-processing in this while loop
while( my( $termId, $props ) = each %outputHashes ){
  my $hidden = lc($props->{hidden}->[0]);
  my $termType = lc($props->{termType}->[0]);
  delete($outputHashes{$termId}->{termType});
  delete($outputHashes{$termId}->{hidden});
  next unless $hidden || $termType;
  printf STDERR ("HIDDEN = $hidden, TERM_TYPE = $termType\n");
  if( $hidden eq 'yes' ){
    $outputHashes{$termId}->{displayType} = ['hidden'];
  }
  elsif( $termType eq 'multifilter' ){
    $outputHashes{$termId}->{displayType} = ['multifilter'];
  }
  # always delete hidden and termType
}

my $max = 0;

printf("SOURCE_ID\tannotation_properties\n");
foreach my $termId (sort keys %outputHashes){
  my $json = to_json($outputHashes{$termId});
  if(length($json) > $max){ $max = length($json) }
  printf("%s\t%s\n", $termId, $json);
}

print STDERR ("Longest string was $max chars\n");
  


sub pp {
  my ($val) = @_;
  $val =~ s/^"|"$//g;
  return $val;
}
