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

my $owl = ApiCommonData::Load::OwlReader->new($owlFile);

my %outputHashes;

##printf STDERR ("Getting attributes\n");
my $it = $owl->execute('get_attributes');
my %sourceId;
while (my $row = $it->next) {
  my $attribName = pp($row->{label}->as_sparql);
  my $attribIri = pp($row->{IRI}->as_sparql);
  $sourceId{$attribName} = $attribIri;
  # printf STDERR ("\t$attribName = $attribIri\n");
}

## printf STDERR ("Getting attribute values\n");
while(my ($attribName, $attribIri) = each %sourceId){
  my $it2 = $owl->execute('get_entity_attributes',{ ATTRIB_NAME => $attribName, ATTRIB_IRI => $attribIri } );
  while (my $row = $it2->next) {
    my $attribValue = pp($row->{ $attribName }->as_sparql);
    my $termId = pp(basename($row->{entity}->as_sparql));
    ##printf("%s\t%s\t%s\n", $termId, $attribName, $attribValue);
    $outputHashes{$termId} ||= {};
    $outputHashes{$termId}->{$attribName} ||= [];
    push(@{$outputHashes{$termId}->{$attribName}},$attribValue);
  }
}

my $max = 0;

printf("SOURCE_ID\tflags\n");
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
