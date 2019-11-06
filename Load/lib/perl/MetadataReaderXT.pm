package ClinEpiData::Load::MetadataReaderXT;
use base qw(ClinEpiData::Load::MetadataReader);
# XT = Extended: use valueMap.txt as ancillary input file 
# apply value mapping and/or replace variables with IRI (Source ID)
use Data::Dumper;
use strict;
use warnings;

sub readAncillaryInputFile {
# use for 1. value mapping, and 2. IRI mapping
  my ($self, $ancFile) = @_;
  open(FH, "<$ancFile") or die "Cannot read $ancFile:$!\n";
  my $anc = {};
  while(my $row = <FH>){
    chomp $row;
    $row =~ s/(\r|\l)$//g;
    my($col, $iri, $val, $mapVal) = split(/\t/,$row);
    if(defined($val) && defined($mapVal)){
      $anc->{var}->{lc($col)}->{lc($val)} =$mapVal;
    }
    $anc->{iri}->{lc($col)}= lc($iri);
  }
  close(FH);
  return $anc;
}

sub applyMappedValues {
  my ($self,$hash) = @_;
  my $anc = $self->getAncillaryData();
  while(my ($k, $v) = each %$hash){
    $k = lc($k);
    next unless(defined($v));
    $v = lc($v);
    if(
      defined($anc->{var}->{$k}) &&
      defined($anc->{var}->{$k}->{$v})){
      if($anc->{var}->{$k}->{$v} eq ':::UNDEF:::'){
        delete($hash->{$k});
      }
      else {
        $hash->{$k} = $anc->{var}->{$k}->{$v};
      }
    }
    if(defined($hash->{$k}) && $hash->{$k} =~ /undef/i){ die Dumper $hash }
  }
}

sub applyMappedIRI {
  my ($self,$hash) = @_;
  my $anc = $self->getAncillaryData();
  while(my ($col, $val) = each %$hash){
    my $iri = $anc->{iri}->{$col};
    if($iri){
      $hash->{$iri} = $hash->{$col};
      delete($hash->{$col});
    }
  }
}

1;
