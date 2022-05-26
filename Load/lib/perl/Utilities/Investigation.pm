package ClinEpiData::Load::Utilities::Investigation;

use strict;
use warnings;
use XML::Simple;


sub new {
  my ($class,$id) = @_;
  my $self = {};
  bless ($self, $class);
  $self->{xml}->{investigation} = {identifier => $id};
  return $self;
}

sub getInvestigationHash { return $_[0]->{xml} }

sub getXml {
  my ($self) = @_;
  return XMLout($self->{xml}, KeepRoot => 1, AttrIndent => 0);
}

sub addStudy{
  my ($self,$filename,$materialType,$idColumn,$protocol,$parentType,$parentIdColumn,$datasetName) = @_;
  my @nodes;
  my @edges;
  if($parentType && $parentIdColumn && $protocol){
    push(@edges, {input=>"PARENT",output=>"ENTITY", protocol=> [ $protocol ] });
    push(@nodes, {isaObject=>"Source",name=>"PARENT",type=>$parentType,idColumn => $parentIdColumn, suffix => "", useExactSuffix => "true" });
  }
  push(@nodes, {isaObject=>"Source",name=>"ENTITY",type=>$materialType,idColumn => $idColumn, suffix => "", useExactSuffix => "true" });
  $self->{xml}->{investigation}->{study} //= [];
  $self->{xml}->{investigation}->{identifier} //= "IDENT001";
  $self->{xml}->{investigation}->{identifierIsDirectoryName} //= "false";
  push( @{$self->{xml}->{investigation}->{study}}, { fileName => $filename, identifierSuffix => "-1", dataset => [$datasetName], node => \@nodes, edge => \@edges } );
}
1;

