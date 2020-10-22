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
  my ($self,$filename,$materialType,$idColumn,$protocol,$parentType,$parentIdColumn) = @_;
  my @nodes;
  my @edges;
  push(@nodes, {isaObject=>"Source",name=>"ENTITY",type=>$materialType,idColumn => $idColumn });
  if($parentType && $parentIdColumn && $protocol){
    push(@nodes, {isaObject=>"Source",name=>"PARENT",type=>$parentType,idColumn => $parentIdColumn });
    push(@edges, {input=>"PARENT",output=>"ENTITY", protocol=> [ $protocol ] });
  }
  $self->{xml}->{investigation}->{study} //= [];
  push( @{$self->{xml}->{investigation}->{study}}, { fileName => $filename, node => \@nodes, edge => \@edges } );
}
1;

