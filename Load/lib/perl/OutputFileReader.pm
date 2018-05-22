package ClinEpiData::Load::OutputFileReader;
use base qw(ClinEpiData::Load::MetadataReader);
use strict;

sub makeParent {
  my ($self, $hash) = @_;
  if($hash->{parent}) {
    return $hash->{parent};
  }
  return undef;
}

sub makePrimaryKey {
  my ($self, $hash) = @_;
  
  return $hash->{"primary_key"};
}

1;
