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

sub getColExcludes {
    my ($self)=@_;
    my $colExcludes = $self->SUPER::getColExcludes();
     $colExcludes->{'__ALL__'}->{primary_key} = 1;
     $colExcludes->{'__ALL__'}->{parent} = 1;
    return $colExcludes;
}

sub cleanAndAddDerivedData{
    my ($self,$hash)=@_;
    
    if (defined $hash->{f5_age}){
	$hash->{age_days}= $hash->{f5_age};
    }else{
	$hash->{age_days}= $hash->{enrollment_age_days};
    }
    




}

1;
