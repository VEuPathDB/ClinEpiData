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

      return $hash->{primary_key};
  

}



sub cleanAndAddDerivedData{
    my ($self,$hash)=@_;


    if (defined $hash->{f5_age}){
        $hash->{age_days}= $hash->{f5_age};
    }else{
        $hash->{age_days}= $hash->{enrollment_age_days};
    }

    if ($hash->{observationprotocol}  eq 'enrollment, outcome if additional rehydration needed'){
	$hash->{observationprotocol} = 'Enrollment, Outcome if additional rehydration needed';
    }
    if($hash->{observationprotocol}  eq 'enrollment, outcome 4 hours after rehydration'){
	$hash->{observationprotocol} = 'Enrollment, Outcome 4 hours after rehydration';
    }
    if($hash->{observationprotocol} eq 'enrollment, last outcome'){
	$hash->{observationprotocol} = 'Enrollment, Last Outcome';
    }
    if($hash->{observationprotocol} eq 'enrollment, outcome leaving hospital/health center'){
	$hash->{observationprotocol} = 'Enrollment, Outcome leaving hospital/health center';
    } 
}



sub getColExcludes {
    my ($self)=@_;
    my $colExcludes = $self->SUPER::getColExcludes();
     $colExcludes->{'__ALL__'}->{primary_key} = 1;
     $colExcludes->{'__ALL__'}->{parent} = 1;
    return $colExcludes;
}


1;
