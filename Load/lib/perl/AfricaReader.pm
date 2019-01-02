package ClinEpiData::Load::SrnAfricaReader;
use base qw(ClinEpiData::Load::MetadataReader);

1;

package ClinEpiData::Load::SrnAfricaReader::HouseholdReader;
use base qw(ClinEpiData::Load::SrnAfricaReader);


#sub cleanAndAddDerivedData {
#  my ($self, $hash) = @_;
#}

sub makeParent {
  return undef; 
}
sub makePrimaryKey {
  my ($self, $hash) = @_;

  if($hash->{"primary_key"}) {
    return $hash->{"primary_key"};
  }

  return $hash->{hh_number};
}

1;

package ClinEpiData::Load::SrnAfricaReader::ParticipantReader;
use base qw(ClinEpiData::Load::SrnAfricaReader);

sub cleanAndAddDerivedData {
  my ($self, $hash) = @_;
	if($hash->{is_participating} eq '0'){ $self->skipRow($hash); }
}

sub makeParent {
  my ($self, $hash) = @_;

  if($hash->{"primary_key"}) {
    return $hash->{"primary_key"};
  }

  return $hash->{hh_number};
}

sub makePrimaryKey {
  my ($self, $hash) = @_;

  if($hash->{"primary_key"}) {
    return $hash->{"primary_key"};
  }

  return $hash->{part_id};
}

1;

package ClinEpiData::Load::SrnAfricaReader::ObservationReader;
use base qw(ClinEpiData::Load::SrnAfricaReader);

sub makeParent {
  ## returns a Participant ID
  my ($self, $hash) = @_;
  if($hash->{parent}) {
    return $hash->{parent};
  }
  return $hash->{part_id}; 
}

sub makePrimaryKey {
  my ($self, $hash) = @_;
  if($hash->{primary_key}) {
    return $hash->{primary_key};
  }
  return join("_", $hash->{part_id}, $hash->{today}); 
}

1;

package ClinEpiData::Load::SrnAfricaReader::SampleReader;
use base qw(ClinEpiData::Load::SrnAfricaReader);

sub makeParent {
  ## returns a Participant ID
  my ($self, $hash) = @_;
  if($hash->{parent}) {
    return $hash->{parent};
  }
  return join("_", $hash->{part_id}, $hash->{today}); 
}

sub makePrimaryKey {
  my ($self, $hash) = @_;
  if($hash->{primary_key}) {
    return $hash->{primary_key};
  }
  return join("_", $hash->{part_id}, $hash->{today}); 
}

sub getPrimaryKeySuffix {
	return "S";
}

#sub cleanAndAddDerivedData {
#  my ($self, $hash) = @_;
#}

1;
