package ClinEpiData::Load::AfricaReader;
use base qw(ClinEpiData::Load::MetadataReader);

sub cleanAndAddDerivedData {
  my ($self, $hash) = @_;
	$self->fixDate($hash);
}

sub fixDate {
  my ($self, $hash) = @_;
	if($hash->{today} =~ /^(\d\d)-(...)-(\d\d)$/){
		$hash->{today} = join("", $1, $2, '20', $3);
	}
}

1;

package ClinEpiData::Load::AfricaReader::HouseholdReader;
use base qw(ClinEpiData::Load::AfricaReader);


sub cleanAndAddDerivedData {
  my ($self, $hash) = @_;
	$self->SUPER::cleanAndAddDerivedData($hash);
}

sub makeParent {
  return undef; 
}
sub makePrimaryKey {
  my ($self, $hash) = @_;

  if($hash->{primary_key}) {
    return $hash->{primary_key};
  }

  return $hash->{hh_number};
}

1;

package ClinEpiData::Load::AfricaReader::ParticipantReader;
use base qw(ClinEpiData::Load::AfricaReader);

sub makeParent {
  my ($self, $hash) = @_;

  if($hash->{parent}) {
    return $hash->{parent};
  }

  return $hash->{hh_number};
}

sub makePrimaryKey {
  my ($self, $hash) = @_;

  if($hash->{primary_key}) {
    return $hash->{primary_key};
  }

  return $hash->{part_id};
}

1;

package ClinEpiData::Load::AfricaReader::ObservationReader;
use base qw(ClinEpiData::Load::AfricaReader);

sub cleanAndAddDerivedData {
  my ($self, $hash) = @_;
	$self->SUPER::cleanAndAddDerivedData($hash);
	$hash->{today_observation} = $hash->{today};
	delete $hash->{today};
	if($hash->{age_months} > 0){
		$hash->{age_years} = sprintf("%0.2f", $hash->{age_years} + ($hash->{age_months} / 12.0));
	}
}

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
	$self->fixDate($hash);
	return join("_", $hash->{part_id}, $hash->{today}); 
}

1;

package ClinEpiData::Load::AfricaReader::SampleReader;
use base qw(ClinEpiData::Load::AfricaReader);

sub makeParent {
  ## returns a Participant ID
  my ($self, $hash) = @_;
  if(defined($hash->{parent})) {
    return $hash->{parent};
  }
  return join("_", $hash->{part_id}, $hash->{today}); 
}

sub makePrimaryKey {
  my ($self, $hash) = @_;
  if($hash->{primary_key}) {
    return $hash->{primary_key};
  }
	$self->fixDate($hash);
  return join("_", $hash->{part_id}, $hash->{today}); 
}

sub getPrimaryKeySuffix {
	return "S";
}

1;
