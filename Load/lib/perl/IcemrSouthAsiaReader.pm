package ClinEpiData::Load::IcemrSouthAsiaReader;
use base qw(ClinEpiData::Load::MetadataReader);

sub cleanAndAddDerivedData {
  my ($self, $hash) = @_;
	for my $field ((
		'x12._temperature_reading_date',
		'x30._antimalarial_therapy_initiation_at_gmc_date',
		'x14._blood_volume_collected_.ml.',
		'x53a._dose')){
		if(defined($hash->{$field}) &&
			$hash->{$field} eq 'na'){
			delete $hash->{$field};
		}
		$hash->{$field} =~ s/_([ap])m$/$1m/;
	}
	for my $field ((
		'x15._collection_time_.24h.',
		'x31._antimalarial_therapy_initiation_at_gmc_time',
		'x11._temperature_reading_time_.24h.'
		)){
		if(defined($hash->{$field}) &&
			$hash->{$field} eq 'na'){
			delete $hash->{$field};
		}
		$hash->{$field} =~ s/_([ap])m$/$1m/;
		$hash->{$field} =~ s/^0:/12:/;
	}
}
1;

package ClinEpiData::Load::IcemrSouthAsiaReader::ParticipantReader;
use base qw(ClinEpiData::Load::IcemrSouthAsiaReader);

sub cleanAndAddDerivedData {
  my ($self, $hash) = @_;
# not needed:
#  $self->SUPER::cleanAndAddDerivedData($hash);
	if(defined($hash->{'x9._time_enrolled'})){ 
		delete $hash->{'x9._time_enrolled'} if($hash->{'x9._time_enrolled'} eq 'na');
		$hash->{'x9._time_enrolled'} =~ s/^0:/12:/;
	}
	if(defined($hash->{'x8._date_enrolled'}) && $hash->{'x8._date_enrolled'} eq 'na'){ 
		delete $hash->{'x8._date_enrolled'};
	}
}

sub makeParent {
	return undef;
}

sub makePrimaryKey {
  my ($self, $hash) = @_;
  if($hash->{primary_key}) {
    return $hash->{primary_key};
  }
  return defined($hash->{'x1._participant_id'}) ? $hash->{'x1._participant_id'} : $hash->{'participant_id'};
}
1;

package ClinEpiData::Load::IcemrSouthAsiaReader::ObservationReader;
use base qw(ClinEpiData::Load::IcemrSouthAsiaReader);
use Data::Dumper;
sub cleanAndAddDerivedData {
  my ($self, $hash) = @_;
  $self->SUPER::cleanAndAddDerivedData($hash);
}

sub makeParent {
  my ($self, $hash) = @_;
  if($hash->{parent}) {
    return $hash->{parent};
  }
	die sprintf("No parent id in %s\n", $self->getMetadataFile) unless length($hash->{'participant_id'}) > 0;
  return $hash->{'participant_id'};
}

sub makePrimaryKey {
  my ($self, $hash) = @_;
  if($hash->{primary_key}) {
    return $hash->{primary_key};
  }
	my $date = $hash->{'date'};
	$date ||= $hash->{'x12._temperature_reading_date'}; 
	$date ||= $hash->{'x16._collection_date'};
	$date ||= $hash->{'x68._date_of_observation_collection'};

	unless($date){
		my $parent = $self->getParentParsedOutput()->{$hash->{participant_id}};
		$date = $parent->{'x8._date_enrolled'};
		$date ||= $parent->{'x34._age_.at_enrollment.'};
	}
	unless($date){
		printf STDERR ("No date available: %s: %s\n",
			$self->getMetadataFile(), $hash->{participant_id});
		print STDERR Dumper $parent; die;
	}
  return join("_", $hash->{participant_id}, $date);
}
1;
