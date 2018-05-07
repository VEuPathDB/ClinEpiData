package ClinEpiData::Load::GatesGEMSReader;
use base qw(ClinEpiData::Load::MetadataReader);

sub cleanAndAddDerivedData {
  my ($self, $hash) = @_;
}

1;

package ClinEpiData::Load::GatesGEMSReader::HouseholdReader;
use base qw(ClinEpiData::Load::GatesGEMSReader);
# use Data::Dumper;
## loads file micro_x24m.csv
use strict;
use warnings;

sub makeParent {
  my ($self, $hash) = @_;
  return undef;
}

sub makePrimaryKey {
  my ($self, $hash) = @_;
  if($hash->{"primary_key"}) {
    return $hash->{"primary_key"};
  }
  return $hash->{center};
}

# sub cleanAndAddDerivedData {
#   my ($self, $hash) = @_;
#   $self->SUPER::cleanAndAddDerivedData($hash);
# }
1;

package ClinEpiData::Load::GatesGEMSReader::ParticipantReader;
use base qw(ClinEpiData::Load::GatesGEMSReader);

sub makeParent {
  my ($self, $hash) = @_;
  if($hash->{parent}) {
    return $hash->{parent};
  }
  return $hash->{center};
}
sub makePrimaryKey {
  my ($self, $hash) = @_;

  if($hash->{primary_key}) {
    return $hash->{primary_key};
  }

  return $hash->{childid};
}

1;

package ClinEpiData::Load::GatesGEMSReader::ObservationReader;
use base qw(ClinEpiData::Load::GatesGEMSReader);

sub makeParent {
  ## returns a Participant ID
  my ($self, $hash) = @_;
  if($hash->{"parent"}) {
    return $hash->{"parent"};
  }
  return $hash->{childid};
}

sub makePrimaryKey {
  my ($self, $hash) = @_;
  if($hash->{"primary_key"}) {
    return $hash->{"primary_key"};
  }
	return $hash->{caseid};
}

package ClinEpiData::Load::GatesGEMSReader::SampleReader;
use base qw(ClinEpiData::Load::GatesGEMSReader);

sub makeParent {
  ## returns a Participant ID
  my ($self, $hash) = @_;
  if($hash->{"parent"}) {
    return $hash->{"parent"};
  }
	return $hash->{caseid};
}

sub makePrimaryKey {
  my ($self, $hash) = @_;
  if($hash->{"primary_key"}) {
    return $hash->{"primary_key"};
  }
	return $hash->{lab_specimen_id};
}

1;
