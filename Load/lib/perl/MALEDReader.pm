package ClinEpiData::Load::MALEDReader;
use base qw(ClinEpiData::Load::MetadataReader);

1;

package ClinEpiData::Load::MALEDReader::ParticipantReader;
use base qw(ClinEpiData::Load::MALEDReader);

sub makeParent {
  return undef; 
}
sub makePrimaryKey {
  my ($self, $hash) = @_;

  if($hash->{"primary_key"}) {
    return $hash->{"primary_key"};
  }

  return $hash->{pid};
}

1;

package ClinEpiData::Load::MALEDReader::StoolSampleReader;
use base qw(ClinEpiData::Load::MALEDReader);
## loads file micro_x24m.csv
use strict;
use warnings;

sub makeParent {
  ## build the Observation primary key
  my ($self, $hash) = @_;
  return join("_", $hash->{pid}, $hash->{agedays});
}

sub makePrimaryKey {
  my ($self, $hash) = @_;
  return $hash->{srfmbsampid};
}

1;

package ClinEpiData::Load::MALEDReader::ObservationReader;
use base qw(ClinEpiData::Load::MALEDReader);
## loads files illnessfull_24m.csv, Zscores_24m.csv, micro_24m.csv

sub makeParent {
  ## returns a Participant ID
  my ($self, $hash) = @_;
  return $hash->{pid}; 
}

sub makePrimaryKey {
  my ($self, $hash) = @_;
  if($hash->{"primary_key"}) {
    return $hash->{"primary_key"};
  }
  die unless(defined($hash->{agedays}) || defined($hash->{age}));
  my $age = defined($hash->{agedays}) ? $hash->{agedays} : $hash->{age};

  return sprintf("%s_%d", $hash->{pid}, $age);
}

1;
