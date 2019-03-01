package ClinEpiData::Load::Prism2Reader;
use base qw(ClinEpiData::Load::MetadataReader);

1;

package ClinEpiData::Load::Prism2Reader::HouseholdReader;
use base qw(ClinEpiData::Load::Prism2Reader);

use strict;

sub makeParent {
  return undef;
}

sub makePrimaryKey {
  my ($self, $hash) = @_;

  if($hash->{"primary_key"}) {
    return $hash->{"primary_key"};
  }

  return $hash->{hhid};
}


1;

package ClinEpiData::Load::Prism2Reader::ParticipantReader;
use base qw(ClinEpiData::Load::Prism2Reader);
use POSIX;

use strict;

sub makeParent {
  my ($self, $hash) = @_;

  return $hash->{hhid};
}

sub makePrimaryKey {
  my ($self, $hash) = @_;

  if($hash->{"primary_key"}) {
    return $hash->{"primary_key"};
  }

  return $hash->{cohortid};
}


1;

package ClinEpiData::Load::Prism2Reader::ObservationReader;
use base qw(ClinEpiData::Load::Prism2Reader);

use strict;

sub makePrimaryKey {
  my ($self, $hash) = @_;
  if($hash->{primary_key}) {
    return $hash->{primary_key};
  }
  return $hash->{uniqueid};
}


sub makeParent {
  my ($self, $hash) = @_;

  return $hash->{cohortid};
}

1;



package ClinEpiData::Load::Prism2Reader::LightTrapReader;
use base qw(ClinEpiData::Load::Prism2Reader);

use strict;

my %type = (
	'cdc light trap' => 'clt',
	'resting collections' => 'rt'
);

sub cleanAndAddDerivedData {
  my ($self, $hash) = @_;

  if($hash->{date}) {
    $hash->{collectiondate} = $hash->{date};
  }
}


sub makePrimaryKey {
  my ($self, $hash) = @_;
  return join("_",$hash->{hhid}, $hash->{date}, $type{$hash->{trapmethod}},$hash->{roomnumm});
}


sub makeParent {
  my ($self, $hash) = @_;
  return $hash->{hhid};
}

1;

package ClinEpiData::Load::Prism2Reader::SampleReader;
use base qw(ClinEpiData::Load::Prism2Reader);

use strict;

sub makePrimaryKey {
  my ($self, $hash) = @_;
  return $hash->{uniqueid};
}

sub getPrimaryKeyPrefix {
	return "S";
}


sub makeParent {
  my ($self, $hash) = @_;

  return $hash->{uniqueid};
}

1;

