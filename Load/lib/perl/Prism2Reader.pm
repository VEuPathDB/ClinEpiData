package ClinEpiData::Load::Prism2Reader;
use base qw(ClinEpiData::Load::MetadataReader);
use Data::Dumper;


sub filterNaRows {
  my ($self, $hash) = @_;
	if(
		$hash->{visittype} eq 'na' && 
		$hash->{timetobed} eq 'na' &&
		$hash->{participantdie} eq 'na'
	){
		#printf STDERR ("Skipping NA row %s\n", $hash->{uniqueid}|| "UNDEF");
		$self->skipRow($hash);
	}
}
1;

package ClinEpiData::Load::Prism2Reader::HouseholdReader;
use base qw(ClinEpiData::Load::Prism2Reader);

use strict;

sub cleanAndAddDerivedData {
  my ($self, $hash) = @_;
	if(defined($hash->{llin})){
		$hash->{household_llin} = $hash->{llin};
		delete($hash->{llin});
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

  return $hash->{hhid};
}


1;

package ClinEpiData::Load::Prism2Reader::ParticipantReader;
use base qw(ClinEpiData::Load::Prism2Reader);
use POSIX;

use strict;

sub makeParent {
  my ($self, $hash) = @_;
  if($hash->{parent}) {
    return $hash->{parent};
  }

  return $hash->{hhid};
}

sub makePrimaryKey {
  my ($self, $hash) = @_;

  if($hash->{primary_key}) {
    return $hash->{primary_key};
  }

  return $hash->{cohortid};
}


1;

package ClinEpiData::Load::Prism2Reader::ObservationReader;
use base qw(ClinEpiData::Load::Prism2Reader);

use strict;
sub cleanAndAddDerivedData {
  my ($self, $hash) = @_;
	$self->filterNaRows($hash);
}

sub makePrimaryKey {
  my ($self, $hash) = @_;
  if($hash->{primary_key}) {
    return $hash->{primary_key};
  }
  return $hash->{uniqueid};
}


sub makeParent {
  my ($self, $hash) = @_;
  if($hash->{parent}) {
    return $hash->{parent};
  }

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
  if($hash->{primary_key}) {
    return $hash->{primary_key};
  }
  #return join("_",$hash->{hhid}, $hash->{date}, $type{$hash->{trapmethod}},$hash->{roomnumm});
  return join("_",$hash->{hhid}, $hash->{date});
}


sub makeParent {
  my ($self, $hash) = @_;
  if($hash->{parent}) {
    return $hash->{parent};
  }
  return $hash->{hhid};
}

1;

package ClinEpiData::Load::Prism2Reader::SampleReader;
use base qw(ClinEpiData::Load::Prism2Reader);

use strict;

sub cleanAndAddDerivedData {
  my ($self, $hash) = @_;
	$self->filterNaRows($hash);
}

sub makePrimaryKey {
  my ($self, $hash) = @_;
  if($hash->{primary_key}) {
    return $hash->{primary_key};
  }
  return $hash->{uniqueid};
}

sub getPrimaryKeyPrefix {
	return "S";
}


sub makeParent {
  my ($self, $hash) = @_;
  if($hash->{parent}) {
    return $hash->{parent};
  }

  return $hash->{uniqueid};
}

1;

