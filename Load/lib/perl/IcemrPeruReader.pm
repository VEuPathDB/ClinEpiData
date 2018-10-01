package ClinEpiData::Load::IcemrPeruReader;
use base qw(ClinEpiData::Load::MetadataReader);

1;

package ClinEpiData::Load::IcemrPeruReader::HouseholdReader;
use base qw(ClinEpiData::Load::IcemrPeruReader);

sub makeParent {
  return undef; 
}
sub makePrimaryKey {
  my ($self, $hash) = @_;

  if($hash->{primary_key}) {
    return $hash->{primary_key};
  }

  return $hash->{cod_casa};
}

sub cleanAndAddDerivedData {
  my ($self, $hash) = @_;
	delete $hash->{fumi} if $hash->{fumi} eq 'na';
}

1;
package ClinEpiData::Load::IcemrPeruReader::ParticipantReader;
use base qw(ClinEpiData::Load::IcemrPeruReader);

sub makeParent {
  my ($self, $hash) = @_;
  if($hash->{parent}) {
    return $hash->{parent};
  }

  return $hash->{cod_casa};
}

sub makePrimaryKey {
  my ($self, $hash) = @_;

  if($hash->{primary_key}) {
    return $hash->{primary_key};
  }

  return $hash->{cod_per};
}
sub cleanAndAddDerivedData {
  my ($self, $hash) = @_;
	delete $hash->{fec_naci} if($hash->{fec_naci} eq 'na');
	delete $hash->{tieanop} if(($hash->{tieanop} eq 'not applicable') || ($hash->{tieanop} eq "don't know"));
	delete $hash->{tieanol} if(($hash->{tieanol} eq 'not applicable') || ($hash->{tieanop} eq "don't know"));
}

1;

package ClinEpiData::Load::IcemrPeruReader::ObservationReader;
use base qw(ClinEpiData::Load::IcemrPeruReader);

sub makeParent {
  ## returns a Participant ID
  my ($self, $hash) = @_;
  if($hash->{parent}) {
    return $hash->{parent};
  }
  return $hash->{cod_per};
}

sub makePrimaryKey {
  my ($self, $hash) = @_;
  if($hash->{primary_key}) {
    return $hash->{primary_key};
  }
	
	return sprintf("%s_%02d%02d%02d", $hash->{cod_uni}, $hash->{aa}, $hash->{mm}, $hash->{dd});
}

1;
