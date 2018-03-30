package ClinEpiData::Load::IcemrIndiaReader;
use base qw(ClinEpiData::Load::MetadataReader);

sub cleanAndAddDerivedData {
  my ($self, $hash) = @_;
}
1;

package ClinEpiData::Load::IcemrIndiaReader::ParticipantReader;
use base qw(ClinEpiData::Load::IcemrIndiaReader);

sub makeParent {
  my ($self, $hash) = @_;
  if($hash->{parent}) {
    return $hash->{parent};
  }
  return $hash->{cen_fid};
}
sub makePrimaryKey {
  my ($self, $hash) = @_;
  my ($self, $hash) = @_;

  if($hash->{primary_key}) {
    return $hash->{primary_key};
  }

  return $hash->{sid};
}

1;

package ClinEpiData::Load::IcemrIndiaReader::HouseholdReader;
use base qw(ClinEpiData::Load::IcemrIndiaReader);
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
  return $hash->{cen_fid};
}

# sub cleanAndAddDerivedData {
#   my ($self, $hash) = @_;
#   $self->SUPER::cleanAndAddDerivedData($hash);
# }
1;

package ClinEpiData::Load::IcemrIndiaReader::ObservationReader;
use base qw(ClinEpiData::Load::IcemrIndiaReader);
## This object is for census data
use Data::Dumper;
## sub cleanAndAddDerivedData {
##   my ($self, $hash) = @_;
##   $self->SUPER::cleanAndAddDerivedData($hash);
##   if(!defined($hash->{cen_fid}) || $hash->{cen_fid} eq "" ){
##     my $parent = $self->getParentParsedOutput();
##     $hash->{cen_fid} = $parent->{parent};
##   }
##   if(!defined($hash->{cen_fid})){
##     return 'NONE';
##   }
## }

sub makeParent {
  ## returns a Participant ID
  my ($self, $hash) = @_;
  if($hash->{"parent"}) {
    return $hash->{"parent"};
  }
  return $hash->{sid};
}

sub makePrimaryKey {
  my ($self, $hash) = @_;
  if($hash->{"primary_key"}) {
    return $hash->{"primary_key"};
  }
  if(defined($hash->{sid}) && defined($hash->{redcap_event_name})){
    return join("_", $hash->{sid}, $hash->{redcap_event_name});
  }
  die "Cannot make primary key:\n" . Dumper($hash);
}

# sub cleanAndAddDerivedData {
#   my ($self, $hash) = @_;
#   $self->SUPER::cleanAndAddDerivedData($hash);
# }

1;
