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
 sub cleanAndAddDerivedData {
   my ($self, $hash) = @_;
   $self->SUPER::cleanAndAddDerivedData($hash);
  if(!defined($hash->{age_fu}) || $hash->{age_fu} eq "" ){
    if(defined($hash->{age_en}) && $hash->{age_en} ne ""){
      $hash->{age_fu} = $hash->{age_en};
    }
    else{
#     print "Get data for parent $hash->{sid}\n";
      my $parent = $self->getParentParsedOutput()->{$hash->{sid}};
      if(defined($parent->{age_en}) && $parent->{age_en} ne ""){
        $hash->{age_fu} = $parent->{age_en};
      }
    }
  }
}

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

1;
