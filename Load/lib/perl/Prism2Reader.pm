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

sub cleanAndAddDerivedData {
  my ($self, $hash) = @_;
  $hash->{country} ||= "Uganda";
  $hash->{household_llin} = $hash->{llin} ;
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
sub cleanAndAddDerivedData {
  my ($self, $hash) = @_;
  foreach my $col ( qw/reenrolldate dow lastdate cause3 death participantdie r2malaria withdrawnreason withdrawnreason2nd dow2nd/ ){
    next unless $hash->{$col} eq 'na';
    delete($hash->{$col});
  }
}


1;

package ClinEpiData::Load::Prism2Reader::ObservationReader;
use base qw(ClinEpiData::Load::Prism2Reader);

use strict;
# sub cleanAndAddDerivedData {
#   my ($self, $hash) = @_;
#   $self->filterNaRows($hash);
#   my %hack = (374621500 => 1, 374721500 => 1, 374921500 => 1);
#   if($hack{$hash->{uniqueid}}){
#     delete($hash->{totalanopheles});
#   }
# }

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
use Data::Dumper;

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
 #foreach my $var (qw/trapcollected trapset/){
 #  if(defined($hash->{$var})){
 #    my $hr = sprintf("%d", $hash->{$var} / 100);
 #    my $min = $hash->{$var} % 100;
 #    $min = $min / 100;
 #    $hash->{$var} = sprintf('%.03f',$hr + $min);
 #  }
 #}
}


sub makePrimaryKey {
  my ($self, $hash) = @_;
  if($hash->{primary_key}) {
    return $hash->{primary_key};
  }
  #return join("_",$hash->{hhid}, $hash->{date}, $type{$hash->{trapmethod}},$hash->{roomnumm});
  #return join("_",$hash->{hhid}, $hash->{date});
  return $hash->{trap};
}


sub makeParent {
  my ($self, $hash) = @_;
  if($hash->{parent}) {
    return $hash->{parent};
  }
  return substr($hash->{trap},0,9);
}

1;

package ClinEpiData::Load::Prism2Reader::SampleReader;
use base qw(ClinEpiData::Load::Prism2Reader);

use strict;

# sub cleanAndAddDerivedData {
#   my ($self, $hash) = @_;
#   $self->filterNaRows($hash);
# }

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

