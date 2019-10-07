package ClinEpiData::Load::ScoreSeasonalReader;
use base qw(ClinEpiData::Load::MetadataReader);

sub cleanAndAddDerivedData {
  my ($self, $hash) = @_;
  return if(defined($hash->{primary_key})); 
  $hash->{$_} =~ s/^\s+$// for keys %$hash;
  $hash->{id} =~ s/\s+/_/g;
}

sub getId {
  my ($self, $hash) = @_;
  $hash->{id} =~ s/\s+/_/g;
  return $hash->{id};
# my $id = $hash->{id};
# if($id =~ /\s+/){
#   my ($prefix,$num) = split(/\s+/,$id);
#   $id=$num;
# }
# return $id;
}


1;

package ClinEpiData::Load::ScoreSeasonalReader::HouseholdReader;
use base qw(ClinEpiData::Load::ScoreSeasonalReader);

sub makeParent {
  return undef; 
}
sub makePrimaryKey {
  my ($self, $hash) = @_;

  if($hash->{primary_key}) {
    return $hash->{primary_key};
  }
  return $hash->{village_id};
}

sub getPrimaryKeyPrefix {
  my ($self, $hash) = @_;
  if($hash->{primary_key}) {
    return undef;
  }
	return "hh";
}


1;


package ClinEpiData::Load::ScoreSeasonalReader::ParticipantReader;
use base qw(ClinEpiData::Load::ScoreSeasonalReader);

sub makeParent {
  my ($self, $hash) = @_;
  if($hash->{parent}) {
    return $hash->{parent};
  }
  #return join("_", $hash->{village_id},$hash->{person_id});
  return $hash->{village_id};
}

sub getParentPrefix {
  my ($self, $hash) = @_;
  if($hash->{parent}) {
    return "";
  }
  return "hh";
}

sub makePrimaryKey {
  my ($self, $hash) = @_;
  if($hash->{primary_key}) {
    return $hash->{primary_key};
  }
  return $self->getId($hash);
}

1;

##HOUSEHOLDOBSERVATIONREADER
package ClinEpiData::Load::ScoreSeasonalReader::HouseholdObservationReader;
use base qw(ClinEpiData::Load::ScoreSeasonalReader);

sub makeParent {
  my ($self, $hash) = @_;
  if($hash->{parent}) {
    return $hash->{parent};
  }
  #return join("_", $hash->{village_id},$hash->{person_id});
  return $hash->{village_id};
}

sub getParentPrefix {
  my ($self, $hash) = @_;
  if($hash->{parent}) {
    return "";
  }
  return "hh";
}

sub makePrimaryKey {
  my ($self, $hash) = @_;
  if($hash->{primary_key}) {
    return $hash->{primary_key};
  }
  return join("_", $hash->{village_id},$hash->{study_year});
}
sub cleanAndAddDerivedData {
  my ($self, $hash) = @_;
  return if(defined($hash->{primary_key})); 
  $self->SUPER::cleanAndAddDerivedData($hash);
  $hash->{village_study_year} = $hash->{study_year};
}

1;
##END HOUSEHOLDOBSERVATIONREADER
#
##OBSERVATIONREADER
package ClinEpiData::Load::ScoreSeasonalReader::ObservationReader;
use base qw(ClinEpiData::Load::ScoreSeasonalReader);

# sub skipIfNoParent {
# 	return 1;
# }

sub makeParent {
  my ($self, $hash) = @_;

  if($hash->{parent}) {
    return $hash->{parent};
  }
  return $self->getId($hash);
}

sub makePrimaryKey {
  my ($self, $hash) = @_;

  if($hash->{primary_key}) {
    return $hash->{primary_key};
  }
  return $self->getId($hash);
}
sub getPrimaryKeyPrefix {
  my ($self, $hash) = @_;
  if($hash->{primary_key}) {
    return undef;
  }
  return "ob";
}

1;

package ClinEpiData::Load::ScoreSeasonalReader::SampleReader;
use base qw(ClinEpiData::Load::ScoreSeasonalReader);
use Data::Dumper;
use strict;
use warnings;

sub makeParent {
  my ($self, $hash) = @_;

  if($hash->{primary_key}) {
    return $hash->{primary_key};
  }
  return $self->getId($hash);
}

sub getParentPrefix {
  my ($self, $hash) = @_;
  if($hash->{parent}) {
    return "";
  }
  return "ob";
}

sub makePrimaryKey {
  my ($self, $hash) = @_;
  if($hash->{primary_key}) {
    return $hash->{primary_key};
  }
  return $self->getId($hash);
}

sub getPrimaryKeyPrefix {
  my ($self, $hash) = @_;
  if($hash->{primary_key}) {
    return undef;
  }
	return "s";
}

1;
