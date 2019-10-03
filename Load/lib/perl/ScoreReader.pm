package ClinEpiData::Load::ScoreReader;
use base qw(ClinEpiData::Load::MetadataReader);

sub cleanAndAddDerivedData {
  my ($self, $hash) = @_;
# $self->fixDate($hash);
#}
#
#sub fixDate {
#  my ($self, $hash) = @_;
# if($hash->{today} =~ /^(\d\d)-(...)-(\d\d)$/){
#   $hash->{today} = join("", $1, $2, '20', $3);
# }
  $hash->{$_} =~ s/^\s+$// for keys %$hash;
}

1;

package ClinEpiData::Load::ScoreReader::HouseholdReader;
use base qw(ClinEpiData::Load::ScoreReader);

sub cleanAndAddDerivedData {
  my ($self, $hash) = @_;
  $self->SUPER::cleanAndAddDerivedData($hash);
#  foreach my $pdcode (split(/\s*,\s*/, $hash->{flag_treatment})){
#    $hash->{ "flag_treatment_$pdcode" } = int($pdcode);
#  }
#  delete($hash->{flag_treatment});
  $hash->{village_name} =~ s/\s\s+/\s/g;
  $hash->{village_name} =~ s/^\s+|\s+$//g;
}

sub makeParent {
  return undef; 
}
sub makePrimaryKey {
  my ($self, $hash) = @_;
  if($hash->{primary_key}) {
    return $hash->{primary_key};
  }
  #return join("_", $hash->{village_id},$hash->{person_id});
  # return join("_", $hash->{village_id},$hash->{study_year});
  return $hash->{village_id};
}

sub getPrimaryKeyPrefix {
  my ($self, $hash) = @_;
  if($hash->{primary_key}) {
    return "";
  }
  return "hh";
}

1;


package ClinEpiData::Load::ScoreReader::ParticipantReader;
use base qw(ClinEpiData::Load::ScoreReader);

sub makeParent {
  my ($self, $hash) = @_;
  if($hash->{parent}) {
    return $hash->{parent};
  }
  #return join("_", $hash->{village_id},$hash->{person_id});
  #return join("_", $hash->{village_id},$hash->{study_year});
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
  return join("_", $hash->{village_id},$hash->{person_id});
}

1;

package ClinEpiData::Load::ScoreReader::HouseholdObservationReader;
use base qw(ClinEpiData::Load::ScoreReader::ParticipantReader);

sub cleanAndAddDerivedData {
  my ($self, $hash) = @_;
  $self->SUPER::cleanAndAddDerivedData($hash);
  $hash->{village_study_year} = $hash->{study_year};
  delete $hash->{study_year};
}

#sub rowMultiplier {
#  my ($self, $hash) = @_;
#  my @clones;
#  foreach my $pdcode (split(/\s*,\s*/, $hash->{flag_treatment})){
#    push(@clones, {
#      village_id => $hash->{village_id},
#      person_id => $hash->{person_id},
#      flag_treatment => int($pdcode)
#    });
#  }
#  $self->skipRow($hash);
#  return \@clones;
#}

sub makePrimaryKey {
  my ($self, $hash) = @_;
  if($hash->{primary_key}) {
    return $hash->{primary_key};
  }
  #return join("_", $hash->{village_id},$hash->{person_id}, $hash->{flag_treatment});
  return join("_", $hash->{village_id}, $hash->{study_year});
}

1;

##END HOUSEHOLDOBSERVATIONREADER
#
##OBSERVATIONREADER
package ClinEpiData::Load::ScoreReader::ObservationReader;
use base qw(ClinEpiData::Load::ScoreReader);

sub makeParent {
  ## returns a Participant ID
  my ($self, $hash) = @_;
  if($hash->{parent}) {
    return $hash->{parent};
  }
  return join("_", $hash->{village_id},$hash->{person_id});
}

sub makePrimaryKey {
  my ($self, $hash) = @_;
  if($hash->{primary_key}) {
    return $hash->{primary_key};
  }
  return join("_", $hash->{village_id},$hash->{person_id});
}

sub getPrimaryKeyPrefix {
  my ($self, $hash) = @_;
  if($hash->{primary_key}) {
    return "";
  }
  return "ob";
}

1;
package ClinEpiData::Load::ScoreReader::SampleReader;
use base qw(ClinEpiData::Load::ScoreReader);
use strict;
use warnings;

sub makeParent {
  ## returns a Participant ID
  my ($self, $hash) = @_;
  if(defined($hash->{parent})) {
    return $hash->{parent};
  }
  return join("_", $hash->{village_id},$hash->{person_id});
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
  return join("_", $hash->{village_id},$hash->{person_id}); 
}

sub getPrimaryKeyPrefix {
  return "s";
}
1;

package ClinEpiData::Load::ScoreReader::AssayReader;
use base qw(ClinEpiData::Load::ScoreReader);
use strict;
use warnings;

sub makeParent {
  ## returns a Participant ID
  my ($self, $hash) = @_;
  if(defined($hash->{parent})) {
    return $hash->{parent};
  }
  return join("_", $hash->{village_id},$hash->{person_id});
}

sub getParentPrefix {
  my ($self, $hash) = @_;
  if($hash->{parent}) {
    return "";
  }
#  return "ob";
  return "s";
}

sub makePrimaryKey {
  my ($self, $hash) = @_;
  if($hash->{primary_key}) {
    return $hash->{primary_key};
  }
  #return join("_", $hash->{village_id},$hash->{person_id}, $hash->{'specimen number'} || '0', $hash->{'a or b slide'} || '0'); 
  return join("_", $hash->{village_id},$hash->{person_id}); 
}

sub getPrimaryKeyPrefix {
  return "ay"
}

1;
