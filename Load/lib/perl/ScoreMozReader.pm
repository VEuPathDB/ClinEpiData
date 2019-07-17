package ClinEpiData::Load::ScoreMozReader;
use base qw(ClinEpiData::Load::MetadataReader);

sub cleanAndAddDerivedData {
  my ($self, $hash) = @_;
  $hash->{$_} =~ s/^\s+$// for keys %$hash;
  if($hash->{village_id} eq $hash->{person_id}){
    $self->skipRow($hash);
  }
}

1;

package ClinEpiData::Load::ScoreMozReader::HouseholdReader;
use base qw(ClinEpiData::Load::ScoreMozReader);

sub makeParent {
  return undef; 
}
sub makePrimaryKey {
  my ($self, $hash) = @_;

  if($hash->{primary_key}) {
    return $hash->{primary_key};
  }
	# $self->formatKeyVars($hash);
  return join("_", $hash->{village_id},$hash->{person_id});
}

sub getPrimaryKeyPrefix {
  my ($self, $hash) = @_;
  if($hash->{primary_key}) {
    return undef;
  }
	return "hh";
}


1;
package ClinEpiData::Load::ScoreMozReader::ParticipantReader;
use base qw(ClinEpiData::Load::ScoreMozReader);

sub makeParent {
  my ($self, $hash) = @_;
  if($hash->{parent}) {
    return $hash->{parent};
  }
  return join("_", $hash->{village_id},$hash->{person_id});
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
sub cleanAndAddDerivedData {
  my ($self, $hash) = @_;
  $self->SUPER::cleanAndAddDerivedData($hash);
}

1;

package ClinEpiData::Load::ScoreMozReader::ObservationReader;
use base qw(ClinEpiData::Load::ScoreMozReader);

# sub skipIfNoParent {
# 	return 1;
# }

sub makeParent {
  my ($self, $hash) = @_;

  if($hash->{primary_key}) {
    return $hash->{primary_key};
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

sub cleanAndAddDerivedData {
  my ($self, $hash) = @_;
  $self->SUPER::cleanAndAddDerivedData($hash);
}
1;

package ClinEpiData::Load::ScoreMozReader::SampleReader;
use base qw(ClinEpiData::Load::ScoreMozReader);

sub makeParent {
  my ($self, $hash) = @_;
	return $self->makePrimaryKey($hash);
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
  my ($self, $hash) = @_;
  if($hash->{primary_key}) {
    return undef;
  }
	return "s";
}

# sub cleanAndAddDerivedData {
#   my ($self, $hash) = @_;
# }
1;
