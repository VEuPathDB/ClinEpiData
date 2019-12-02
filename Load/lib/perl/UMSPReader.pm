package ClinEpiData::Load::UMSPReader;
use base qw(ClinEpiData::Load::MetadataReader);

sub getId {
  my ($self, $hash) = @_;
  return undef unless defined($hash->{pid});
  return sprintf("%07d",$hash->{pid});
}

1;

package ClinEpiData::Load::UMSPReader::HouseholdReader;
use base qw(ClinEpiData::Load::UMSPReader);

sub makeParent {
  return undef; 
}
sub makePrimaryKey {
  my ($self, $hash) = @_;

  if($hash->{primary_key}) {
    return $hash->{primary_key};
  }
  #return $self->getId($hash);
  my $id = sprintf("%s%09d%09d",substr($hash->{site},0,4),$hash->{parish},$hash->{village});
#  print STDERR "$id\n";
  return $id;
}

1;
package ClinEpiData::Load::UMSPReader::ParticipantReader;
use base qw(ClinEpiData::Load::UMSPReader);

sub makeParent {
  my ($self, $hash) = @_;
  if($hash->{parent}) {
    return $hash->{parent};
  }
  my $id = sprintf("%s%09d%09d",substr($hash->{site},0,4),$hash->{parish},$hash->{village});
}

sub makePrimaryKey {
  my ($self, $hash) = @_;

  if($hash->{primary_key}) {
    return $hash->{primary_key};
  }

  return $self->getId($hash);
}
sub cleanAndAddDerivedData {
  my ($self, $hash) = @_;
}

1;

package ClinEpiData::Load::UMSPReader::ObservationReader;
use base qw(ClinEpiData::Load::UMSPReader);

sub makeParent {
  my ($self, $hash) = @_;

  if($hash->{primary_key}) {
    return $hash->{primary_key};
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
	return "o";
}

1;

package ClinEpiData::Load::UMSPReader::SampleReader;
use base qw(ClinEpiData::Load::UMSPReader);

sub makeParent {
  my ($self, $hash) = @_;
  if($hash->{parent}) {
    return $hash->{parent};
  }
  return $self->getId($hash);
}
sub getParentPrefix {
  my ($self, $hash) = @_;
  if($hash->{parent}) {
    return undef;
  }
	return "o";
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
