package ClinEpiData::Load::ExampleReader;
use base qw(ClinEpiData::Load::MetadataReader);

1;

package ClinEpiData::Load::ExampleReader::HouseholdReader;
use base qw(ClinEpiData::Load::ExampleReader);

sub makeParent {
  return undef; 
}
sub makePrimaryKey {
  my ($self, $hash) = @_;

  if($hash->{primary_key}) {
    return $hash->{primary_key};
  }
	# $self->formatKeyVars($hash);
  return $hash->{HID};
}

sub cleanAndAddDerivedData {
  my ($self, $hash) = @_;
}

1;
package ClinEpiData::Load::ExampleReader::ParticipantReader;
use base qw(ClinEpiData::Load::ExampleReader);

sub makeParent {
  my ($self, $hash) = @_;
  if($hash->{parent}) {
    return $hash->{parent};
  }
  return $hash->{HID};
}

sub makePrimaryKey {
  my ($self, $hash) = @_;

  if($hash->{primary_key}) {
    return $hash->{primary_key};
  }

  return $hash->{PID};
}
sub cleanAndAddDerivedData {
  my ($self, $hash) = @_;
}

1;

package ClinEpiData::Load::ExampleReader::ObservationReader;
use base qw(ClinEpiData::Load::ExampleReader);

# sub skipIfNoParent {
# 	return 1;
# }

sub makeParent {
  my ($self, $hash) = @_;

  if($hash->{primary_key}) {
    return $hash->{primary_key};
  }
  return $hash->{PID};
}

sub makePrimaryKey {
  my ($self, $hash) = @_;

  if($hash->{primary_key}) {
    return $hash->{primary_key};
  }
	$hash->{DATE} = $self->formatDate($hash->{DATE}, "US");
  return join("_", $hash->{PID},$hash->{DATE});
}

sub cleanAndAddDerivedData {
  my ($self, $hash) = @_;
	$hash->{DATE} = $self->formatDate($hash->{DATE}, "US");
	$rowId++;
}
1;

package ClinEpiData::Load::ExampleReader::SampleReader;
use base qw(ClinEpiData::Load::ExampleReader);

my $rowId = 0;

sub makeParent {
  my ($self, $hash) = @_;
	return $self->makePrimaryKey($hash);
}

sub makePrimaryKey {
  my ($self, $hash) = @_;

  if($hash->{primary_key}) {
    return $hash->{primary_key};
  }
  return join("_", $hash->{PID},$hash->{DATE});
}

sub getPrimaryKeyPrefix {
  my ($self, $hash) = @_;
  if($hash->{primary_key}) {
    return undef;
  }
	return "s";
}

sub cleanAndAddDerivedData {
  my ($self, $hash) = @_;
	$hash->{DATE} = $self->formatDate($hash->{DATE}, "US");
	$rowId++;
}
1;
