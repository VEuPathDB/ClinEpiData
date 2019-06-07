package ClinEpiData::Load::SEAsiaReader;
use base qw(ClinEpiData::Load::MetadataReader);

sub getParentIfExists {
  my ($self, $hash) = @_;
	my $pid = $self->getParentKey();
	my $pre = $self->getParentPrefix();
	my $fullpid = join("", $pre, $pid);
	my $parentMerged = $self->getParentMergedData();
	if($parentMerged->{$fullpid}){
		return $pid;
	}
	else {
		printf STDERR ("%s\n", join("\t",$pid,"SKIPPED NO PARENT"));
		 return "";
	}
}

1;

package ClinEpiData::Load::SEAsiaReader::HouseholdReader;
use base qw(ClinEpiData::Load::SEAsiaReader);

sub makeParent {
  return undef; 
}
sub makePrimaryKey {
  my ($self, $hash) = @_;

  if($hash->{primary_key}) {
    return $hash->{primary_key};
  }
	# $self->formatKeyVars($hash);
  return $hash->{housecode};
}

sub cleanAndAddDerivedData {
  my ($self, $hash) = @_;
	# $self->formatKeyVars($hash);
	$hash->{hh_surveydate} = $hash->{surveydate};
}

1;
package ClinEpiData::Load::SEAsiaReader::ParticipantReader;
use base qw(ClinEpiData::Load::SEAsiaReader);

sub makeParent {
  my ($self, $hash) = @_;
  if($hash->{parent}) {
    return $hash->{parent};
  }
	# $self->formatKeyVars($hash);
	if($hash->{housecode}){
  	return $hash->{housecode};
	}
	elsif(substr($hash->{studycode},0,4) eq '6305'){
		return "na";
	} 
	else {
		my @pid = split(/-/, $hash->{studycode});
		pop(@pid);
		return join("-", @pid);
	}
}

sub makePrimaryKey {
  my ($self, $hash) = @_;

  if($hash->{primary_key}) {
    return $hash->{primary_key};
  }

  return $hash->{studycode};
}
sub cleanAndAddDerivedData {
  my ($self, $hash) = @_;
	$hash->{surveydate} = $self->formatDate($hash->{surveydate}, "US");
}

1;

package ClinEpiData::Load::SEAsiaReader::ObservationReader;
use base qw(ClinEpiData::Load::SEAsiaReader);

my $rowId = 0;

# sub skipIfNoParent {
# 	return 1;
# }

sub makeParent {
  my ($self, $hash) = @_;

  if($hash->{primary_key}) {
    return $hash->{primary_key};
  }
	$hash->{puncdate} = $self->formatDate($hash->{puncdate}, "US");
	if(substr($hash->{studycode},0,4) eq '6305'){
  	return sprintf("%s:%s", $hash->{studycode},$rowId);
	}
  return $hash->{studycode};
}

sub makePrimaryKey {
  my ($self, $hash) = @_;

  if($hash->{primary_key}) {
    return $hash->{primary_key};
  }
	# $self->formatKeyVars($hash);
	$hash->{puncdate} = $self->formatDate($hash->{puncdate}, "US");
	if(substr($hash->{studycode},0,4) eq '6305'){
  	return sprintf("%s:%s_%s", $hash->{studycode},$rowId,$hash->{puncdate});
	}
  return join("_", $hash->{studycode},$hash->{puncdate});
}

sub cleanAndAddDerivedData {
  my ($self, $hash) = @_;
	$hash->{puncdate} = $self->formatDate($hash->{puncdate}, "US");
	$rowId++;
}
1;

package ClinEpiData::Load::SEAsiaReader::SampleReader;
use base qw(ClinEpiData::Load::SEAsiaReader);

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
	$hash->{puncdate} = $self->formatDate($hash->{puncdate}, "US");
	if(substr($hash->{studycode},0,4) eq '6305'){
  	return sprintf("%s:%s_%s", $hash->{studycode},$rowId,$hash->{puncdate});
	}
  return join("_", $hash->{studycode},$hash->{puncdate});
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
	$hash->{puncdate} = $self->formatDate($hash->{puncdate}, "US");
	$rowId++;
}
1;
