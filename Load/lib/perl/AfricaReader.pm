package ClinEpiData::Load::AfricaReader;
use base qw(ClinEpiData::Load::MetadataReader);

# sub cleanAndAddDerivedData {
#   my ($self, $hash) = @_;
# #	$self->fixDate($hash);
# }
# 
# # sub fixDate {
# #   my ($self, $hash) = @_;
# # 	if($hash->{today} =~ /^(\d\d)-(...)-(\d\d)$/){
# # 		$hash->{today} = join("", $1, $2, '20', $3);
# # 	}
# # }

1;

package ClinEpiData::Load::AfricaReader::HouseholdReader;
use base qw(ClinEpiData::Load::AfricaReader);


# sub cleanAndAddDerivedData {
#   my ($self, $hash) = @_;
# 	$self->SUPER::cleanAndAddDerivedData($hash);
# }

sub makeParent {
  return undef; 
}
sub makePrimaryKey {
  my ($self, $hash) = @_;

  if(defined($hash->{primary_key})) {
    return $hash->{primary_key};
  }
  return $hash->{hh_number};
}

sub getPrimaryKeyPrefix {
  my ($self, $hash) = @_;

  if(defined($hash->{primary_key})) {
    return ;
  }
  return "hh";
}

1;
package ClinEpiData::Load::AfricaReader::HouseholdObservationReader;
use base qw(ClinEpiData::Load::AfricaReader);
use Data::Dumper;

# sub cleanAndAddDerivedData {
#   my ($self, $hash) = @_;
# 	$self->SUPER::cleanAndAddDerivedData($hash);
# }

sub makeParent {
  my ($self, $hash) = @_;
  if(defined($hash->{parent})) {
    return $hash->{parent};
  }
  return $hash->{hh_number};
}
sub getParentPrefix {
  my ($self, $hash) = @_;

  if(defined($hash->{parent})) {
    return ;
  }
  return "hh";
}

sub makePrimaryKey {
  my ($self, $hash) = @_;

  if($hash->{primary_key}) {
    return $hash->{primary_key};
  }
  my $date = $self->formatDate($hash->{today}, "US");

  return join("_", $hash->{hh_number}, $date);
}

sub getPrimaryKeyPrefix {
  my ($self, $hash) = @_;

  if(defined($hash->{primary_key})) {
    return ;
  }
  return "ho";
}

1;

package ClinEpiData::Load::AfricaReader::ParticipantReader;
use base qw(ClinEpiData::Load::AfricaReader);

sub makeParent {
  my ($self, $hash) = @_;

  if($hash->{parent}) {
    return $hash->{parent};
  }

  return $hash->{hh_number};
}
sub getParentPrefix {
  my ($self, $hash) = @_;

  if(defined($hash->{parent})) {
    return ;
  }
  return "hh";
}

sub makePrimaryKey {
  my ($self, $hash) = @_;

  if($hash->{primary_key}) {
    return $hash->{primary_key};
  }

  return $hash->{part_id};
}

1;

package ClinEpiData::Load::AfricaReader::ObservationReader;
use base qw(ClinEpiData::Load::AfricaReader);

sub cleanAndAddDerivedData {
  my ($self, $hash) = @_;
	$self->SUPER::cleanAndAddDerivedData($hash);
	$hash->{today_observation} = $hash->{today};
	($hash->{today_month}) = split(/\//, $hash->{today});
	delete $hash->{today};
	if($hash->{age_months} > 0){
		$hash->{age_years} = sprintf("%0.2f", $hash->{age_years} + ($hash->{age_months} / 12.0));
	}
}

sub makeParent {
  ## returns a Participant ID
  my ($self, $hash) = @_;
  if($hash->{parent}) {
    return $hash->{parent};
  }
  return $hash->{part_id}; 
}

sub makePrimaryKey {
  my ($self, $hash) = @_;
  if($hash->{primary_key}) {
    return $hash->{primary_key};
  }
	#$self->fixDate($hash);
	#return join("_", $hash->{part_id}, $hash->{today}); 
	return $hash->{part_id}; 
}

sub getPrimaryKeyPrefix {
  my ($self, $hash) = @_;
  if($hash->{primary_key}) {
    return;
  }
  return "o";
}


1;

package ClinEpiData::Load::AfricaReader::SampleReader;
use base qw(ClinEpiData::Load::AfricaReader);

sub makeParent {
  ## returns a Participant ID
  my ($self, $hash) = @_;
  if(defined($hash->{parent})) {
    return $hash->{parent};
  }
	return $hash->{part_id}; 
  #return join("_", $hash->{part_id}, $hash->{today}); 
}

sub getParentPrefix {
  my ($self, $hash) = @_;
  if(defined($hash->{parent})) {
    return ;
  }
  return "o";
}

sub makePrimaryKey {
  my ($self, $hash) = @_;
  if($hash->{primary_key}) {
    return $hash->{primary_key};
  }
	return $hash->{part_id}; 
	#$self->fixDate($hash);
  #return join("_", $hash->{part_id}, $hash->{today}); 
}

sub getPrimaryKeyPrefix {
  my ($self, $hash) = @_;
  if(defined($hash->{primary_key})) {
    return ;
  }
  return "s";
}

1;
