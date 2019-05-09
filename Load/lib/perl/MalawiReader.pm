package ClinEpiData::Load::MalawiReader;
use base qw(ClinEpiData::Load::MetadataReader);

# sub cleanAndAddDerivedData {
#   my ($self, $hash) = @_;
# 	foreach my $col (keys %$hash){
# 		if($hash->{$col} eq "."){
# 			delete($hash->{$col});
# 		}
# 	}
# }

1;

package ClinEpiData::Load::MalawiReader::HouseholdReader;
use base qw(ClinEpiData::Load::MalawiReader);

sub makeParent {
  return undef; 
}
sub makePrimaryKey {
  my ($self, $hash) = @_;
  if($hash->{primary_key}) {
    return $hash->{primary_key};
  }
  #return $hash->{study_id};
  return $hash->{study_id};
}

sub getPrimaryKeyPrefix {
  my ($self, $hash) = @_;
  if($hash->{primary_key}) {
    return "";
  }
  return "hh";
}

1;

package ClinEpiData::Load::MalawiReader::EntoReader;
use base qw(ClinEpiData::Load::MalawiReader);

sub makeParent {
  my ($self, $hash) = @_;
  if($hash->{parent}) {
    return $hash->{parent};
  }
  return $hash->{study_id};
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
  #return join("_",$hash->{study_id},$hash->{mossitrapid});
  return $hash->{study_id};
}

sub getPrimaryKeyPrefix {
  my ($self, $hash) = @_;
  if($hash->{primary_key}) {
    return "";
  }
  return "en";
}

1;
package ClinEpiData::Load::MalawiReader::ParticipantReader;
use base qw(ClinEpiData::Load::MalawiReader);

sub makeParent {
  my ($self, $hash) = @_;
  if($hash->{parent}) {
    return $hash->{parent};
  }
  return $hash->{study_id};
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
  return $hash->{study_id};
}

1;

package ClinEpiData::Load::MalawiReader::ObservationReader;
use base qw(ClinEpiData::Load::MalawiReader);


sub makeParent {
  ## returns a Participant ID
  my ($self, $hash) = @_;
  if($hash->{parent}) {
    return $hash->{parent};
  }
  return $hash->{study_id};
}

sub makePrimaryKey {
  my ($self, $hash) = @_;
  if($hash->{primary_key}) {
    return $hash->{primary_key};
  }
	my $date;
	
  return $hash->{study_id};
}

sub getPrimaryKeyPrefix {
  my ($self, $hash) = @_;
  if($hash->{primary_key}) {
    return "";
  }
  return "ob";
}

1;

package ClinEpiData::Load::MalawiReader::SampleReader;
use base qw(ClinEpiData::Load::MalawiReader);
use strict;

sub makeParent {
  my ($self, $hash) = @_;
  if(defined($hash->{parent})) {
    return $hash->{parent};
  }
  return $hash->{study_id};
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
  return $hash->{study_id};
}

sub getPrimaryKeyPrefix {
  my ($self, $hash) = @_;
  if($hash->{primary_key}) {
    return "";
  }
  return "s";
}

1;
