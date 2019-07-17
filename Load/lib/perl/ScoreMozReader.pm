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
  return if(defined($hash->{primary_key})); 
  $self->SUPER::cleanAndAddDerivedData($hash);
}
1;

package ClinEpiData::Load::ScoreMozReader::SampleReader;
use base qw(ClinEpiData::Load::ScoreMozReader);
use Data::Dumper;
use strict;
use warnings;

sub rowMultiplier {
  my ($self, $hash) = @_;
  my $clone1 = { %$hash };
  $clone1->{filtration} = 'a';
  delete($clone1->{sh1b});
  delete($clone1->{sh1b_vol});
  my $clone2 = { %$hash };
  $clone2->{filtration} = 'b';
  delete($clone2->{sh1a});
  delete($clone2->{sh1a_vol});
  return [$clone1, $clone2];
}

sub makeParent {
  my ($self, $hash) = @_;

  if($hash->{primary_key}) {
    return $hash->{primary_key};
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
  if(keys %$hash){
    unless($hash->{filtration} eq "a" || $hash->{filtration} eq "b"){
      print Dumper $hash;
      exit;
    }
  }
  return sprintf("%s_%s%s", $hash->{village_id}, $hash->{person_id}, $hash->{filtration});
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
