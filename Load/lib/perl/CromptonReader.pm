package ClinEpiData::Load::CromptonReader;
use base qw(ClinEpiData::Load::MetadataReader);

use Data::Dumper;

sub getId {
  my ($self, $hash) = @_;
  # printf STDERR ("%s\tsubj_id empty\n", $self->getMetadataFile()) unless $hash->{subj_id};
  return sprintf("%03d",$hash->{subj_id});
}

sub cleanAndAddDerivedData {
  my ($self, $hash) = @_;
  return if defined $hash->{primary_key};
  # if($hash->{subj_id} eq "0"){ print STDERR ("%s has subj_id %s\n", $self->getMetadataFile(), $hash->{subj_id})  }
  unless($hash->{subj_id}){ $self->skipRow($hash); }
  # if($hash->{dob} && $hash->{dob} eq '.u'){ printf STDERR ("%s has garbage dob\n", $self->getMetadataFile()) }
}

1;

package ClinEpiData::Load::CromptonReader::HouseholdReader;
use base qw(ClinEpiData::Load::CromptonReader);
use Data::Dumper;

sub makeParent {
  return undef; 
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
	return "h";
}

1;

package ClinEpiData::Load::CromptonReader::ParticipantReader;
use base qw(ClinEpiData::Load::CromptonReader);

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
	return "h";
}

sub makePrimaryKey {
  my ($self, $hash) = @_;

  if($hash->{primary_key}) {
    return $hash->{primary_key};
  }

  return $self->getId($hash);
}

1;

package ClinEpiData::Load::CromptonReader::HouseholdObservationReader;
use base qw(ClinEpiData::Load::CromptonReader::ParticipantReader);

sub makePrimaryKey {
  my ($self, $hash) = @_;

  if($hash->{primary_key}) {
    return $hash->{primary_key};
  }
  my $visit = $hash->{visnum} || $hash->{dfseq} || "0";
  return sprintf("%s_%04d", $self->getId($hash), $visit);
}
sub getPrimaryKeyPrefix {
  my ($self, $hash) = @_;
  if($hash->{primary_key}) {
    return undef;
  }
	return "h";
}

1;


package ClinEpiData::Load::CromptonReader::ObservationReader;
use base qw(ClinEpiData::Load::CromptonReader);
use strict;
use warnings;

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
  my $visit = $hash->{visnum} || $hash->{dfseq} || "0";
  return sprintf("%s_%04d", $self->getId($hash), $visit);
}

sub getPrimaryKeyPrefix {
  my ($self, $hash) = @_;
  if($hash->{primary_key}) {
    return undef;
  }
	return "o";
}

1;

package ClinEpiData::Load::CromptonReader::SampleReader;
use base qw(ClinEpiData::Load::CromptonReader);

sub makeParent {
  my ($self, $hash) = @_;
  if($hash->{parent}) {
    return $hash->{parent};
  }
  my $visit = $hash->{visnum} || $hash->{dfseq} || "0";
  return sprintf("%s_%04d", $self->getId($hash), $visit);
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
  my $visit = $hash->{visnum} || $hash->{dfseq} || "0";
  return sprintf("%s_%04d", $self->getId($hash), $visit);
}

sub getPrimaryKeyPrefix {
  my ($self, $hash) = @_;
  if($hash->{primary_key}) {
    return undef;
  }
	return "s";
}
1;
