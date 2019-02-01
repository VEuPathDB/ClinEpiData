package ClinEpiData::Load::ScoreReader;
use base qw(ClinEpiData::Load::MetadataReader);

#sub cleanAndAddDerivedData {
#  my ($self, $hash) = @_;
# $self->fixDate($hash);
#}
#
#sub fixDate {
#  my ($self, $hash) = @_;
# if($hash->{today} =~ /^(\d\d)-(...)-(\d\d)$/){
#   $hash->{today} = join("", $1, $2, '20', $3);
# }
#}

1;

package ClinEpiData::Load::ScoreReader::HouseholdReader;
use base qw(ClinEpiData::Load::ScoreReader);

# sub cleanAndAddDerivedData {
#   my ($self, $hash) = @_;
#   $self->SUPER::cleanAndAddDerivedData($hash);
# }

sub makeParent {
  return undef; 
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

1;

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
use File::Basename;


sub rowMultiplier {
  my ($self, $hash) = @_;
  my @multi;
  foreach my $specnum (qw/1 2 3/){
    foreach my $abslide (qw/a b/){
      my %clone = ( %$hash  );
      $clone{'specimen number'} = $specnum;
      $clone{'a or b slide'} = $abslide;
      push(@multi, \%clone);
    }
  }
  return \@multi;
}

sub cleanAndAddDerivedData {
  my ($self, $hash) = @_;
  my $sample = join("", $hash->{'specimen number'}, $hash->{'a or b slide'});
  for my $specnum (qw/1 2 3/){
    for my $abslide (qw/a b/){
      my $fieldset = join("",$specnum, $abslide);
      unless($sample eq $fieldset){
        for my $col ( qw/sm hook asc trich/ ){
          delete $hash->{ sprintf("%s%s", $col, $sample) };
          delete $hash->{ sprintf("%s%s_count", $col, $sample) };
        }
      }
    }
  }
}

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
  return join("_", $hash->{village_id},$hash->{person_id}, $hash->{'specimen number'}, $hash->{'a or b slide'}); 
}

1;
