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
  foreach my $pdcode (split(/\s*,\s*/, $hash->{flag_treatment})){
    $hash->{ "flag_treatment_$pdcode" } = int($pdcode);
  }
  delete($hash->{flag_treatment});
}

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

package ClinEpiData::Load::ScoreReader::HouseholdObservationReader;
use base qw(ClinEpiData::Load::ScoreReader::ParticipantReader);

sub rowMultiplier {
  my ($self, $hash) = @_;
  my @clones;
  foreach my $pdcode (split(/\s*,\s*/, $hash->{flag_treatment})){
    push(@clones, {
      village_id => $hash->{village_id},
      person_id => $hash->{person_id},
      flag_treatment => int($pdcode)
    });
  }
  $self->skipRow($hash);
  return \@clones;
}

sub makePrimaryKey {
  my ($self, $hash) = @_;
  if($hash->{primary_key}) {
    return $hash->{primary_key};
  }
  return join("_", $hash->{village_id},$hash->{person_id}, $hash->{flag_treatment});
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
use warnings;

sub rowMultiplier {
  my ($self, $hash) = @_;
  my @multi = ({
        'village_id' => $hash->{village_id},
        'person_id' => $hash->{person_id},
        'trunc' => $hash->{trunc},
        'mean_epg' => $hash->{mean_epg},
        'sm_binary' => $hash->{sm_binary},
  });
  foreach my $specnum (qw/1 2 3/){
    foreach my $abslide (qw/a b/){
      my %clone = (
        'village_id' => $hash->{village_id},
        'person_id' => $hash->{person_id},
        'trunc' => $hash->{trunc},
        'specimen number' => $specnum,
        'a or b slide' => $abslide,
      );
      for my $col ( qw/sm hook asc trich/ ){
        my $assay = sprintf("%s%d%s", $col, $specnum, $abslide);
        next unless defined($hash->{$assay});
        $clone{"${col}_sample"} = $hash->{$assay};
        if($col eq "sm"){
          $clone{"${col}_epg"} = $hash->{"${assay}_epg"};
        }
        else{
          $clone{"${col}_count"} = $hash->{"${assay}_count"}
        }
        ## Merge will trigger warnings if there are multiple values
      }
      push(@multi, \%clone);
    }
  }
  return \@multi;
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
  return join("_", $hash->{village_id},$hash->{person_id}, $hash->{'specimen number'} || '0', $hash->{'a or b slide'} || '0'); 
}
sub getPrimaryKeyPrefix {
  return "s";
}
1;

package ClinEpiData::Load::ScoreReader::AssayReader;
use base qw(ClinEpiData::Load::ScoreReader);
use strict;
use warnings;

sub rowMultiplier {
  my ($self, $hash) = @_;
  my @multi ;
# = ({
#        'village_id' => $hash->{village_id},
#        'person_id' => $hash->{person_id},
#        'trunc' => $hash->{trunc},
#        'mean_epg' => $hash->{mean_epg},
#        'sm_binary' => $hash->{sm_binary},
#  });
  foreach my $specnum (qw/1 2 3/){
    foreach my $abslide (qw/a b/){
      my %clone = (
        'village_id' => $hash->{village_id},
        'person_id' => $hash->{person_id},
        'trunc' => $hash->{trunc},
        'specimen number' => $specnum,
        'a or b slide' => $abslide,
      );
      for my $col ( qw/sm hook asc trich/ ){
        my $assay = sprintf("%s%d%s", $col, $specnum, $abslide);
        next unless defined($hash->{$assay});
        $clone{"${col}_sample"} = $hash->{$assay};
        if($col eq "sm"){
          $clone{"${col}_epg"} = $hash->{"${assay}_epg"};
        }
        else{
          $clone{"${col}_count"} = $hash->{"${assay}_count"}
        }
        ## Merge will trigger warnings if there are multiple values
      }
      push(@multi, \%clone);
    }
  }
  $self->skipRow($hash);
  return \@multi;
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
#  return "ob";
  return "s";
}

sub makePrimaryKey {
  my ($self, $hash) = @_;
  if($hash->{primary_key}) {
    return $hash->{primary_key};
  }
  return join("_", $hash->{village_id},$hash->{person_id}, $hash->{'specimen number'} || '0', $hash->{'a or b slide'} || '0'); 
}

1;
