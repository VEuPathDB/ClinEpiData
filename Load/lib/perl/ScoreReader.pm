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

#my %flag_treatment = (
#1 => "Tanzania: Between Years 1 and 2 and in Year 4, there was a longer interval between treatment and testing than required by the protocol.",
#2 => "Tanzania: Pregnant women were excluded from MDA in CWT villages. ",
#3 => "Tanzania: In Year 1, TAN040 (Ihale, Arm 3) did not enroll any 9-12 year old children with both lab and age data. ",
#4 => "Tanzania: In Year 2, parasitologic data for village TAN009 (Bukumbi, Arm 1) are unreliable and have been set to missing.",
#5 => "Tanzania: There are two study villages named Kahunda. Study village TAN060 was initially assigned to Arm 5 and study village TAN061 was initially assigned to Arm 2.  They switched arms throughout the study, and in the final dataset, TAN060 (Kahunda I) is in Arm 2 and TAN061 (Kahunda II) is in Arm 5.",
#6 => "Tanzania: In Year 4, Geita district villages TAN131 (Nyamboge, Arm 3), TAN063 (Kakubilo, Arm 5), TAN090 (Luhara, Arm 6), and TAN101 (Mharamba, Arm 6) received treatment while they should have been on holiday.",
#7 => "Kenya: Sm1 and Sm2 truncated egg counts at 42 eggs instead of 1,000.",
#8 => "Kenya: In Years 1-2 of the Kenya Sm2 study, MDA in CWT villages was not provided at schools, resulting in lower coverage among school-age children than required by the protocol.",
#9 => "Kenya: In Year 4 of the Kenya Sm1 study, 2 villages received double treatment. KEN039 (Kamingu, Arm 1) and KEN043 (Wambara, Arm 1) had SCORE SBT in March 2013 and albendazole and PZQ as part of the national deworming program in June 2013 at the school only.",
#10 => "Kenya: In Year 4 of the Kenya Sm1 study, village KEN040 (Kamwania, Arm 2) should have been on holiday. However, it had national program deworming with albendazole and PZQ in June 2013.",
#);

sub cleanAndAddDerivedData {
  my ($self, $hash) = @_;
  $self->SUPER::cleanAndAddDerivedData($hash);
# my @flags;
# foreach my $pdcode (split(/\s*,\s*/, $hash->{flag_treatment})){
#   push(@flags, $flag_treatment{int($pdcode)});
# }
# $hash->{flag_treatment} = join("|", @flags);
#***************** or do this: ****************
  #$hash->{flag_treatment} =~ s/\s*,\s*/|/g;
#***************** or do this: ****************
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
use File::Basename;


sub rowMultiplier {
  my ($self, $hash) = @_;
  my @multi;
  foreach my $specnum (qw/1 2 3/){
    foreach my $abslide (qw/a b/){
      my %clone = (
        'village_id' => $hash->{village_id},
        'person_id' => $hash->{person_id},
        'trunc' => $hash->{trunc},
        'specimen number' => $specnum,
        'a or b slide' => $abslide,
        'mean_epg' => $hash->{mean_epg},
        'sm_binary' => $hash->{sm_binary},
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
