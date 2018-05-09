package ClinEpiData::Load::IcemrIndiaReader;
use base qw(ClinEpiData::Load::MetadataReader);

sub cleanAndAddDerivedData {
  my ($self, $hash) = @_;
}

1;

package ClinEpiData::Load::IcemrIndiaReader::HouseholdReader;
use base qw(ClinEpiData::Load::IcemrIndiaReader);
# use Data::Dumper;
## loads file micro_x24m.csv
use strict;
use warnings;

sub cleanAndAddDerivedData {
  my ($self, $hash) = @_;
  $self->SUPER::cleanAndAddDerivedData($hash);
	my $file = $self->getMetadataFile();
	if(defined($hash->{redcap_event_name}) && $hash->{redcap_event_name} =~ /^houseinfo/){
		$hash = {};
	}
}

sub makeParent {
  my ($self, $hash) = @_;
  return undef;
}

sub makePrimaryKey {
  my ($self, $hash) = @_;
  if($hash->{"primary_key"}) {
    return $hash->{"primary_key"};
  }
  return $hash->{cen_fid};
}

# sub cleanAndAddDerivedData {
#   my ($self, $hash) = @_;
#   $self->SUPER::cleanAndAddDerivedData($hash);
# }
1;

package ClinEpiData::Load::IcemrIndiaReader::ParticipantReader;
use base qw(ClinEpiData::Load::IcemrIndiaReader);
sub cleanAndAddDerivedData {
  my ($self, $hash) = @_;
  $self->SUPER::cleanAndAddDerivedData($hash);
	my $file = $self->getMetadataFile();
	$hash->{study_design} = "Cross-sectional";
	if($file =~ /longitud/i){
		$hash->{study_design} = "Longitudinal";
	}
	$hash->{state_birth} =~ s/^-$/Missing/;
}

sub makeParent {
  my ($self, $hash) = @_;
  if($hash->{parent}) {
    return $hash->{parent};
  }
  return $hash->{cen_fid};
}
sub makePrimaryKey {
  my ($self, $hash) = @_;

  if($hash->{primary_key}) {
    return $hash->{primary_key};
  }

  return $hash->{sid};
}

1;

package ClinEpiData::Load::IcemrIndiaReader::ObservationReader;
use base qw(ClinEpiData::Load::IcemrIndiaReader);
## This object is for census data
use Data::Dumper;
use Scalar::Util qw/looks_like_number/;
sub cleanAndAddDerivedData {
  my ($self, $hash) = @_;
  $self->SUPER::cleanAndAddDerivedData($hash);
  ## get a value for age at time of visit
  if(!defined($hash->{age_fu}) || $hash->{age_fu} eq "" ){
    if(defined($hash->{age_en}) && $hash->{age_en} ne ""){
      $hash->{age_fu} = $hash->{age_en};
    }
    elsif(my $parentData = $self->getParentParsedOutput()){
    # print "Get data for parent $hash->{sid}\n";
      my $parent = $parentData->{$hash->{sid}};
      if(defined($parent->{age_en}) && $parent->{age_en} ne ""){
        $hash->{age_fu} = $parent->{age_en};
      }
    }
    else{
      $hash->{age_fu} = -1;
      ## set an impossible value
    }
  }
	$hash->{travel_2wk_district} =~ s/^-$/Missing/;
	$hash->{travel_2wk_state} =~ s/^-$/Missing/;

  if(1){
    my $score = 0;
    my @vars = grep { /^fever_2wk_where_chk___/ } keys %$hash;
    if($hash->{fever_2wk_yn} eq '1'){
    # fever_2wk_where_chk__* 
    # if all are '0', change all 4 to "N/A"
    # may have to add rows to valueMap.txt
      map { $score += $hash->{$_} } @vars;
      unless( $score ){
        delete($hash->{$_}) for @vars;
      }
    }
    else {
    # do not load any of these
      delete($hash->{$_}) for @vars;
    }
  }
# pastyear_treatdrugs_chk__* 
# if all are '0', change all 4 to "N/A"
# may have to add rows to valueMap.txt
  if($hash->{pastyear_treat_rad} eq '1' || $hash->{pastyear_treat_rad} eq '3'){
    setIfZero($hash, '^pastyear_treatdrugs_chk___\d+$', 'NULL');
    setIfZero($hash, '^pastyear_treatwhere_chk___\d+$', 'NULL');
  }
  else {
    my @vars = grep { /^pastyear_treatdrugs_chk___/ } keys %$hash;
    delete($hash->{$_}) for @vars;
    @vars = grep { /^pastyear_treatwhere_chk___/ } keys %$hash;
    delete($hash->{$_}) for @vars;
  }

# sprayed_chk___1-4
  setIfZero($hash, '^sprayed_chk___\d$', 'NULL');
# repellent_chk___1-7
  setIfZero($hash, '^repellent_chk___\d$', 'NULL');
# bednet_chk___1-7
  setIfZero($hash, '^bednet_chk___\d$', 'NULL');
# pastyear_dx_chk___1-4
  setIfZero($hash, '^pastyear_dx_chk___\d$', 'NULL');
# pcr_species_chk___1-5
  setIfZero($hash, '^pcr_species_chk___\d$', 'NULL');
# rdt_op_chk___1-4
  if(defined($hash->{rdt_op_chk___4})){
    unless(setIfZero($hash, '^rdt_op_chk___\d$')){
      $hash->{rdt_op_chk___4} = 1;
    }
  }
    
# rdt_fv_chk___1-4
  if(defined($hash->{rdt_fv_chk___4})){
    unless(setIfZero($hash, '^rdt_fv_chk___\d$')){
      $hash->{rdt_fv_chk___4} = 1;
    }
  }
# mx_species_chk___1-5
  unless(setIfZero($hash, '^mx_species_chk___\d$')){
    $hash->{mx_species_chk___5} = 1;
  }


  foreach my $var (qw/height weight temp_celsius hemocue/){
    delete($hash->{$var}) unless(looks_like_number($hash->{$var}));
  }
  if(defined($hash->{pastyear_treatdate})){
    my $val = $hash->{pastyear_treatdate};
    $val =~ tr/_/ /; ## some dates look like mmm_YYYY
    $val =~ s/^(\d\d)-([a-zA-Z]{3})$/$2 20$1/; ## some dates look like YY-mmm
    $val =~ s/^([a-zA-Z]{3})-(\d\d)$/$1 20$2/; ## some dates look like mmm-YY
    $val =~ s/^\W+(\d+)\W+$/1-1-$1/; ## just a year
    $val =~ s/^(\d{1,2})-(\d+)$/1-$1-$2/; ## mm-yyyy
#		$val =~ s/^(\d+)\W(\d+)(\W)(\d+)$/$2$3$4/; ## strip day of month !!! ASSUMES D-M-Y
    $hash->{pastyear_treatdate} = $val;
  }
	if(defined($hash->{bp})){
		if($hash->{bp} !~ /^\d+\/\d+$/){
			delete($hash->{bp});
		}
		else{
			($hash->{systolic_bp},$hash->{diastolic_bp}) = split(/\//, $hash->{bp});
		}
	}
}

## if all hash vars matching pattern are zero, delete them, or set to $value if provided
sub setIfZero {
  my ($hash, $pattern, $value) =  @_;
  my $score = 0;
  my @vars = grep { /$pattern/ } keys %$hash;
  foreach my $var (@vars){
    unless(looks_like_number($hash->{$var})){
      die "ERROR value in '$var' is not a number: $hash->{$var}\n";
    }
    $score += $hash->{$var};
  }
  unless(defined($value)){
    return $score;
  }
  unless( $score ){
    if($value eq 'NULL'){
      delete($hash->{$_}) for @vars;
    }
    else {
      $hash->{$_} = $value for @vars;
    }
  # printf STDERR ("All set to %s: %s\n", $value, join(",", @vars));
  }
  return 1;
}



sub makeParent {
  ## returns a Participant ID
  my ($self, $hash) = @_;
  if($hash->{"parent"}) {
    return $hash->{"parent"};
  }
  return $hash->{sid};
}

sub makePrimaryKey {
  my ($self, $hash) = @_;
  if($hash->{"primary_key"}) {
    return $hash->{"primary_key"};
  }
  if(defined($hash->{sid}) && defined($hash->{redcap_event_name})){
    return join("_", $hash->{sid}, $hash->{redcap_event_name});
  }
  die "Cannot make primary key:\n" . Dumper($hash);
}

1;
