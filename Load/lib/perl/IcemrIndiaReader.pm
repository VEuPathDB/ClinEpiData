package ClinEpiData::Load::IcemrIndiaReader;
use base qw(ClinEpiData::Load::MetadataReader);

sub cleanAndAddDerivedData {
  my ($self, $hash) = @_;
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
}

1;

package ClinEpiData::Load::IcemrIndiaReader::HouseholdReader;
use base qw(ClinEpiData::Load::IcemrIndiaReader);
use strict;
use warnings;
use Switch;

sub cleanAndAddDerivedData {
  my ($self, $hash) = @_;
  $self->SUPER::cleanAndAddDerivedData($hash);
	if(defined($hash->{redcap_event_name})){
		if($hash->{redcap_event_name} !~ /^house/i){
			delete $hash->{$_} for keys %$hash;
			return undef;
		}
	  else{
	  	$hash->{household_redcap_event_name} = $hash->{redcap_event_name};
	  	delete $hash->{redcap_event_name};
	  }
	}
	if(!defined($hash->{studysite})){
		my $id = $hash->{cen_fid} || $hash->{sid};
		switch(substr(lc($hash->{cen_fid}),0,2)){
			case 'cc' { $hash->{studysite} = 'Chennai' }
			case 'nc' { $hash->{studysite} = 'Nadiad' }
			case 'rc' { $hash->{studysite} = 'Raurkela' }
		}
	}
	if(!defined($hash->{studysite})){
		my $file = $self->getMetadataFile();
		switch (lc($file)){
			case /nadiad/ { $hash->{studysite} = 'Nadiad' }
			case /raurkela/ { $hash->{studysite} = 'Raurkela' }
			case /chennai/ { $hash->{studysite} = 'Chennai' }
		}
	}
	if(defined($hash->{cen_district}) && (!defined($hash->{studysite}) || $hash->{studysite} eq "")){
		switch(lc($hash->{cen_district})){
			case 'chennai' { $hash->{studysite} = 'Chennai' }
			case 'kheda' { $hash->{studysite} = 'Nadiad' }
			case 'sundergarh' { $hash->{studysite} = 'Raurkela' }
			case 'sundargarh' { $hash->{studysite} = 'Raurkela' }
		}
	}
	if((defined($hash->{studysite})) && (!defined($hash->{state}) || $hash->{state} eq "")){
		switch(lc($hash->{studysite})){
			case 'chennai' { $hash->{state} = 'tamil nadu' }
			case 'raurkela' { $hash->{state} = 'odisha' }
			case 'nadiad' { $hash->{state} = 'gujarat' }
		}
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

1;

package ClinEpiData::Load::IcemrIndiaReader::ParticipantReader;
use base qw(ClinEpiData::Load::IcemrIndiaReader);
use Scalar::Util qw/looks_like_number/;
sub cleanAndAddDerivedData {
  my ($self, $hash) = @_;
  $self->SUPER::cleanAndAddDerivedData($hash);
#	my $file = $self->getMetadataFile();
#	$hash->{study_design} = "Cross-sectional";
#	if($file =~ /longitud/i){
#		$hash->{study_design} = "Longitudinal";
#	}
	$hash->{state_birth} =~ s/^-$/Missing/;
	## pastyear_treatdrugs_chk_* only take visit_1_enrollment_arm_1
  my @vars = grep { /^pastyear_treatdrugs_chk|^pastyear_treatwhere_chk|^pastyear_dx_chk/ } keys %$hash;
	foreach my $var (@vars){
		next unless defined($hash->{$var});
		next unless defined($hash->{redcap_event_name});
		delete($hash->{$var}) unless($hash->{redcap_event_name} eq 'visit_1_enrollment_arm_1');
	}
#	if(defined($hash->{pastyear_num_mo})){
#		$hash->{pastyear_num_mo} =~ s/\s+.*$//;
#  }
#	unless(looks_like_number($hash->{pastyear_num_mo})){
#		delete($hash->{pastyear_num_mo});
#	}
#	else{
#		$hash->{pastyear_num_mo} = sprintf("%03d", $hash->{pastyear_num_mo}); # force numeric
#	}
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
	return undef if(defined($hash->{primary_key})); ## already processed, do not run when loading parentMergedFile for Samples
  $self->SUPER::cleanAndAddDerivedData($hash);
	return if(ref($self) eq 'ClinEpiData::Load::IcemrIndiaReader::HouseholdObservationReader');
	if(defined($hash->{existing_illness_list}) && ($hash->{existing_illness_list} =~ /^3$/i)){
		delete $hash->{existing_illness_list};
	}
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
	if(defined($hash->{travel_2wk_district})){ $hash->{travel_2wk_district} =~ s/^-$/Missing/; }
	if(defined($hash->{travel_2wk_state})){ $hash->{travel_2wk_state} =~ s/^-$/Missing/; }

  if(1){
    my $score = 0;
    my @vars = grep { /^fever_2wk_where_chk___/ } keys %$hash;
    if(@vars && $hash->{fever_2wk_yn} eq '1'){
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
	if(defined($hash->{pastyear_treat_rad})){
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
	}

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
      $hash->{rdt_op_chk___1} = 'Not applicable';
      $hash->{rdt_op_chk___2} = 'Not applicable';
      $hash->{rdt_op_chk___3} = 'Not applicable';
    }
    elsif(($hash->{rdt_op_chk___4} == 1) && ($hash->{rdt_op_chk___3} == 0)){ 
      $hash->{rdt_op_chk___3} = 'Not applicable';
    }
  }
    
# rdt_fv_chk___1-4
  if(defined($hash->{rdt_fv_chk___4})){
    unless(setIfZero($hash, '^rdt_fv_chk___\d$')){
      $hash->{rdt_fv_chk___4} = 1;
      $hash->{rdt_fv_chk___1} = 'Not applicable';
      $hash->{rdt_fv_chk___2} = 'Not applicable';
      $hash->{rdt_fv_chk___3} = 'Not applicable';
    }
    elsif(($hash->{rdt_fv_chk___4} == 1) && ($hash->{rdt_fv_chk___3} == 0)){ 
      $hash->{rdt_fv_chk___3} = 'Not applicable';
    }
  }
# mx_species_chk___1-5
  unless(setIfZero($hash, '^mx_species_chk___\d$')){
    $hash->{mx_species_chk___5} = 1;
  }


  foreach my $var (qw/height weight temp_celsius hemocue/){
    delete($hash->{$var}) unless(looks_like_number($hash->{$var}));
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
package ClinEpiData::Load::IcemrIndiaReader::HouseholdObservationReader;
use base qw(ClinEpiData::Load::IcemrIndiaReader::ObservationReader);

sub makeParent {
  my ($self, $hash) = @_;
  if($hash->{parent}) {
    return $hash->{parent};
  }
  return $hash->{cen_fid};
}
sub makePrimaryKey {
  my ($self, $hash) = @_;
  if($hash->{"primary_key"}) {
    return $hash->{"primary_key"};
  }
  if(defined($hash->{cen_fid}) && defined($hash->{redcap_event_name})){
    return join("_", $hash->{cen_fid}, $hash->{redcap_event_name});
  }
  die "Cannot make primary key:\n" . Dumper($hash);
}

sub getPrimaryKeyPrefix {
  my ($self, $hash) = @_;
	return undef if(defined($hash->{primary_key}));
	return "HO";
}

sub cleanAndAddDerivedData {
# sprayed_chk___1-4
  my ($self, $hash) = @_;
	if(defined($hash->{redcap_event_name})){
		$hash->{household_redcap_event_name} = $hash->{redcap_event_name};
		delete($hash->{redcap_event_name});
	#unless ($hash->{household_redcap_event_name} =~ /houseinfo_arm_1|visit_1_enrollment_arm_1|visit 1|household census/i){
		unless ($hash->{household_redcap_event_name} =~ /visit_1_enrollment_arm_1|visit 1/i){
			delete $hash->{$_} for keys %$hash;
		}
	}
# sprayed_chk___1-4
  $self->SUPER::setIfZero($hash, '^sprayed_chk___\d$', 'NULL');
  $self->SUPER::cleanAndAddDerivedData($hash);
}
1;

package ClinEpiData::Load::IcemrIndiaReader::SampleReader;
use base qw(ClinEpiData::Load::IcemrIndiaReader);
use Scalar::Util qw/looks_like_number/;

sub makeParent {
  my ($self, $hash) = @_;
	return shift->makePrimaryKey(shift);
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

sub getPrimaryKeyPrefix {
  my ($self, $hash) = @_;
	return undef if(defined($hash->{primary_key}));
	return "SA";
}
sub cleanAndAddDerivedData {
# sprayed_chk___1-4
  my ($self, $hash) = @_;
  delete($hash->{hemocue}) unless(looks_like_number($hash->{hemocue}));
}

1;
