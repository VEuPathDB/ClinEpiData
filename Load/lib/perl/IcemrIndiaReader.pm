package ClinEpiData::Load::IcemrIndiaReader;
use base qw(ClinEpiData::Load::MetadataReader);

sub cleanAndAddDerivedData {
  my ($self, $hash) = @_;
}
1;

package ClinEpiData::Load::IcemrIndiaReader::ParticipantReader;
use base qw(ClinEpiData::Load::IcemrIndiaReader);

sub makeParent {
  my ($self, $hash) = @_;
  if($hash->{parent}) {
    return $hash->{parent};
  }
  return $hash->{cen_fid};
}
sub makePrimaryKey {
  my ($self, $hash) = @_;
  my ($self, $hash) = @_;

  if($hash->{primary_key}) {
    return $hash->{primary_key};
  }

  return $hash->{sid};
}

1;

package ClinEpiData::Load::IcemrIndiaReader::HouseholdReader;
use base qw(ClinEpiData::Load::IcemrIndiaReader);
# use Data::Dumper;
## loads file micro_x24m.csv
use strict;
use warnings;

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

  if(defined($hash->{fever_2k_yn})){
    my $score = 0;
    my @vars = grep { /^fever_2wk_where_chk___/ } keys %$hash;
    if($hash->{fever_2k_yn} eq '1'){
    # fever_2wk_where_chk__* 
    # if all are '0', change all 4 to "N/A"
    # may have to add rows to valueMap.txt
      map { $score += $hash->{$_} } @vars;
      unless( $score ){
        map {$hash->{$_} = "N/A" } @vars;
      }
    }
    else {
    # do not load any of these
      map {$hash->{$_} = "N/A" } @vars;
    }
  }
# pastyear_treatdrugs_chk__* 
# if all are '0', change all 4 to "N/A"
# may have to add rows to valueMap.txt
  if($hash->{pastyear_treat_rad} eq '1' || $hash->{pastyear_treat_rad} eq '3'){
    setIfZero($hash, '^pastyear_treatdrugs_chk___\d+$', 'NA');
    setIfZero($hash, '^pastyear_treatwhere_chk___\d+$', 'NA');
  # my $score = 0;
  # my @vars = grep { /^pastyear_treatdrugs_chk___/ } keys %$hash;
  # foreach my $var (@vars){
  #   unless(looks_like_number($hash->{$_} || 0)){
  #     die "ERROR value in '$_' is not a number\n";
  #   }
  # }
  # map { $score += $hash->{$_} } @vars;
  # unless( $score ){
  #   map {$hash->{$_} = "N/A" } @vars;
  # }
  # $score = 0;
  # @vars = grep { /^pastyear_treatwhere_chk___/ } keys %$hash;
  # foreach my $var (@vars){
  #   unless(looks_like_number($hash->{$_} || 0)){
  #     die "ERROR value in '$_' is not a number\n";
  #   }
  # }
  # map { $score += $hash->{$_} } @vars;
  # unless( $score ){
  #   map {$hash->{$_} = "N/A" } @vars;
  # }
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
  setIfZero($hash, '^rdt_op_chk___\d$', '1');
# rdt_fv_chk___1-4
  setIfZero($hash, '^rdt_fv_chk___\d$', '1');
# mx_species_chk___1-5
  setIfZero($hash, '^mx_species_chk___\d$', '1');


  foreach my $var (qw/height weight temp_celcius hemocue/){
    if(defined($hash->{$var}) && $hash->{$var} !~ /\d/){
      delete($hash->{$var});
    }
  }
  if(defined($hash->{pastyear_treatdate})){
    my $val = $hash->{pastyear_treatdate};
    $val =~ tr/_/ /; ## some dates look like mmm_YYYY
    $val =~ s/^(\d\d)-(...)$/$2 $1/; ## some dates look like YY-mmm
    $val =~ s/^(\d+)$/1-1-$1/; ## just a year

    $hash->{pastyear_treatdate} = $val;
  }
}

## if all hash vars matching pattern are zero, delete them
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
  unless( $score ){
    if($value eq 'NULL'){
      delete($hash->{$_}) for @vars;
    }
    else {
      $hash->{$_} = $value for @vars;
    }
  # printf STDERR ("All set to %s: %s\n", $value, join(",", @vars));
  }
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
