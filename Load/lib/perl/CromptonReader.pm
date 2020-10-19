package ClinEpiData::Load::CromptonReader;
use base qw(ClinEpiData::Load::GenericReader);
1;

package ClinEpiData::Load::CromptonReader::ObservationReader;
use base qw(ClinEpiData::Load::GenericReader::CategoryReader);
use strict;
use warnings;
use Date::Manip qw/ParseDate DateCalc/;
use Data::Dumper;

sub cleanAndAddDerivedData {
  my ($self, $hash) = @_;
  return if defined $hash->{primary_key};
  foreach my $k (keys %$hash){
    next unless(defined($hash->{$k}));
    $hash->{$k} =~ s/^\s*|\s*$//g;
  }
  my $noDuration = 0;
  my $mdfile = $self->getMetadataFileLCB();
  if($mdfile eq '126plate100' || $mdfile eq '226plate400' || $mdfile eq '226plate300' || $mdfile eq '227plate300'){
    foreach my $k ( qw/conmedstdt cmstdat aestdt aestdat/){
      if(defined($hash->{$k}) && $hash->{$k} =~ /^na$/i){
        $hash->{$k} = '1900-01-01';
        $noDuration = 1;
      }
    }
  }
### 2020-07-06 add Unknown derived vars
  my @unknownvars = qw/cardio_end_dt gastrohep_end_dt gastrohep_st_dt genitorenal_st_dt heent_end_dt heent_st_dt hemlymph_st_dt musc_st_dt neuro_st_dt otherhx_st_dt.1 otherhx_st_dt psych_st_dt pulresp_end_dt pulresp_st_dt skinderm_st_dt/;
  foreach my $var ( @unknownvars ){
    if(defined($hash->{$var})){
      if($hash->{$var} =~ /^(r|u|nd|00)/){
        $hash->{ "${var}_unknown" } = "Yes";
      }
      else {
        $hash->{ "${var}_unknown" } = "No";
      }
    }
  }
###

  my $anc = $self->getAncillaryData();
  my ($cmstartvar) = grep { /conmedstdt$|cmstdat$/ } keys %$hash;
  my ($cmendvar) = grep { /conmedenddt$|cmendat$/ } keys %$hash;
  my ($aestartvar) = grep { /aestdt$|aestdat$/ } keys %$hash;
  my ($aeendvar) = grep { /aeenddt$|aeendat$/ } keys %$hash;
  #my @datevars = grep { /^${mdfile}::(aestdat|aestdt|cmstdat|conmedstdt|misseddat|otherhx_end_dt|otherhx_end_dt.1|otherhx_st_dt|otherhx_st_dt.1|visitdat|visitdate|lbdat|.*_st_dt|.*dt\.1|.*end_dt)/ } keys %{$anc->{iri}};
  my @datevars = grep { /^(aestdat|aestdt|cmstdat|conmedstdt|misseddat|otherhx_end_dt|otherhx_end_dt.1|otherhx_st_dt|otherhx_st_dt.1|visitdat|visitdate|lbdat|.*_st_dt|.*dt\.1|.*end_dt)$/ } keys %{$hash};
  #printf STDERR ("DEBUG: datevars %s\n", join(",", @datevars));
  if($cmstartvar){ push(@datevars, $cmstartvar) }
  if($cmendvar){ push(@datevars, $cmendvar) }
  my $conmedduration = "";
  if($aestartvar){ push(@datevars, $aestartvar) }
  if($aeendvar){ push(@datevars, $aeendvar) }
  my $aeduration = "";
  foreach my $var (@datevars){ 
    # $var =~ s/^${mdfile}:://;
    $var =~ s/\/fev\//\/feb\//;
    if(defined($hash->{$var}) && length($hash->{$var})){
      my $date = $hash->{$var};
      if($date =~ /^na$|^nd$|^u$|^\*$|^\.nd$|0000$/i){ delete($hash->{$var}); next }
      $date =~ s/^00\/000\//15\/jun\//;
      $date =~ s/^00\//15\//;
      $hash->{$var} = $self->formatDate($date);
print STDERR "DEBUG: $var = $hash->{$var} = $date\n" if $hash->{$var} =~ /\//;
      #printf STDERR ("DEBUG: %s\t%s\n", $date, $hash->{$var}) if ($date ne $hash->{$var});
    }
  }
  if($cmstartvar && $cmendvar && length($hash->{$cmstartvar}) && length($hash->{$cmendvar})){
    unless($noDuration){
      $conmedduration = $self->dateDiff($hash->{$cmstartvar},$hash->{$cmendvar});
    }
  }
  if($aestartvar && $aeendvar && length($hash->{$aestartvar}) && length($hash->{$aeendvar})){
    unless($noDuration){
      $aeduration = $self->dateDiff($hash->{$aestartvar},$hash->{$aeendvar});
    }
  }
  $self->SUPER::cleanAndAddDerivedData($hash);
  foreach my $var ( @unknownvars ){
    $var .= "_unknown";
    next unless defined $hash->{ "${mdfile}::$var" };
    $hash->{ $var } = $hash->{ "${mdfile}::$var" };
    delete $hash->{ "${mdfile}::$var" };
  }
  $hash->{conmedduration} = $conmedduration;
  $hash->{aeduration} = $aeduration;
  foreach my $v (grep { /_dt$/ } keys %$hash){
    $hash->{$v} =~ s/^\s*(u|nd|\*)\s*$|\s*.*0000\s*$// if defined($hash->{$v});
  }
}

sub dateDiff {
  my($self, $var1, $var2) = @_;
  if($var1 && $var2){
    my $screen = '/^1900-01-01$|^na$|^nd$|^u$|^00|\/000\/|^\*$/';
    if($var1 =~ $screen || $var2 =~ $screen){ return undef;}
    my $start = ParseDate($var1);
    my $end = ParseDate($var2);
    my @delta = split(/:/, DateCalc($start,$end));
    return $delta[4] / 24.0;
  }
  return undef;
}

 
1;

package ClinEpiData::Load::CromptonReader::SampleReader;
use base qw(ClinEpiData::Load::GenericReader::ObservationReader);

sub cleanAndAddDerivedData { return $_[0]->SUPER::cleanAndAddDerivedData($_[1]) }

1;

package ClinEpiData::Load::CromptonReader::OutputReader;
use base qw(ClinEpiData::Load::GenericReader::OutputReader);
1;
