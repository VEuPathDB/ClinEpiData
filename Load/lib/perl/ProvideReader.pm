package ClinEpiData::Load::ProvideReader;
use base qw(ClinEpiData::Load::GenericReader);

1;

package ClinEpiData::Load::ProvideReader::HouseholdReader;
use base qw(ClinEpiData::Load::ProvideReader);
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

package ClinEpiData::Load::ProvideReader::ParticipantReader;
use base qw(ClinEpiData::Load::ProvideReader);
use strict;
use warnings;
use Data::Dumper;

# sub updateConfig {
#   my ($self, $hash) = @_;
#   $self->setConfig('useFilePrefix',0);
# }

sub makeParent {
  my ($self, $hash) = @_;
  if($hash->{parent}) {
    return $hash->{parent};
  }
  return $self->getId($hash,'household');
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

  my $id = $self->getId($hash);
  unless($id){ $self->skipRow($hash) }
  else { return $id }
}

1;


package ClinEpiData::Load::ProvideReader::ObservationReader;
#use base qw(ClinEpiData::Load::ProvideReader);
use base qw(ClinEpiData::Load::GenericReader::CategoryReader);
use strict;
use warnings;
use Data::Dumper;

sub cleanAndAddDerivedData {
  my ($self, $hash) = @_;
  if(defined($hash->{primary_key})){ return }
  $self->SUPER::cleanAndAddDerivedData($hash);
  $self->applyMappedIRI($hash,1);

  $self->applyMappedValues($hash);

 #if(defined($hash->{observation_date})){
 #  $hash->{observation_date} = $self->formatDate($hash->{observation_date},"US");
 #}

  my @dateVars = grep { /dt$|doc$|date$|dov$/ } keys %$hash;
 #foreach my $var (qw/ext_bv_dir_st::dov ext_mgmt_bv_ddep_st::epidate ext_bv_dep_st::dedt ext_bv_dep_st::dedt ext_bv_ptreat_st::dov/){
  foreach my $var (@dateVars){
    if(defined($hash->{$var}) && length($hash->{$var}) && ($hash->{$var} ne "na")){
      $hash->{$var} = $self->formatDate($hash->{$var},"US");
      if($hash->{$var} eq '1999-09-09'){ $hash->{$var} = "" }
    }
  }

  ## aeduration
  if(defined($hash->{'ext_mgmt_bv_aed_st::aedt'}) && defined($hash->{'ext_mgmt_bv_aed_st::aeresdt'})){
    $hash->{aeduration} = $self->dateDiff($hash->{'ext_mgmt_bv_aed_st::aedt'},$hash->{'ext_mgmt_bv_aed_st::aeresdt'});
  }
  elsif(defined($hash->{'ext_bv_sae_st::saerdt'}) && defined($hash->{'ext_bv_sae_st::saersdt'})){
    $hash->{aeduration} = $self->dateDiff($hash->{'ext_bv_sae_st::saerdt'},$hash->{'ext_bv_sae_st::saersdt'});
  }
}

1;

package ClinEpiData::Load::ProvideReader::SampleReader;
use base qw(ClinEpiData::Load::GenericReader::CategoryReader);

sub cleanAndAddDerivedData {
  my ($self, $hash) = @_;
  if(defined($hash->{primary_key})){ return }
 #my $a = $hash->{vstnum};
 $hash->{$_} =~ s/"//g for keys %$hash;
 $self->applyMappedValues($hash);
 #my $b = $hash->{vstnum};
 ##if($a && ($a ne $b))
 #{print STDERR ("remapped vstnum %s = %s\n", $a, $b) }
  my @dateVars = grep { /dt$|doc$|date$|dov|dt18$/ } keys %$hash;
  foreach my $var (@dateVars){
    next unless $hash->{$var};
    if(defined($hash->{$var}) && ($hash->{$var} ne "na")){
      my $date = $self->formatDate($hash->{$var},"US");
      if(defined($date)){
        if($hash->{$var} eq '1999-09-09'){ $hash->{$var} = "" }
        else { $hash->{$var} = $date }
      }
      else{ print STDERR "cannot format date $hash->{$var}\n" }
        
    }
  }
  $self->SUPER::cleanAndAddDerivedData($hash);
}

1;

package ClinEpiData::Load::ProvideReader::OutputReader;
use base qw(ClinEpiData::Load::GenericReader::OutputReader);

1;
