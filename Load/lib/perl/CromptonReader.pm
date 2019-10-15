package ClinEpiData::Load::CromptonReader;
use base qw(ClinEpiData::Load::MetadataReader);

use strict;
use warnings;
use Data::Dumper;

sub getId {
  my ($self, $hash) = @_;
  #printf STDERR ("%s\tsubj_id empty\n", $self->getMetadataFile()) unless $hash->{subj_id};
  my ($idcol) = grep { /subj_id/i } keys %$hash;
  unless(defined($idcol)){
    printf STDERR ("No id column in %s\n", $self->getMetadataFileLCB());
    return;
  }
  $hash->{subj_id} = $hash->{$idcol};
  if(!defined($hash->{subj_id}) || $hash->{subj_id} eq ""){ return }
  return sprintf("%03d",$hash->{subj_id});
}

sub cleanAndAddDerivedData {
  my ($self, $hash) = @_;
  return if defined $hash->{primary_key};
  ## simplfy id column headers for this Reader code
  foreach my $idcol (qw/subj_id visitdate visnum dfseq/){
    my ($realCol) = grep { /$idcol/ } keys %$hash;
    next unless defined($realCol);
    $hash->{$idcol} = $hash->{$realCol};
  }
  unless(defined($hash->{subj_id})){ $self->skipRow($hash); return; }
  # if($hash->{dob} && $hash->{dob} eq '.u'){ printf STDERR ("%s has garbage dob\n", $self->getMetadataFile()) }
  foreach my $k (keys %$hash){
    next unless defined($hash->{$k});
    $hash->{$k} =~ s/^(\s+|nd|u|\.u|\.nd|\.f|\*|\r)$//;
  }
  $self->applyMappedValues($hash);
  foreach my $col ( grep { /visitdate|dateenrol|_dt$|_dt\.1$/ } keys %$hash ){
    next unless $hash->{$col};
    my ($day,$mon,$year) = split(/\//, $hash->{$col});
    if(!defined($year) || ($year =~ /^0+$/)){ delete($hash->{$col}); next }
    if(!defined($day) || ($day  =~ /^0+$/)){ $day = 15 }
    if(!defined($mon) || ($mon =~ /^0+$/)){ $mon = 'jul' }
    if($year && $mon && $day){
      $hash->{$col} = $self->formatDate(sprintf("%04d-%s-%02d", $year, $mon, $day));
    }
    else { printf STDERR ("Bad date: $hash->{$col}"); delete $hash->{$col} }
  }
}

sub readAncillaryInputFile {
# use for 1. value mapping, and 2. IRI mapping
  my ($self, $ancFile) = @_;
  open(FH, "<$ancFile") or die "Cannot read $ancFile:$!\n";
  my $anc = {};
  while(my $row = <FH>){
    chomp $row;
    $row =~ s/(\r|\l)$//g;
    my($col, $iri, $val, $mapVal) = split(/\t/,$row);
    if(defined($val) && defined($mapVal)){
      $anc->{var}->{lc($col)}->{lc($val)} =$mapVal;
    }
    $anc->{iri}->{lc($col)}= lc($iri);
  }
  close(FH);
  return $anc;
}

sub applyMappedValues {
  my ($self,$hash) = @_;
  my $anc = $self->getAncillaryData();
  while(my ($k, $v) = each %$hash){
    $k = lc($k);
    next unless(defined($v));
    $v = lc($v);
    if(
      defined($anc->{var}->{$k}) &&
      defined($anc->{var}->{$k}->{$v})){
      if($anc->{var}->{$k}->{$v} eq ':::UNDEF:::'){
        delete($hash->{$k});
      }
      else {
        $hash->{$k} = $anc->{var}->{$k}->{$v};
      }
    }
    if(defined($hash->{$k}) && $hash->{$k} =~ /undef/i){ die Dumper $hash }
  }
}

sub applyMappedIRI {
  my ($self,$hash) = @_;
  my $anc = $self->getAncillaryData();
  while(my ($col, $val) = each %$hash){
    my $iri = $anc->{iri}->{$col};
    if($iri){
      $hash->{$iri} = $hash->{$col};
      delete($hash->{$col});
    }
  }
}

sub makeObservationKey {
  my ($self, $hash) = @_;
  my $visit = sprintf("%04d", $hash->{visnum} || $hash->{dfseq} || "0");
  $hash->{visitdate} =~ s/^(\s+|nd|\*)$// if $hash->{visitdate};
  my $date = "na";
  $date = $self->formatDate($hash->{visitdate}) if $hash->{visitdate};
  my $id = $self->getId($hash);
  if($id){
    return sprintf("%s_%s_%s", $self->getId($hash), $date, $visit);
  }
  return undef;
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

#sub cleanAndAddDerivedData {
#  my ($self, $hash) = @_;
#  if(defined($hash->{primary_key})){ return }
#  $self->SUPER::cleanAndAddDerivedData($hash);
#  $hash->{country} = 'Mali';
#}

1;

package ClinEpiData::Load::CromptonReader::ParticipantReader;
use base qw(ClinEpiData::Load::CromptonReader);
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
use strict;
use warnings;
use Data::Dumper;

sub makePrimaryKey {
  my ($self, $hash) = @_;

  if($hash->{primary_key}) {
    return $hash->{primary_key};
  }
  my $visit = $hash->{visnum} || $hash->{dfseq} || "0";
  my $id = $self->getId($hash);
  return unless defined($id);
  #die "No ID " . Dumper $hash unless defined $id; 
  return sprintf("%s_%04d", $self->getId($hash), $visit);
}
sub getPrimaryKeyPrefix {
  my ($self, $hash) = @_;
  if($hash->{primary_key}) {
    return undef;
  }
	return "h";
}
sub cleanAndAddDerivedData {
  my ($self, $hash) = @_;
  if(defined($hash->{primary_key})){ return }
  $self->SUPER::cleanAndAddDerivedData($hash);
  $hash->{country} = 'Mali';
  if(defined($hash->{subj_id})){
    $self->applyMappedIRI($hash);
  }
}

1;


package ClinEpiData::Load::CromptonReader::ObservationReader;
use base qw(ClinEpiData::Load::CromptonReader);
use strict;
use warnings;
use Data::Dumper;

sub makeParent {
  my ($self, $hash) = @_;

  if($hash->{parent}) {
    return $hash->{parent};
  }
  my $pid = $self->getId($hash);
}

sub makePrimaryKey {
  my ($self, $hash) = @_;

  if(defined($hash->{primary_key})) {
    return $hash->{primary_key};
  }
  my $id = $self->getId($hash);
  return unless defined($id);
  my @idcols = ('visitdate', 'visnum', 'dfseq');
  unless(
    defined($hash->{visitdate}) || defined($hash->{visnum})|| defined($hash->{dfseq})){
    my @vals = map { $hash->{$_} || '-' } @idcols;
    printf STDERR ("cannot make key:%s:%s\n",join(",",@idcols), join(",", @vals));
  }
  my $visit = sprintf("%04d", $hash->{visnum} || $hash->{dfseq} || "0");
  $hash->{visitdate} =~ s/^(\s+|nd|\*)$// if $hash->{visitdate};
  my $date = "na";
  $date = $self->formatDate($hash->{visitdate}) if $hash->{visitdate};
  my $oid = sprintf("%s_%s_%s", $id, $date, $visit);
  return $oid;
}

sub getPrimaryKeyPrefix {
  my ($self, $hash) = @_;
  if($hash->{primary_key}) {
    return undef;
  }
	return "o";
}
sub cleanAndAddDerivedData {
  my ($self, $hash) = @_;
  if(defined($hash->{primary_key})){ printf STDERR ("No CLEANING will be done"); return; }
  $self->SUPER::cleanAndAddDerivedData($hash);
  if(defined($hash->{subj_id})){
    $self->applyMappedIRI($hash);
  }
}

1;

package ClinEpiData::Load::CromptonReader::SampleReader;
use base qw(ClinEpiData::Load::CromptonReader);

sub makeParent {
  my ($self, $hash) = @_;
  if($hash->{parent}) {
    return $hash->{parent};
  }
  return $self->makeObservationKey($hash);
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
  return $self->makeObservationKey($hash);
}

sub getPrimaryKeyPrefix {
  my ($self, $hash) = @_;
  if($hash->{primary_key}) {
    return undef;
  }
	return "s";
}

sub cleanAndAddDerivedData {
  my ($self, $hash) = @_;
  $self->SUPER::cleanAndAddDerivedData($hash);
  $self->applyMappedIRI($hash);
}

sub readAncillaryInputFile {
  my ($self, $ancFile) = @_;
  my $anc = $self->SUPER::readAncillaryInputFile($ancFile);
  while( my ($col, $iri) = %{ $anc->{iri} } ){
    if($iri =~ /eupath_0033265|eupath_0041075|eupath_0000047/){
      delete($anc->{iri}->{$col});
    }
  }
  return $anc;
}


1;
