package ClinEpiData::Load::CromptonReader;
use base qw(ClinEpiData::Load::MetadataReader);

use strict;
use warnings;
use Data::Dumper;

sub getId {
  my ($self, $hash) = @_;
  #printf STDERR ("%s\tsubj_id empty\n", $self->getMetadataFile()) unless $hash->{subj_id};
  my ($idcol) = grep { /subj_id/ } keys %$hash;
  $hash->{subj_id} = $hash->{$idcol};
  if(!defined($hash->{subj_id}) || $hash->{subj_id} eq ""){ return }
  return sprintf("%03d",$hash->{subj_id});
}

sub cleanAndAddDerivedData {
  my ($self, $hash) = @_;
  return if defined $hash->{primary_key};
  ## simplfy id column headers for this Reader code
  for my $idcol (qw/subj_id visitdate visnum dfseq/){
    my ($realCol) = grep { /$idcol/ } keys %$hash;
    next unless defined($realCol);
    #$self->debug("idcol $idcol => $realCol");
    $hash->{$idcol} = $hash->{$realCol};
  }
  unless($hash->{subj_id}){ $self->skipRow($hash); return; }
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
    else { delete $hash->{$col} }
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

  if($hash->{primary_key}) {
    return $hash->{primary_key};
  }
  return unless($hash->{subj_id});
  my $visit = sprintf("%04d", $hash->{visnum} || $hash->{dfseq} || "0");
  $hash->{visitdate} =~ s/^(\s+|nd|\*)$// if $hash->{visitdate};
  my $date = "na";
  $date = $self->formatDate($hash->{visitdate}) if $hash->{visitdate};
  return sprintf("%s_%s_%s", $self->getId($hash), $date, $visit);
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
  return if $hash->{primary_key};
# my $prefix = $self->getMetadataFileLCB();
# my %idIRI = (
#   eupath_0000095 => 'subj_id',
#   eupath_0004991 => 'visitdate',
#   bfo_0000015 => 'visnum',
# ); 
# map { $hash->{ $idIRI{$_} } = $hash->{$_} } keys %idIRI;
# # $self->debug($prefix . "\n" . Dumper($hash));exit;
# foreach my $idvar ( qw/subj_id visitdate visnum dfseq/ ){
#   next if(defined($hash->{$idvar}));
#   my $key = join("::", $prefix, $idvar);
#   $hash->{$idvar} ||= $hash->{$key} if defined $hash->{$key};
# }
# return unless($hash->{subj_id});
  $self->SUPER::cleanAndAddDerivedData($hash);
  unless($self->getId($hash)) { $self->skipRow($hash) }
  else { $self->applyMappedIRI($hash) }
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
