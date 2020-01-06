package ClinEpiData::Load::GenericReader;
use base qw(ClinEpiData::Load::MetadataReaderXT);

use strict;
use warnings;
use Data::Dumper;
my $DEBUG=0;

sub updateConfig {
  my ($self) = @_;
  my $idMappingFile = $self->getConfig('idMappingFile');
  my $noFilePrefix = $self->getConfig('noFilePrefix');
  if($idMappingFile){
    open(FH, "<", $idMappingFile) or die "Cannot open $idMappingFile:$!\n";
    my $idMap = {};
    foreach my $row (<FH>){
      chomp $row;
      next unless length($row);
      my($mdfile,$type,$col) = split(/\t/, $row);
      $mdfile = lc($mdfile);
      my @types = split(/\//,lc($type));
      my @cols = split(/\//,lc($col));
      for (my $i = 0; $i < @types; $i++){
        unless($types[$i] && $cols[$i]){ print STDERR "WARN: check syntax on row:\n$row\n" }
        my @idCols;
        if($noFilePrefix){
          (@idCols) = split(/\+/, $cols[$i]);
        }
        else{
          (@idCols) = map { join("::", $mdfile, $_) } split(/\+/, $cols[$i]);
        }
        $idMap->{$mdfile}->{$types[$i]} = \@idCols;
        $idMap->{idHash}->{$mdfile}->{$types[$i]}->{$_} = 1 for @idCols;
        
      }
    }
    close(FH);
    $self->setConfig('idMap', $idMap);
  }
  my $type = $self->getConfig('type');
  unless($type){
    ($type) = map { lc($_) } (ref($self) =~ m/::([^:]*)Reader$/);
    $self->setConfig('type', $type);
  }
} 

sub getId {
  my ($self, $hash, $type) = @_;
  unless($type){
    $type = $self->getConfig('type');
  }
  my $idMap = $self->getConfig('idMap');
  my $placeholder = $self->getConfig('placeholder');
  my $warnings = $self->getConfig('_warnings');
  unless($warnings){
    $warnings = {};
    $self->setConfig('_warnings',$warnings);
  }
  my $mdfile = $self->getMetadataFileLCB();
  die "$mdfile has no ID mapping" unless $mdfile;
  unless(defined($idMap->{$mdfile}->{$type})){
    my $warn = "WARNING: $type not defined for $mdfile";
    printf STDERR ("$warn\n") unless $warnings->{$warn};
    $warnings->{$warn} = 1;
    return undef;
  }
  #"$type ID mapping not defined in $mdfile" unless defined($idMap->{$mdfile}->{$type});
  my @idCols = @{ $idMap->{$mdfile}->{$type} };
  foreach my $col (@idCols){
    next if(defined($hash->{$col}));
    my $warn = "$col not defined in $mdfile for $type";
    printf STDERR ("$warn\n") unless $warnings->{$warn};
    $warnings->{$warn} = 1;
  }
  my @idValues;
  foreach my $col (@idCols){
    my $val = $hash->{$col};
    if(defined($placeholder) && ! defined($val)){
      $val = $placeholder;
    }
    push(@idValues,$val);
  }
  my $id = join("_", @idValues);
  printf STDERR "ID: $id\n" if $DEBUG;
  return $id;
}

sub cleanAndAddDerivedData {
  my ($self, $hash) = @_;
  return if defined $hash->{primary_key};
  my $type = lc($self->getConfig('type'));
  my $renameColumns = $self->getConfig('renameColumns');
  my $mdfile = $self->getMetadataFileLCB();
# my $idMap = $self->getConfig('idMap');
# unless(defined($idMap->{$mdfile}->{$type})){
#   # print STDERR ("$type:$mdfile - deleting row\n");
#   $self->skipRow($hash);
#   return;
# }
  my $noFilePrefix = $self->getConfig('noFilePrefix');
  unless($noFilePrefix){
    foreach my $var (keys %$hash){
      $hash->{join("::",$mdfile,$var) } = $hash->{$var};
      delete($hash->{$var});
    }
  }
  if($renameColumns){
    $self->applyMappedIRI($hash,1);
  }
}

1;
package ClinEpiData::Load::GenericReader::CategoryReader;
use base qw(ClinEpiData::Load::GenericReader);

sub makeParent {
  my ($self, $hash) = @_;
  if($hash->{parent}) {
    return $hash->{parent};
  }
  my $parentType = $self->getConfig('parentType');
  if(defined($parentType)){
    return $self->getId($hash,$parentType);
  }
}

sub getParentPrefix {
  my ($self, $hash) = @_;
  if($hash->{parent}) {
    return undef;
  }
  my $parentType = $self->getConfig('parentType');
  return unless defined $parentType;
	return $self->getConfig('prefix/'. $parentType);
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
  my $type = $self->getConfig('type');
  return unless defined $type;
	return $self->getConfig('prefix/' . $type);
}

1;

package ClinEpiData::Load::GenericReader::HouseholdReader;
use base qw(ClinEpiData::Load::GenericReader::CategoryReader);

1;
package ClinEpiData::Load::GenericReader::HouseholdObservationReader;
use base qw(ClinEpiData::Load::GenericReader::CategoryReader);

1;

package ClinEpiData::Load::GenericReader::ParticipantReader;
use base qw(ClinEpiData::Load::GenericReader::CategoryReader);

1;

package ClinEpiData::Load::GenericReader::ObservationReader;
use base qw(ClinEpiData::Load::GenericReader::CategoryReader);

1;

package ClinEpiData::Load::GenericReader::SampleReader;
use base qw(ClinEpiData::Load::GenericReader::CategoryReader);

1;
