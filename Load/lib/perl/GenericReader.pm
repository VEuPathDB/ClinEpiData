package ClinEpiData::Load::GenericReader;
use base qw(ClinEpiData::Load::MetadataReaderXT);

use strict;
use warnings;

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
      my($mdfile,$type,$col) = map { lc($_) } split(/\t/, $row);
      next unless $col;
      $mdfile = lc($mdfile);
      my @idCols;
      if($noFilePrefix){
        @idCols = split(/\+/, $col);
      }
      else{
        foreach my $c (split(/\+/, $col)){
          if($c =~ /^\{\{.*\}\}$/){ push(@idCols, $c) }
          else { push(@idCols, join("::", $mdfile, $c)) }
        }
      }
      $idMap->{$mdfile}->{lc($type)} = \@idCols;
      $idMap->{idHash}->{$mdfile}->{lc($type)}->{$_} = 1 for @idCols;
    }
    close(FH);
    $self->setConfig('idMap', $idMap);
  }
  my $type = $self->getConfig('category') || $self->getConfig('type');
  unless($type){
    ($type) = map { lc($_) } (ref($self) =~ m/::([^:]*)Reader$/);
    $self->setConfig('type', $type);
  }
} 

sub getId {
  my ($self, $hash, $type) = @_;
  unless($type){
    $type = $self->getConfig('category') || $self->getConfig('type');
    $type = lc($type) if $type;
  }
  my $idMap = $self->getConfig('idMap');
  my $placeholder = $self->getConfig('placeholder');
  my $warnings = $self->getConfig('_warnings');
  unless($warnings){
    $warnings = {};
    $self->setConfig('_warnings',$warnings);
  }
  my $mdfile = $self->getMetadataFileLCB();
  # die "$mdfile has no ID mapping" unless $mdfile;
  unless(defined($idMap->{$mdfile}->{$type})){
   #my $warn = "[a] WARNING: $type not defined for $mdfile";
   #printf STDERR ("$warn\n") unless $warnings->{$warn};
   #$warnings->{$warn} = 1;
    return undef;
  }
  print STDERR "$type ID mapping not defined in $mdfile" unless defined($idMap->{$mdfile}->{$type});
  my @idCols = @{ $idMap->{$mdfile}->{$type} };
  foreach my $col (@idCols){
    next if(defined($hash->{$col}));
    next if($col =~ /^\{.*\}$/);
    my $warn = "[b] $col not defined in $mdfile for $type";
    printf STDERR ("$warn\n") unless $warnings->{$warn};
    $warnings->{$warn} = 1;
  }
  my @idValues;
  # my @qaCheck;
  foreach my $col (@idCols){
    my $origValue = $hash->{$col};
    if(defined($origValue)){
      # push(@qaCheck, "$col=$origValue");
      $origValue =~ s/^\s*|\s*$//g;
    }
    my $val = "UnDeF";
    if($col =~ /^\{\{.*\}\}$/){ # the "column" is a literal string, ex. "id+{{time0}}" gets the value from column "id" and adds "time0", result 00129_time0 for id=00129
      ($val) = ($col =~ m/^\{\{(.*)\}\}$/);
    }
    elsif($col =~ /\{\{/){ # bad syntax?
      die "$col did not match\n"
    }
    elsif($col =~ /.+\{\d*,\d*\}$/){ # substring, ex. "id{3,99}" to get "id" minus the first 3 characters
      my ($start, $end) = ($col =~ m/\{(\d*),(\d*)\}$/);
      $col =~ s/\{.*$//;
      unless(defined($origValue)){
        my $warn = "[c] $col not defined in $mdfile for $type";
        printf STDERR ("$warn\n") unless $warnings->{$warn};
        $warnings->{$warn} = 1;
      }
      else{
        $val = substr($origValue, $start, $end);
        print STDERR ("$origValue = $val\n");
      }
    }
    else {
      $val = $origValue;
    }
    if(!(defined($val) && length($val)) && defined($placeholder)){
      if($placeholder =~ /__line__/){
        $val = $hash->{__line__};
      }
      else {
        $val = $placeholder;
      }
    }
    unless(defined($val)){
      die "ID Mapping invalid/missing for type/category [$type]:\n\tFILE=[$mdfile] COL=[$col]\nvalid columns:\n" . join("\n", map { "[$_]" } keys %$hash);
    }
    push(@idValues,$val);
  }
  my $id = join("_", @idValues);
  # printf STDERR ("QA CHECK: %s => %s\n", join("+", @qaCheck), $id);
  return $id;
}

sub cleanAndAddDerivedData {
  my ($self, $hash) = @_;
  return if defined $hash->{primary_key};
  my $type = lc($self->getConfig('type'));
  my $renameColumns = $self->getConfig('renameColumns');
  $renameColumns ||= $self->getConfig('applyMappedIRI');
  my $doValueMapping = $self->getConfig('doValueMapping');
  $doValueMapping ||= $self->getConfig('applyMappedValues');
  my $mdfile = $self->getMetadataFileLCB();
  my $noFilePrefix = $self->getConfig('noFilePrefix');
  unless($noFilePrefix){
    foreach my $var (keys %$hash){
      next if ($var eq '__line__');
      $hash->{join("::",$mdfile,$var) } = $hash->{$var};
      delete($hash->{$var});
    }
  }
  if($renameColumns){
    # $self->log("applyMappedIRI ...");
    $self->applyMappedIRI($hash,1);
  }
  if($doValueMapping){
    # $self->log("applyMappedValues ...");
    $self->applyMappedValues($hash,1);
  }
}

1;
package ClinEpiData::Load::GenericReader::CategoryReader;
use base qw(ClinEpiData::Load::GenericReader);
use strict;
use warnings;
use Data::Dumper;

sub makeParent {
  my ($self, $hash) = @_;
  if($hash->{parent}) {
    return $hash->{parent};
  }
  my $parentType = $self->getConfig('parentCategory') || $self->getConfig('parentType');
  if(defined($parentType)){
    return $self->getId($hash,lc($parentType));
  }
  #else{ print STDERR Dumper $self->{_CONFIG}; exit }
}

sub getParentPrefix {
  my ($self, $hash) = @_;
  if($hash->{parent}) {
    return undef;
  }
  my $parentType = $self->getConfig('parentCategory') || $self->getConfig('parentType');
  return unless defined $parentType;
	return $self->getConfig('prefix/'. lc($parentType));
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
	return $self->getConfig('prefix/' . lc($type));
}

1;

package ClinEpiData::Load::GenericReader::HouseholdReader;
use base qw(ClinEpiData::Load::GenericReader::CategoryReader);
1;
package ClinEpiData::Load::GenericReader::EntomologyReader;
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
package ClinEpiData::Load::GenericReader::OutputReader;
use base qw(ClinEpiData::Load::GenericReader::CategoryReader);
#use base qw(ClinEpiData::Load::MetadataReader);

sub makeParent {
  my ($self, $hash) = @_;
  return $hash->{parent};
}

sub makePrimaryKey {
  my ($self, $hash) = @_;
  return $hash->{primary_key};
}

1;
