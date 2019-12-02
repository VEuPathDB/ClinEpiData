package ClinEpiData::Load::ProvideReader;
use base qw(ClinEpiData::Load::MetadataReaderXT);

use strict;
use warnings;
use Data::Dumper;

sub updateConfig {
  my ($self) = @_;
  my $idMappingFile = $self->getConfig('idMappingFile');
  if($idMappingFile){
    open(FH, "<", $idMappingFile) or die "Cannot open $idMappingFile:$!\n";
    my $idMap = {};
    foreach my $row (<FH>){
      chomp $row;
      my($mdfile,$type,$col) = split(/\t/, $row);
      my @types = split(/\//,lc($type));
      my @cols = split(/\//,lc($col));
      for (my $i = 0; $i < @types; $i++){
        unless($types[$i] && $cols[$i]){ print STDERR "WARN: check syntax on row:\n$row\n" }
        my(@idCols) = map { join("::", $mdfile, $_) } split(/\+/, $cols[$i]);
        $idMap->{$mdfile}->{$types[$i]} = \@idCols;
        $idMap->{idHash}->{$mdfile}->{$types[$i]}->{$_} = 1 for @idCols;
        
      }
    }
    close(FH);
    $self->setConfig('idMap', $idMap);
  # $self->setConfig('cleanFirst', 1);
  }
  my ($type) = map { lc($_) } (ref($self) =~ m/::([^:]*)Reader$/);
  $self->setConfig('type', $type);
} 

sub getId {
  my ($self, $hash, $type) = @_;
  unless($type){
    $type = $self->getConfig('type');
  }
  my $idMap = $self->getConfig('idMap');
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
  my $id = join("_", map { $hash->{$_} } @idCols);
  return $id;
}

sub cleanAndAddDerivedData {
  my ($self, $hash) = @_;
  return if defined $hash->{primary_key};
  my $type = lc($self->getConfig('type'));
  my $idMap = $self->getConfig('idMap');
  my $mdfile = $self->getMetadataFileLCB();
# unless(defined($idMap->{$mdfile}->{$type})){
#   # print STDERR ("$type:$mdfile - deleting row\n");
#   $self->skipRow($hash);
#   return;
# }
  foreach my $var (keys %$hash){
    $hash->{join("::",$mdfile,$var) } = $hash->{$var};
    delete($hash->{$var});
  }
}

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

#sub cleanAndAddDerivedData {
#  my ($self, $hash) = @_;
#  if(defined($hash->{primary_key})){ return }
#  $self->SUPER::cleanAndAddDerivedData($hash);
#  $hash->{country} = 'Mali';
#}

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
use base qw(ClinEpiData::Load::ProvideReader);
use strict;
use warnings;
use Data::Dumper;

sub makeParent {
  my ($self, $hash) = @_;

  if($hash->{parent}) {
    return $hash->{parent};
  }
  return $self->getId($hash,'participant');
}

sub makePrimaryKey {
  my ($self, $hash) = @_;

  if(defined($hash->{primary_key})) {
    return $hash->{primary_key};
  }
  return $self->getId($hash);
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
  if(defined($hash->{primary_key})){ return }
  $self->SUPER::cleanAndAddDerivedData($hash);
  $self->applyMappedIRI($hash,1);

  $self->applyMappedValues($hash);

 #if(defined($hash->{observation_date})){
 #  $hash->{observation_date} = $self->formatDate($hash->{observation_date},"US");
 #}

 foreach my $var (qw/ext_bv_dir_st::dov ext_mgmt_bv_ddep_st::epidate ext_bv_dep_st::dedt ext_bv_dep_st::dedt/){
   if(defined($hash->{$var})){
     $hash->{$var} = $self->formatDate($hash->{$var},"US");
   }
 }

}

1;

package ClinEpiData::Load::ProvideReader::SampleReader;
use base qw(ClinEpiData::Load::ProvideReader::ObservationReader);

sub makeParent {
  my ($self, $hash) = @_;
  if($hash->{parent}) {
    return $hash->{parent};
  }
  return $self->getId($hash,'observation');
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
  return $self->getId($hash);
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
  if(defined($hash->{primary_key})){ return }
 #my $a = $hash->{vstnum};
 $self->applyMappedValues($hash);
 #my $b = $hash->{vstnum};
 ##if($a && ($a ne $b))
 #{print STDERR ("remapped vstnum %s = %s\n", $a, $b) }
  my @dateVars = grep { /dt$|^lmdoc$|^doc$/ } keys %$hash;
  foreach my $var (@dateVars){
    if(defined($hash->{$var}) && ($hash->{$var} ne "na")){
      $hash->{$var} = $self->formatDate($hash->{$var},"US");
      if($hash->{$var} eq '1999-09-09'){ delete $hash->{$var} }
    }
  }
  $self->SUPER::cleanAndAddDerivedData($hash);
}

1;
