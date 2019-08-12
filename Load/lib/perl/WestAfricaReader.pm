package ClinEpiData::Load::WestAfricaReader;
use base qw(ClinEpiData::Load::MetadataReader);
use strict;
use warnings;
use Switch;
use File::Basename;
use Carp qw/cluck/;
use Data::Dumper;

sub cleanAndAddDerivedData {
  my ($self, $hash) = @_;
  my $mdfile = $self->getSource();
  my $src = $self->getSrc();
  if($src eq 'crf1'){
    if( ($self->getPid($hash) eq "") or ($hash->{consent} eq "0") ){
      $self->skipRow($hash);
      return;
    }
  }
 #if($src eq 'crf3' || $src eq 'crf8'){
    $hash->{dov} ||= '7/7/07'; 
    $hash->{doc} ||= '7/7/07'; 
 #}
  foreach my $var (keys %$hash){
    next if(($var eq "__PARENT__") || ($var eq "PRIMARY_KEY"));
    my $newVar = join("::", $mdfile, $var);
    $hash->{ $newVar } = $hash->{$var};
    delete($hash->{$var});
  }
}

sub getSource {
  my ($self) = @_;
  my ($filename) = fileparse(lc($self->getMetadataFile()), qr/\.[^.]+$/);
  return $filename;
}
sub getSrc {
  my ($self) = @_;
  return substr($self->getSource(), 0, 4);
}

sub getValue {
  my ($self, $hash, $var) = @_;
  if(defined($hash->{$var})){ return $hash->{$var}; }
  my $fullKey = join("::", $self->getSource(), $var);
  my $val = $hash->{$fullKey};
  unless(defined($val)){
    my @similar = grep { /$var/ } keys %$hash;
    printf STDERR ("%s not found, similar: %s\n",$fullKey, join(",", @similar));
  }
  return $val;
}

sub getPid {
  my ($self, $hash) = @_;
  switch ($self->getSrc()){
    case 'crf1' { return $hash->{studyid} }
    case 'crf3' { return $hash->{studyid1} }
    case 'crf8' { return $hash->{a6_study_subject_id} }
    case 'crf9' { return $hash->{projectspecificid} }
  };
}
  

sub getIntervalName {
  my ($self, $hash) = @_;
  my ($varName) = grep { /intervalname/ } keys %$hash;
  unless($varName && $hash->{$varName}){
    cluck(sprintf("No intervalname for %s in %s \n", $self->getPid($hash), $self->getSource()));
    print Dumper $hash;
    exit;
  }
  my $val = lc($hash->{$varName});
  $val =~ s/^.*(enroll).*$/$1/;
  $val =~ tr/- //d;
  return $val;
}

sub parentExists {
  my ($self,$id) = @_;
  my $pre = $self->getParentPrefix();
  my $ppo = $self->getParentParsedOutput();
  return unless defined $ppo;
  my $pid = $id;
  return unless $pid;
  $pid = join("", $pre, $id) if(defined($pre));
  return defined($ppo->{$pid});
}

1;

package ClinEpiData::Load::WestAfricaReader::HouseholdReader;
use base qw(ClinEpiData::Load::WestAfricaReader);

sub makeParent {
  return undef; 
}
sub makePrimaryKey {
  my ($self, $hash) = @_;
  if($hash->{primary_key}) {
    return $hash->{primary_key};
  }
  return $self->getPid($hash);
}

sub getPrimaryKeyPrefix {
  my ($self, $hash) = @_;
  if($hash->{primary_key}) {
    return "";
  }
  return "h";
}

sub cleanAndAddDerivedData {
  my ($self, $hash) = @_;
  return if defined $hash->{primary_key}; ## when reading parentParsedOutput
  $hash->{country} ||= 3;
  $self->SUPER::cleanAndAddDerivedData($hash);
}

1;
package ClinEpiData::Load::WestAfricaReader::ParticipantReader;
use base qw(ClinEpiData::Load::WestAfricaReader);

sub makeParent {
  my ($self, $hash) = @_;
  return $self->makePrimaryKey($hash);
}

sub getParentPrefix {
  my ($self, $hash) = @_;
  if(defined($hash->{parent})) {
    return "";
  }
  return "h";
}

sub makePrimaryKey {
  my ($self, $hash) = @_;

  if($hash->{primary_key}) {
    return $hash->{primary_key};
  }
  return $self->getPid($hash);
}
sub cleanAndAddDerivedData {
  my ($self, $hash) = @_;
  if(defined($hash->{primary_key})){ return; }
  $self->SUPER::cleanAndAddDerivedData($hash);
}

1;

package ClinEpiData::Load::WestAfricaReader::ObservationReader;
use base qw(ClinEpiData::Load::WestAfricaReader);
use Data::Dumper;
sub skipIfNoParent {
	return 1;
}

sub makeParent {
  my ($self, $hash) = @_;

  if($hash->{primary_key}) {
    return $hash->{primary_key};
  }
  my $id = $hash->{participantid};
  return $id if $self->parentExists($id);
}

sub makePrimaryKey {
  my ($self, $hash) = @_;

  if($hash->{primary_key}) {
    return $hash->{primary_key};
  }
  my $pid = $self->getPid($hash);
  unless($pid){
    $self->skipRow($hash);
    return;
  }
  my $int = $self->getIntervalName($hash);
  return join("_", $pid, $int);
}

sub getPrimaryKeyPrefix {
  my ($self, $hash) = @_;
  if($hash->{primary_key}) {
    return "";
  }
  return "o";
}

sub cleanAndAddDerivedData {
  my ($self, $hash) = @_;
  if(defined($hash->{primary_key})){ return; }
  my $int = $self->getIntervalName($hash);
  $hash->{intervalname} ||= $self->getIntervalName($hash);
  $self->SUPER::cleanAndAddDerivedData($hash);
}
1;

package ClinEpiData::Load::WestAfricaReader::SampleReader;
use base qw(ClinEpiData::Load::WestAfricaReader);
use Switch;

sub makeParent {
  my ($self, $hash) = @_;
  if($hash->{primary_key}) {
    return $hash->{primary_key};
  }
	return $self->makePrimaryKey($hash);
}

sub makePrimaryKey {
  my ($self, $hash) = @_;

  if($hash->{primary_key}) {
    return $hash->{primary_key};
  }
  my $int = $self->getIntervalName($hash);
  die "No intervalname\n" . Dumper $hash unless $int;
  return join("_", $self->getPid($hash), $int);
}

sub getParentPrefix {
  return "o";
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
  if(defined($hash->{primary_key})){ return; }
  $self->SUPER::cleanAndAddDerivedData($hash);
}
1;
