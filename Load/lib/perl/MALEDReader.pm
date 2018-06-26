package ClinEpiData::Load::MALEDReader;
use base qw(ClinEpiData::Load::MetadataReader);

sub cleanAndAddDerivedData {
  my ($self, $hash) = @_;
  while( my ($key, $val) = each %$hash){
    delete $hash->{$key} if $val eq ".";
    delete $hash->{$key} if $val eq "";
  }
  if(defined($hash->{srfdate})){
    $hash->{srfdate} =~ s/:00:00:00$//;
  }
}
1;

package ClinEpiData::Load::MALEDReader::ParticipantReader;
use base qw(ClinEpiData::Load::MALEDReader);

sub cleanAndAddDerivedData {
  my ($self, $hash) = @_;
  $self->SUPER::cleanAndAddDerivedData($hash);
	if(defined($hash->{cafsex}) && !defined($hash->{gender})){
		$hash->{gender} = $hash->{cafsex};
		delete $hash->{cafsex};
	}
	delete $hash->{cafsex};
}

sub makeParent {
  return undef; 
}
sub makePrimaryKey {
  my ($self, $hash) = @_;

  if($hash->{"primary_key"}) {
    return $hash->{"primary_key"};
  }

  return $hash->{pid};
}

1;

package ClinEpiData::Load::MALEDReader::StoolSampleReader;
use base qw(ClinEpiData::Load::MALEDReader);
# use Data::Dumper;
## loads file micro_x24m.csv
use strict;
use warnings;

sub makeParent {
  ## build the Observation primary key
  my ($self, $hash) = @_;
  return join("_", $hash->{pid}, $hash->{agedays});
}

sub makePrimaryKey {
  my ($self, $hash) = @_;
  return $hash->{srfmbsampid};
}

sub cleanAndAddDerivedData {
  my ($self, $hash) = @_;
  $self->SUPER::cleanAndAddDerivedData($hash);
}
1;

package ClinEpiData::Load::MALEDReader::ObservationReader;
use base qw(ClinEpiData::Load::MALEDReader);
# use Data::Dumper;
## loads files illnessfull_24m.csv, Zscores_24m.csv, micro_24m.csv

sub makeParent {
  ## returns a Participant ID
  my ($self, $hash) = @_;
  if($hash->{"parent"}) {
    return $hash->{"parent"};
  }
  return $hash->{pid}; 
}

sub makePrimaryKey {
  my ($self, $hash) = @_;
  if($hash->{"primary_key"}) {
    return $hash->{"primary_key"};
  }
  unless(defined($hash->{agedays}) || defined($hash->{age})){
    print STDERR "Cannot make primary key: agedays/age not defined\n";# . Dumper $hash;
    exit;
  }
  my $age = defined($hash->{agedays}) ? $hash->{agedays} : $hash->{age};

  return sprintf("%s_%d", $hash->{pid}, $age);
}

sub cleanAndAddDerivedData {
  my ($self, $hash) = @_;
  $self->SUPER::cleanAndAddDerivedData($hash);
  if(defined($hash->{"form"}) && $hash->{"form"} eq "CAF-BW"){
    delete($hash->{"zwei"});
    delete($hash->{"weight"});
  }
  unless(defined($hash->{"agedays"}) && $hash->{"agedays"} ne ""){
    if(defined($hash->{"age"}) && $hash->{"age"} ne ""){
      $hash->{"agedays"} = $hash->{"age"};
    }
    else {
      my ($pid, $age) = split(/_/, $hash->{primary_key});
      if(defined($age)){
        $hash->{"agedays"} = $age;
      }
    }
    #die Dumper($hash) unless defined $hash->{"agedays"};
    die "agedays value missing\n" unless defined $hash->{"agedays"};
  }
  if(defined($hash->{"agemonths"}) && $hash->{"agemonths"} =~ /\./){
    my $intval = int($hash->{"agemonths"});
    my $frac = $hash->{"agemonths"} - $intval;
    my $val = $intval + $frac;
    $hash->{"agemonths"} = $val;
  }
}

1;
