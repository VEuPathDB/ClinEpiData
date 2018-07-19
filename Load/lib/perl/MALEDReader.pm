package ClinEpiData::Load::MALEDReader;
use base qw(ClinEpiData::Load::MetadataReaderCSV);

sub cleanAndAddDerivedData {
  my ($self, $hash) = @_;
  while( my ($key, $val) = each %$hash){
    delete $hash->{$key} if $val eq ".";
    delete $hash->{$key} if $val eq "";
		$hash->{$key} =~ s/^[0]+$/0/;
		$hash->{$key} =~ s/^[0]+(\d+)$/$1/;
  }
	foreach my $df( qw/srfdate cafddob/ ){
		if(defined($hash->{$df})){
		  $hash->{$df} =~ s/:00:00:00$//;
			$hash->{$df} =~ tr/\///d;
		  $hash->{$df} =~ s/^(\d\d)(...)(\d\d)$/${1}${2}20${3}/;
		}
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

package ClinEpiData::Load::MALEDReader::SampleReader;
use base qw(ClinEpiData::Load::MALEDReader);
# use Data::Dumper;
## loads file micro_x24m.csv
use strict;
use warnings;

sub makeParent {
  my ($self, $hash) = @_;
  if($hash->{parent}) {
    return $hash->{parent};
  }
  ## build the Observation primary key
  return join("_", $hash->{pid}, $hash->{agedays});
}

sub makePrimaryKey {
  my ($self, $hash) = @_;
  if($hash->{"primary_key"}) {
    return $hash->{"primary_key"};
  }
  my $mdfile = $self->getMetadataFile();
	if($mdfile =~ /micro_x_24m/){
  	return $hash->{srfmbsampid};
	}
	elsif($mdfile =~ /mn_blood_iar_24m/){
		return sprintf("%s%04d%s", $hash->{pid}, $hash->{agedays}, substr($hash->{sampletype},0,1));
	}
	elsif($mdfile =~ /mpo_neo_ala_24m/){
		return $hash->{srfmbsampid};
	}
	die "$mdfile not recongnized, cannot make primary key\n";
}

sub cleanAndAddDerivedData {
  my ($self, $hash) = @_;
  $self->SUPER::cleanAndAddDerivedData($hash);
  my $mdfile = $self->getMetadataFile();
	if($mdfile =~ /mpo_neo_ala_24m/){
		$hash->{srfdate} =~ tr/\///d;
	}
#	if($mdfile =~ /micro_x_24m|mpo_neo_ala_24m/){
#  	if(defined($hash->{srffrstsid}) && defined($hash->{srfmbsampid}) && ($hash->{srffrstsid} eq $hash->{srfmbsampid})){
#			delete $hash->{srffrstsid};
#		}
#	}
	if($mdfile =~ /micro_x_24m|mpo_neo_ala_24m/){
		$hash->{sampletype} = 'stool';
	}
	foreach my $k (qw/bllconc iarconc hb_adj iardate brfdate agpval1 adjzinc_mml urinevol adjfar adjrar adjtfr conc.ala conc.neo conc.mpo lnconc.ala lnconc.neo lnconc.mpo/){
		if(defined($hash->{$k}) && ($hash->{$k} =~ /^na$/i || $hash->{$k} eq "")){
			delete $hash->{$k};
		}
	}
}
1;

package ClinEpiData::Load::MALEDReader::ObservationReader;
use base qw(ClinEpiData::Load::MALEDReader);
# use Data::Dumper;
## loads files illnessfull_24m.csv, Zscores_24m.csv, micro_24m.csv

sub makeParent {
  ## returns a Participant ID
  my ($self, $hash) = @_;
  if($hash->{parent}) {
    return $hash->{parent};
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
  my $metadataFile = $self->getMetadataFile();
  if(defined($hash->{form}) && $hash->{form} =~ /caf-bw/i){
		delete $hash->{$_} for keys %$hash;
		return;
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
# if(defined($hash->{"agemonths"}) && $hash->{"agemonths"} =~ /\./){
#   my $intval = int($hash->{"agemonths"});
#   my $frac = $hash->{"agemonths"} - $intval;
#   my $val = $intval + $frac;
#   $hash->{"agemonths"} = $val;
# }
	if($metadataFile =~ /illnessfull/i){
		if($hash->{dprev3} ||  $hash->{safcough} ||  $hash->{saffev} ||  $hash->{safvom}){
			$hash->{ill} = 1;
		}
		else { $hash->{ill} = 0; }
	}
}

1;
