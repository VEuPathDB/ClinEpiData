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

package ClinEpiData::Load::MALEDReader::HouseholdReader;
use base qw(ClinEpiData::Load::MALEDReader);

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

sub getPrimaryKeyPrefix {
  my ($self, $hash) = @_;

  unless($hash->{"primary_key"}) {
    return "HH";
  }
  return "";
}

1;
package ClinEpiData::Load::MALEDReader::ParticipantReader;
use base qw(ClinEpiData::Load::MALEDReader);

sub makeParent {
  my ($self, $hash) = @_;
	return $self->makePrimaryKey($hash);
}

sub getParentPrefix {
  my ($self, $hash) = @_;

  return "HH";
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
use strict;
use warnings;

sub makeParent {
  my ($self, $hash) = @_;
  if($hash->{parent}) {
    return $hash->{parent};
  }
  ## build the Observation primary key
  if(defined $hash->{age}){ # illnessfull_24m 
  	return join("_", $hash->{pid}, $hash->{age});
	}
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
	elsif($mdfile =~ /illnessfull_24m/){
		return sprintf("%s%04dS", $hash->{pid}, $hash->{age});
	}
	elsif($mdfile =~ /mn_blood_iar_24m/){
		return sprintf("%s%04d%s", $hash->{pid}, $hash->{agedays}, substr($hash->{sampletype},0,1));
	}
	elsif($mdfile =~ /mpo_neo_ala_24m/){
		return $hash->{srfmbsampid};
	}
	elsif($mdfile =~ /MAL-ED TAC data/){
		return $self->makeParent($hash) . "T";
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
	if($mdfile =~ /micro_x_24m|mpo_neo_ala_24m/){
		$hash->{sampletype} = 'stool';
		if((defined($hash->{srffrstsid}) && defined($hash->{srffrstsid})) && (($hash->{srffrstsid} eq "na") || ($hash->{srffrstsid} eq $hash->{srfmbsampid}))){
			delete $hash->{srffrstsid};
		}
	}
	foreach my $k (qw/bllconc iarconc hb_adj iardate brfdate agpval1 adjzinc_mml urinevol adjfar adjrar adjtfr conc.ala conc.neo conc.mpo lnconc.ala lnconc.neo lnconc.mpo/){
		if(defined($hash->{$k}) && ($hash->{$k} =~ /^na$/i || $hash->{$k} eq "")){
			delete $hash->{$k};
		}
	}
	if($mdfile =~ /TAC data/){
		foreach my $k (qw/aeromonas eaec hnana lt_etec salmonella stec st_etec/){
			$hash->{$k . "_tac"} = $hash->{$k};
			delete $hash->{$k};
		}
		foreach my $k ( keys %$hash ){
			if(defined($hash->{$k}) && ($hash->{$k} =~ /^na$/i || $hash->{$k} eq "")){
				delete $hash->{$k};
			}
		}
	}
	if($mdfile =~ /mn_blood_iar_24m|mpo_neo_ala_24m/){
		$hash->{sample_target_month} = $hash->{target_month};
	 	delete $hash->{target_month};
	}
}

1;

package ClinEpiData::Load::MALEDReader::ObservationReader;
use base qw(ClinEpiData::Load::MALEDReader);

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
  if($hash->{primary_key}) {
    return $hash->{primary_key};
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
    die "agedays value missing\n" unless defined $hash->{"agedays"};
  }
	if($metadataFile =~ /illnessfull/i){
		if($hash->{dprev3} ||  $hash->{safcough} ||  $hash->{saffev} ||  $hash->{safvom}){
			$hash->{ill} = 1;
		}
		else { $hash->{ill} = 0; }
	}
}

package ClinEpiData::Load::MALEDReader::HouseholdObservationReader;
use base qw(ClinEpiData::Load::MALEDReader::ObservationReader);

sub getPrimaryKeyPrefix {
  my ($self, $hash) = @_;
  unless($hash->{primary_key}) {
		return "HO";
  }
  return "";
}
sub getParentPrefix {
  my ($self, $hash) = @_;

  unless($hash->{parent}) {
    return "HH";
  }
  return "";
}

1;
