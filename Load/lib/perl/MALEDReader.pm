package ClinEpiData::Load::MALEDReader;
use base qw(ClinEpiData::Load::MetadataReader);

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

sub cleanAndAddDerivedData {
  my ($self, $hash) = @_;
  $self->SUPER::cleanAndAddDerivedData($hash);
	if(!defined($hash->{gender}) || $hash->{gender} eq ""){
		$hash->{gender} = $hash->{cafsex};
	}
}

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
use Data::Dumper;

sub makeParent {
  my ($self, $hash) = @_;
  if($hash->{parent}) {
    return $hash->{parent};
  }
  ## build the Observation primary key
  unless(defined($hash->{agedays}) || defined($hash->{age})){
  	my $mdfile = $self->getMetadataFile();
    print STDERR "$mdfile: Cannot make parent: agedays/age not defined\n" . Dumper $hash;
    exit;
  }
  my $age = defined($hash->{agedays}) ? $hash->{agedays} : $hash->{age};
  return join("_", $hash->{pid}, $age);
}

sub makePrimaryKey {
  my ($self, $hash) = @_;
  if($hash->{"primary_key"}) {
    return $hash->{"primary_key"};
  }
  my $mdfile = $self->getMetadataFile();
	if($mdfile =~ /micro_x/){
  	return $hash->{srfmbsampid};
	}
	elsif($mdfile =~ /illnessfull/){
		return sprintf("%s%04dS", $hash->{pid}, $hash->{age});
	}
	elsif($mdfile =~ /mn_blood_iar/){
		return sprintf("%s%04d%s", $hash->{pid}, $hash->{agedays}, substr($hash->{sampletype},0,1));
	}
	elsif($mdfile =~ /mpo_neo_ala/){
		return $hash->{srfmbsampid};
	}
	elsif($mdfile =~ /MAL-ED.TAC.data/){
		return $self->makeParent($hash) . "T";
	}
	die "$mdfile not recongnized, cannot make primary key\n";
}

sub cleanAndAddDerivedData {
  my ($self, $hash) = @_;
  $self->SUPER::cleanAndAddDerivedData($hash);
  my $mdfile = $self->getMetadataFile();
	if($mdfile =~ /mpo_neo_ala/){
		$hash->{srfdate} =~ tr/\///d;
	}
	if($mdfile =~ /micro_x|mpo_neo_ala/){
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
	if($mdfile =~ /TAC.data/){
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
	if($mdfile =~ /mn_blood_iar|mpo_neo_ala/){
		$hash->{sample_target_month} = $hash->{target_month};
	 	delete $hash->{target_month};
	}
	for my $col (qw/srfconsist srfstblood/){
		delete($hash->{$col}) if(defined($hash->{$col}) && $hash->{$col} =~ /NA/i);
	}
	for my $col (qw/date datecol srfdate ucfdate/ ){
		if(defined($hash->{$col} && $hash->{$col} ne "")){
			$hash->{sample_collection_date} = $hash->{$col};
		}
	}
	if(defined($hash->{window}) && $mdfile =~ /mn_blood_iar/) { # sampletype = blood or urine
		my $newcol = join("_", $hash->{sampletype}, "window");
		$hash->{$newcol} = $hash->{window};
		delete $hash->{window};
	}
}

1;

package ClinEpiData::Load::MALEDReader::ObservationReader;
use base qw(ClinEpiData::Load::MALEDReader);
use strict;
use warnings;
use Data::Dumper;
use Switch;
use File::Basename;

my @obs_date_cols = (
"bcgdate1", "bcgdate2", "cpoxdate1", "dewormdate1", "dewormdate2", "dptdate1", "dptdate2", "dptdate3", "dptdate4", "dptdate5", "fludate1", "fludate2",
"fludate3", "fludate4", "fludate5", "frqdate", "fsedate", "fsqdate", "hepbdate1", "hepbdate2", "hepbdate3", "hepbdate4", "hepbdate5", "hibdate1",
"hibdate2", "hibdate3", "hibdate4", "hibdate5", "ipvdate1", "ipvdate2", "ipvdate3", "ipvdate4", "ipvdate5", "jedate1", "meadate1", "meadate2",
"meadate3", "meadate4", "mendate1", "mendate2", "mendate3", "mmrdate1", "mmrdate2", "mmrdate3", "moadate", "mumdate1", "mumdate2", "mumdate3",
"opvdate1", "opvdate10", "opvdate11", "opvdate12", "opvdate13", "opvdate14", "opvdate15", "opvdate16", "opvdate17", "opvdate18", "opvdate19", "opvdate2",
"opvdate20", "opvdate21", "opvdate22", "opvdate23", "opvdate24", "opvdate25", "opvdate3", "opvdate4", "opvdate5", "opvdate6", "opvdate7", "opvdate8",
"opvdate9", "pcvdate1", "pcvdate2", "pcvdate3", "pcvdate4", "rabdate1", "rabdate2", "rabdate3", "rabdate4", "rabdate5", "rotadate1", "rotadate2",
"rotadate3", "rubdate1", "rubdate2", "rubdate3", "tetadate1", "tetadate2", "tetadate3", "tetadate4", "tetadate5", "tetadate6", "tetadate7", "tetadate8",
"tetadate9", "typdate1", "vitadate1", "vitadate2", "vitadate3", "vitadate4", "vitadate5", "vitadate6", "vitadate7", "yfdate1");
my @vac_date_cols = (
"bcgdate1", "bcgdate2", "cpoxdate1", "dptdate1", "dptdate2", "dptdate3", "dptdate4", "dptdate5", "fludate1", "fludate2",
"fludate3", "fludate4", "fludate5", "hepbdate1", "hepbdate2", "hepbdate3", "hepbdate4", "hepbdate5", "hibdate1",
"hibdate2", "hibdate3", "hibdate4", "hibdate5", "ipvdate1", "ipvdate2", "ipvdate3", "ipvdate4", "ipvdate5", "jedate1", "meadate1", "meadate2",
"meadate3", "meadate4", "mendate1", "mendate2", "mendate3", "mmrdate1", "mmrdate2", "mmrdate3", "mumdate1", "mumdate2", "mumdate3",
"opvdate1", "opvdate10", "opvdate11", "opvdate12", "opvdate13", "opvdate14", "opvdate15", "opvdate16", "opvdate17", "opvdate18", "opvdate19", "opvdate2",
"opvdate20", "opvdate21", "opvdate22", "opvdate23", "opvdate24", "opvdate25", "opvdate3", "opvdate4", "opvdate5", "opvdate6", "opvdate7", "opvdate8",
"opvdate9", "pcvdate1", "pcvdate2", "pcvdate3", "pcvdate4", "rabdate1", "rabdate2", "rabdate3", "rabdate4", "rabdate5", "rotadate1", "rotadate2",
"rotadate3", "rubdate1", "rubdate2", "rubdate3", "tetadate1", "tetadate2", "tetadate3", "tetadate4", "tetadate5", "tetadate6", "tetadate7", "tetadate8",
"tetadate9", "typdate1", "yfdate1");

sub normalizeDate {
	my ($self, $val) = @_;
	$val =~ s/^(\d+)[-\/]?([a-z]{3})[-\/]?(\d+)$/$1-$2-$3/;
	$val =~ s/-(\d\d)$/-20$1/; 
	$val =~ s/^(\d)-/0$1-/;
	return $val;
}

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
  	my $mdfile = $self->getMetadataFile();
    print STDERR "$mdfile: Cannot make parent: agedays/age not defined\n" . Dumper $hash;
    exit;
  }
  my $age = defined($hash->{agedays}) ? $hash->{agedays} : $hash->{age};

  return sprintf("%s_%d", $hash->{pid}, $age);
}


sub cleanAndAddDerivedData {
  my ($self, $hash) = @_;
  $self->SUPER::cleanAndAddDerivedData($hash);
  my $mdFile = basename($self->getMetadataFile());
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
	if($mdFile =~ /illnessfull/i){
		if($hash->{dprev3} ||  $hash->{safcough} ||  $hash->{saffev} ||  $hash->{safvom}){
			$hash->{ill} = 1;
		}
		else { $hash->{ill} = 0; }
		if(defined($hash->{visit})){
			$hash->{ill_visit} = $hash->{visit};
			delete $hash->{visit};
		}
	}
	for my $type (qw/who diet/){
		if($mdFile =~ /$type/i){
			$hash->{"${type}_target_month"} = $hash->{target_month};
		}
	}
	for my $col (@obs_date_cols){
		if(defined($hash->{$col})){
			$hash->{observation_date} = $self->normalizeDate($hash->{$col});
		}
	}
	for my $col (@vac_date_cols){
		if(defined($hash->{$col})){
			$hash->{vaccination_date} = $self->normalizeDate($hash->{$col});
		}
	}
	if($mdFile =~ /zscores/i && defined($hash->{date})){
		$hash->{anthro_date} = $hash->{date};
	}
	if(defined($hash->{window}) && $hash->{window} =~ /^\d$/){
		switch(lc($mdFile)) {
			case /fsq/ { $hash->{fsq_window} = $hash->{window}; }
			case /diet/ { $hash->{diet_window} = $hash->{window}; }
			case /whocore/ { $hash->{who_window} = $hash->{window}; }
			case /zscores/ { $hash->{zscores_window} = $hash->{window}; }
		}
		delete $hash->{window};
	}
}

1;

package ClinEpiData::Load::MALEDReader::HouseholdObservationReader;
use base qw(ClinEpiData::Load::MALEDReader::ObservationReader);

sub cleanAndAddDerivedData {
  my ($self, $hash) = @_;
  $self->SUPER::cleanAndAddDerivedData($hash);
  my $mdFile = $self->getMetadataFile();
	for my $type (qw/fsq wami/){
		if($mdFile =~ /$type/i){
			$hash->{"${type}_target_month"} = $hash->{target_month};
		}
	}
}

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
