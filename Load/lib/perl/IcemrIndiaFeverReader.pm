package ClinEpiData::Load::IcemrIndiaFeverReader;
use base qw(ClinEpiData::Load::MetadataReader);

#sub cleanAndAddDerivedData {
#  my ($self, $hash) = @_;
#}

sub getId {
  my ($self, $hash) = @_;
	for my $idcol (qw/sid igm_chik_sid1 igm_typhus_sid1 mic_sid1 ns1_dengue_sid1 pcr_sid1 rdt_sid1/){
		return $hash->{$idcol} if defined $hash->{$idcol};
	}
}
1;


package ClinEpiData::Load::IcemrIndiaFeverReader::ParticipantReader;
use base qw(ClinEpiData::Load::IcemrIndiaFeverReader);
# sub cleanAndAddDerivedData {
#   my ($self, $hash) = @_;
#   if(defined($hash->{pastyear_treatdate})){
# 		$hash->{pastyear_treat_date} = $hash->{pastyear_treatdate};
# 		delete $hash->{pastyear_treatdate};
# 	}
#   if(defined($hash->{pastyear_treat_date})){
#     my $val = $hash->{pastyear_treat_date};
#     $val =~ tr/_/ /; ## some dates look like mmm_YYYY
#     $val =~ s/^(\d\d)-([a-zA-Z]{3})$/$2 20$1/; ## some dates look like YY-mmm
#     $val =~ s/^([a-zA-Z]{3})-(\d\d)$/$1 20$2/; ## some dates look like mmm-YY
#     $val =~ s/^\W+(\d+)\W+$/1-1-$1/; ## just a year
#     $val =~ s/^(\d{1,2})-(\d+)$/1-$1-$2/; ## mm-yyyy
#     $hash->{pastyear_treat_date} = $val;
#   }
# }

sub makeParent {
#  my ($self, $hash) = @_;
#	return $hash->{parent} if defined $hash->{parent};
#  return $self->getId($hash);
}
# sub getParentPrefix {
#   my ($self, $hash) = @_;
# 	return undef if defined $hash->{parent};
# 	return "HH";
# }
sub makePrimaryKey {
  my ($self, $hash) = @_;

  if($hash->{primary_key}) {
    return $hash->{primary_key};
  }

  return $self->getId($hash);

}

1;

#package ClinEpiData::Load::IcemrIndiaFeverReader::HouseholdReader;
#use base qw(ClinEpiData::Load::IcemrIndiaFeverReader::ParticipantReader);
#sub makeParent {
#	return undef;
#}
#
#sub getParentPrefix {
#	return undef;
#}
#
#sub getPrimaryKeyPrefix {
#  my ($self, $hash) = @_;
#	return undef if defined $hash->{parent};
#	return "HH";
#}
#
#1;

package ClinEpiData::Load::IcemrIndiaFeverReader::ObservationReader;
use base qw(ClinEpiData::Load::IcemrIndiaFeverReader);
## This object is for census data
use Data::Dumper;


sub cleanAndAddDerivedData {
  my ($self, $hash) = @_;
	if(defined($hash->{bp})){
		($hash->{systolic_bp},$hash->{diastolic_bp}) = split(/\//, $hash->{bp});
		delete($hash->{bp});
	}
}

sub makeParent {
  ## returns a Participant ID
  my ($self, $hash) = @_;
  if($hash->{"parent"}) {
    return $hash->{"parent"};
  }
  return $self->getId($hash);
}

sub makePrimaryKey {
  my ($self, $hash) = @_;
  if($hash->{"primary_key"}) {
    return $hash->{"primary_key"};
  }
  my $id = $self->getId($hash);
#	my $file = basename($self->getMetadataFile());
#	my $field = $datefield{$file};
#	die "No date field for $file" unless $field;
#  return join("_", $id, $field);
	my $event = substr($hash->{redcap_event_name},0,3);
	return join("_", $id, $event);
}

1;
#package ClinEpiData::Load::IcemrIndiaFeverReader::FollowupReader;
#use base qw(ClinEpiData::Load::IcemrIndiaFeverReader::ObservationReader);
#sub cleanAndAddDerivedData {
#  my ($self, $hash) = @_;
#	if($hash->{redcap_event_name} eq 'enrollment_arm_1'){
#		delete $hash->{$_} for keys %$hash;
#	}
#}
#sub makeParent {
#  ## returns a Participant ID
#  my ($self, $hash) = @_;
#  if($hash->{"parent"}) {
#    return $hash->{"parent"};
#  }
#  my $id = $self->getId($hash);
#	return join("_", $id, 'enrollment_arm_1');
#}
#
#1;

package ClinEpiData::Load::IcemrIndiaFeverReader::SampleReader;
use base qw(ClinEpiData::Load::IcemrIndiaFeverReader);
use File::Basename;
my %source = (
'IGHFeverCRF_DATA_2018-09-12_1813.csv' => 'crfdata',
'IGHFeverCRF_DATA_2018-09-12_1813.txt' => 'crfdata',
'IGHFeverELISAChikung_DATA_2018-09-12_1817.csv' => 'elisachikung',
'IGHFeverELISADengueI_DATA_2018-09-12_1819.csv' => 'elisadenguei',
'IGHFeverELISADengueN_DATA_2018-09-12_1819.csv' => 'elisadenguen',
'IGHFeverELISAScrubty_DATA_2018-09-12_1818.csv' => 'elisyscrubty',
'IGHFeverMxMalaria_DATA_2018-09-12_1821.csv' => 'mxmalaria',
'IGHFeverPCRMalaria_DATA_2018-11-14_2100.csv' => 'pcrmalaria',
'IGHFeverPCRSTyphi_DATA_2018-09-12_1812.csv' => 'pcrstyphi',
'IGHFeverRDTDengueChi_DATA_2018-09-12_1816.csv' => 'rdtdenguechi',
);
sub cleanAndAddDerivedData {
  my ($self, $hash) = @_;
	my $mdfile = basename($self->getMetadataFile());
	if($mdfile =~ /IGHFeverMxMalaria_DATA/){
##			mic_counts1 needs to be split into 4 different variables:
##			If the value for mic_species___2 is 1, 
##			then the number before the “/” maps to “Plasmodium falciparum asexual stage density (per uL blood), by microscopy” EUPATH_0000550 pfal_a
##			and the number after the “/” maps to “Plasmodium falciparum gametocyte density (per uL blood), by microscopy” EUPATH_0000546 pfal_g
##			If the value for mic_species___3 is 1,
##			then the number before the “/” maps to “Plasmodium vivax asexual stage density (per uL blood), by microscopy” EUPATH_0000551 pviv_a
##			and the number after the “/” maps to “Plasmodium vivax gametocyte density (per uL blood), by microscopy” EUPATH_0000547 pviv_g

		my @mc1 = split(/\//, $hash->{mic_counts1});
		if($hash->{mic_species___2}){
			($hash->{pfal_a},$hash->{pfal_g}) = @mc1;
		}
		elsif($hash->{mic_species___3}){
			($hash->{pviv_a},$hash->{pviv_g}) = @mc1;
		}
	}
}
sub makeParent {
  ## returns a Participant ID
  my ($self, $hash) = @_;
  if($hash->{"parent"}) {
    return $hash->{"parent"};
  }
  my $id = $self->getId($hash);
	my $event = substr($hash->{redcap_event_name} || 'enrollment_arm_1', 0, 3);
	return join("_", $id, $event);
}

sub makePrimaryKey {
  ## returns a Participant ID
  my ($self, $hash) = @_;
  if($hash->{"primary_key"}) {
    return $hash->{"primary_key"};
  }
  my $id = $self->getId($hash);
	#my $field = $datefield{basename($self->getMetadataFile())};
 	my $event = substr($hash->{redcap_event_name},0,3);
#	return join("_", $id, $event) ;
	my $src = $source{basename($self->getMetadataFile())};
	return join("_", $id, $src, $event);
}

1;

#package ClinEpiData::Load::IcemrIndiaFeverReader::FollowupSampleReader;
#use base qw(ClinEpiData::Load::IcemrIndiaFeverReader::SampleReader);
#sub cleanAndAddDerivedData {
#  my ($self, $hash) = @_;
#	unless(defined($hash->{redcap_event_name}) && $hash->{redcap_event_name}){
#		$hash->{redcap_event_name} = 'enrollment_arm_1';
#	}
#	if($hash->{redcap_event_name} eq 'enrollment_arm_1'){
#		delete $hash->{$_} for keys %$hash;
#	}
#}
#
#sub makePrimaryKey {
#  ## returns a Participant ID
#  my ($self, $hash) = @_;
#  if($hash->{"primary_key"}) {
#    return $hash->{"primary_key"};
#  }
#  my $id = $self->getId($hash);
#	#my $field = $datefield{basename($self->getMetadataFile())};
#	my $event = $hash->{redcap_event_name};
#	return join("_", $id, $event, "S") ;
#}
#
#1;
