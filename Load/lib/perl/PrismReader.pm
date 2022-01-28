package ClinEpiData::Load::PrismReader;
use base qw(ClinEpiData::Load::MetadataReader);

1;

package ClinEpiData::Load::PrismReader::HouseholdReader;
use base qw(ClinEpiData::Load::PrismReader);

use strict;

sub makeParent {
  return undef;
}

sub makePrimaryKey {
  my ($self, $hash) = @_;

  if($hash->{"primary_key"}) {
    return $hash->{"primary_key"};
  }

  return $hash->{hhid};
}

sub getPrimaryKeyPrefix {
  my ($self, $hash) = @_;

  unless($hash->{"primary_key"}) {
    return "h_";
  }
  return "";
}

sub cleanAndAddDerivedData {
  my ($self, $hash) = @_;
  $hash->{country} ||= "Uganda";
}

1;

package ClinEpiData::Load::PrismReader::ParticipantReader;
use base qw(ClinEpiData::Load::PrismReader);
use POSIX;

use strict;

sub makeParent {
  my ($self, $hash) = @_;

  return $hash->{hhid};
}

sub makePrimaryKey {
  my ($self, $hash) = @_;

  if($hash->{"primary_key"}) {
    return $hash->{"primary_key"};
  }

  return $hash->{id};
}

sub getParentPrefix {
  my ($self, $hash) = @_;

  return "h_";
}
sub cleanAndAddDerivedData {
  my ($self, $hash) = @_;
	
	# deal with DD-Mmm-YY ... we have to assume no participants born 100 years ago
	foreach my $field ( qw/lastdate enrolldate dob/ ){
		next unless defined($hash->{$field});
	  if($hash->{$field} =~ /^\d{1,2}-\w{3}-\d\d$/) {
			my ($day,$month,$year) = split(/-/, $hash->{$field});
			my ($cent, $dec) = split(/:/, strftime("%C:%y", localtime));
			if($year > $dec){ $cent--; }
			if(int($day) < 10){ $day = "0$day"; }
			$hash->{$field} = join("", $day,lc($month),$cent,$year);
		}
	}
	foreach my $field ( qw/age ageyrs/ ){
		if(defined($hash->{$field})){
			$hash->{$field} =~ s/^\./0./;
		}
	}
}


1;

package ClinEpiData::Load::PrismReader::ClinicalVisitReader;
use base qw(ClinEpiData::Load::PrismReader);
use File::Basename;

use strict;

sub skipIfNoParent { return 0; }

sub makeParent {
  my ($self, $hash) = @_;

  if($hash->{parent}) {
    return $hash->{parent};
  }
	unless(defined($hash->{id})){
		$hash->{id} = substr( $hash->{uniqueid}, 0, 4 );
	}
  return $hash->{id};
}

sub makePrimaryKey {
  my ($self, $hash) = @_;

  if($hash->{"primary_key"}) {
    return $hash->{"primary_key"};
  }

  return $hash->{uniqueid};
}
sub getPrimaryKeyPrefix {
  my ($self, $hash) = @_;

  unless($hash->{"primary_key"}) {
    return "o_";
  }
  return "";
}


sub cleanAndAddDerivedData {
  my ($self, $hash) = @_;

# my $mdfile = basename($self->getMetadataFile());
# my $lamp = $hash->{lamp};
# my $mcat = $hash->{malariacat};
# if($mdfile =~ /updated_lamp_results/){
# 	my %vmap = (
# 		1 => 'Blood smear not indicated',
# 		2 => 'Blood smear indicated but not done',
# 		3 => 'Blood smear negative / LAMP negative',
# 		4 => 'Blood smear negative / LAMP not done',
# 		5 => 'Blood smear negative / LAMP positive',
# 		6 => 'Blood smear positive / no malaria',
# 		7 => 'Symptomatic malaria',
# 	);
# 	if(defined($vmap{$mcat})){ $mcat = $vmap{$mcat};}
# }

# if($mcat eq 'negative blood smear') {
#   if(($lamp eq 'positive') || ($lamp eq '1')) {
#     $mcat = 'Blood smear negative / LAMP positive';    
#   }
#   elsif(($lamp eq 'negative') || ($lamp eq '0')) {
#     $mcat = 'Blood smear negative / LAMP negative';    
#   }
#   else {
#     $mcat = 'Blood smear negative / LAMP not done';
#   }
# }
# elsif($mcat eq 'malaria') {
# 	$mcat = 'Symptomatic malaria';
# }
# elsif($mcat eq 'asymptomatic parasitemia') {
# 	$mcat = 'Blood smear positive / no malaria';
# }
# elsif($mcat eq 'blood smear not indicated') {
# 	$mcat = 'Blood smear not indicated';
# }
# elsif($mcat eq 'blood smear should have been done') {
# 	$mcat = 'Blood smear indicated but not done';
# }
# $hash->{malariacat} = $mcat;


  my @symptomsAndSigns = (['abdominalpain', 'apainduration'],
                          ['anorexia', 'aduration'],
                          ['cough', 'cduration'],
                          ['diarrhea', 'dduration'],
                          ['fatigue', 'fmduration'],
                          ['fever', 'fduration'],
                          ['headache', 'hduration'],
                          ['jaundice', 'jduration'],
                          ['jointpains', 'djointpains'],
                          ['muscleaches', 'mduration'],
                          ['seizure', 'sduration'],
                          ['vomiting', 'vduration']
      );


  foreach my $ar(@symptomsAndSigns) {
    my $ss = $ar->[0];
    my $dur = $ar->[1];

    $hash->{$dur} = '0' if($hash->{$ss} eq '0' || lc($hash->{$ss}) eq 'no');
  }


  if($hash->{anymalaria} != 1) {
    $hash->{complicatedmalaria} = undef;
  }

  if($hash->{complicatedmalaria} != 1) {
    $hash->{cmcategory} = undef;
  }

 #foreach my $key (keys %$hash) {
 #  if($key =~ /^med\d*code$/) {

 #    # these 3 are the malaria ones
 #    if($hash->{$key} eq '40' || $hash->{$key} eq '41' || $hash->{$key} eq '50') {

 #      my $newKey = $key . "_malaria";

 #      $hash->{$newKey} = $hash->{$key};

 #      delete $hash->{$key};
 #    }
 #  }
 #}



}


1;

package ClinEpiData::Load::PrismReader::SampleReader;
use base qw(ClinEpiData::Load::PrismReader);

use strict;

use ClinEpiData::Load::MetadataReader;

# use Date::Manip qw(Date_Init ParseDate UnixDate);

use File::Basename;


sub skipIfNoParent { return 1; }

sub getClinicalVisitMapper { $_[0]->{_clinical_visit_mapper} }
sub setClinicalVisitMapper { $_[0]->{_clinical_visit_mapper} = $_[1] }

sub new {
  my ($class, $metadataFile, $rowExcludes, $colExcludes, $clinicalVisitsParsedOutput, $clinicalVisitMapper) = @_;

  my $self = bless {}, $class;

  $self->setMetadataFile($metadataFile);
  $self->setRowExcludes($rowExcludes);
  $self->setColExcludes($colExcludes);
  $self->setParentParsedOutput($clinicalVisitsParsedOutput);

  unless($clinicalVisitMapper) {
    foreach my $uniqueid (keys %$clinicalVisitsParsedOutput) {

      # not sure why can't just grab the primarykey here??
#    my $clinicalVisitPrimaryKey = $clinicalVisitsParsedOutput->{$uniqueid}->{'primary_key'} 
      my $participant = $clinicalVisitsParsedOutput->{$uniqueid}->{'__PARENT__'}; # the parent of the clinical visit is the participant
      my $date = $clinicalVisitsParsedOutput->{$uniqueid}->{'date'};
      my $admitdate = $clinicalVisitsParsedOutput->{$uniqueid}->{'admitdate'};
      my $dischargedate = $clinicalVisitsParsedOutput->{$uniqueid}->{'dischargedate'};

      my $hasDate;

      if($date) {
        my $formattedDate = $self->formatDate($date);
        my $key = "$participant.$formattedDate";

        $clinicalVisitMapper->{$key} = $uniqueid;
      }

      if($admitdate) {
        my $formattedDate = $self->formatDate($admitdate);
        my $key = "$participant.$formattedDate";
        $clinicalVisitMapper->{$key} = $uniqueid;
      }

      if($dischargedate) {
        my $formattedDate = $self->formatDate($dischargedate);
        my $key = "$participant.$formattedDate";
        $clinicalVisitMapper->{$key} = $uniqueid;
      }

    }
  }

  $self->setClinicalVisitMapper($clinicalVisitMapper);

  return $self;
}


sub read {
  my ($self) = @_;

  my $metadataFile = $self->getMetadataFile();
  #my $baseMetaDataFile = basename $metadataFile;
  my $mdfile = $self->getMetadataFileLCB;

  #if(($baseMetaDataFile eq "Prism_samples.txt" || $baseMetaDataFile eq "Prism_samples_update.txt") && (ref($self) eq "ClinEpiData::Load::PrismReader::SampleReader")) {
  if(($mdfile =~ /prism_samples/) && (ref($self) eq "ClinEpiData::Load::PrismReader::SampleReader")) {
    my $colExcludes = $self->getColExcludes();
    my $rowExcludes = $self->getRowExcludes();
    my $parentParsedOutput = $self->getParentParsedOutput();
    my $clinicalVisitMapper = $self->getClinicalVisitMapper();

    my $fp = ClinEpiData::Load::PrismReader::SampleReader::FP->new($metadataFile, $rowExcludes, $colExcludes, $parentParsedOutput, $clinicalVisitMapper);
    $fp->read();
    $fp->addSpecimenType();

    my $bc = ClinEpiData::Load::PrismReader::SampleReader::BC->new($metadataFile, $rowExcludes, $colExcludes, $parentParsedOutput, $clinicalVisitMapper);
    $bc->read();
    $bc->addSpecimenType();

    my $p1 = ClinEpiData::Load::PrismReader::SampleReader::P1->new($metadataFile, $rowExcludes, $colExcludes, $parentParsedOutput, $clinicalVisitMapper);
    $p1->read();
    $p1->addSpecimenType();

    my $p2 = ClinEpiData::Load::PrismReader::SampleReader::P2->new($metadataFile, $rowExcludes, $colExcludes, $parentParsedOutput, $clinicalVisitMapper);
    $p2->read();
    $p2->addSpecimenType();

    $self->setNestedReaders([$fp, $bc, $p1, $p2]);
  }

  # this will handle tororo file && each call above
  else{
    $self->SUPER::read();
  }

}

sub addSpecimenType {
  my ($self) = @_;

  my $parsedOutput = $self->getParsedOutput();

  my $types = {"3" => "Plasma",
               "4" => "Filter Paper",
               "5" => "Pellet",
               "6" => "Buffy Coat",
  };


  foreach my $pk (keys %$parsedOutput) {

    if($pk =~ /\w\w(\d)-/i) {
      my $type = $types->{$1};

      if($type) {
        $parsedOutput->{$pk}->{specimentype} = $type;
      }
      else {
        die "No Type for sample $pk\n";
      }
    }
  }
}


sub makeParent {
  my ($self, $hash) = @_;

  my $mapper = $self->getClinicalVisitMapper();

  my $metadataFile = $self->getMetadataFile();
  my $baseMetaDataFile = basename $metadataFile;

  my $date;
  if($baseMetaDataFile eq "Prism_tororo.txt" || $baseMetaDataFile eq "Prism_tororo_update.txt") {
    $date = $hash->{date};
  }
  elsif($baseMetaDataFile eq "Prism_samples.txt" || $baseMetaDataFile eq "Prism_samples_update.txt") {
    $date = $hash->{reqdate};
  }
  else {
    die "File $baseMetaDataFile not handled for makeParent Method";
  }

  my $participant = $hash->{subjectid};
  if($date) {
    my $formattedDate = $self->formatDate($date);
    
    my $key = "$participant.$formattedDate";
		unless($mapper->{$key}){
			warn "No mapped parent for $key ($date)";
		}
    return $mapper->{$key};
  }

  my $primaryKey = $self->makePrimaryKey($hash);
  die "No Date found for Sample $primaryKey (Participant ID=$participant)\n";

}
sub getParentPrefix {
  my ($self, $hash) = @_;

  return "o_";
}

# Default is for the tororo file
sub makePrimaryKey {
  my ($self, $hash) = @_;

  my $metadataFile = $self->getMetadataFile();
  my $baseMetaDataFile = basename $metadataFile;
  
  if($hash->{randomnumber}) {
    return $hash->{subjectid}  . $hash->{randomnumber};
  }
  return undef;
}
sub getPrimaryKeyPrefix {
  my ($self, $hash) = @_;

  return "s_";
}


1;


package ClinEpiData::Load::PrismReader::LightTrapReader;
use base qw(ClinEpiData::Load::PrismReader);

use strict;

sub cleanAndAddDerivedData {
  my ($self, $hash) = @_;

  if($hash->{date}) {
    $hash->{collectiondate} = $hash->{date};
  }
  else {
    $hash->{collectiondate} = $hash->{collectionmonthyear};
  }
}


sub makePrimaryKey {
  my ($self, $hash) = @_;

  return $hash->{uniqueid};
}


sub makeParent {
  my ($self, $hash) = @_;

  return $hash->{hhid};
}

sub getParentPrefix {
  my ($self, $hash) = @_;

  return "h_";
}

1;


package ClinEpiData::Load::PrismReader::SampleReader::FP;
use base qw(ClinEpiData::Load::PrismReader::SampleReader);

use strict;

sub makePrimaryKey {
  my ($self, $hash) = @_;
  
  if($hash->{fp_barcode} && $hash->{fp_barcode} !~ /^SKIP/i ) {
    return $hash->{subjectid}  . $hash->{fp_barcode};
  }
  return undef;
}

1;

package ClinEpiData::Load::PrismReader::SampleReader::BC;
use base qw(ClinEpiData::Load::PrismReader::SampleReader);

use strict;

sub makePrimaryKey {
  my ($self, $hash) = @_;

  if($hash->{bc_barcode} && $hash->{bc_barcode} !~ /^SKIP/i ) {
    return $hash->{subjectid}  . $hash->{bc_barcode};
  }
  return undef;
}

1;

package ClinEpiData::Load::PrismReader::SampleReader::P1;
use base qw(ClinEpiData::Load::PrismReader::SampleReader);

use strict;

sub makePrimaryKey {
  my ($self, $hash) = @_;

  if($hash->{p1_barcode} && $hash->{p1_barcode} !~ /^SKIP/i ) {
    return $hash->{subjectid}  . $hash->{p1_barcode};
  }
  return undef;
}

1;

package ClinEpiData::Load::PrismReader::SampleReader::P2;
use base qw(ClinEpiData::Load::PrismReader::SampleReader);

use strict;

sub makePrimaryKey {
  my ($self, $hash) = @_;

  if($hash->{p2_barcode} && $hash->{p2_barcode} !~ /^SKIP/i ) {
    return $hash->{subjectid}  . $hash->{p2_barcode};
  }
  return undef;
}

1;
