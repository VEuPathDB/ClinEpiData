package ClinEpiData::Load::IcemrSouthAsiaReader;
use base qw(ClinEpiData::Load::MetadataReaderXT);
use Data::Dumper;

sub cleanAndAddDerivedData {
  my ($self, $hash) = @_;
  $self->applyMappedValues($hash);
	for my $field ((
		'x12._temperature_reading_date',
		'x30._antimalarial_therapy_initiation_at_gmc_date',
		'x14._blood_volume_collected_.ml.',
		'x53a._dose')){
		if(defined($hash->{$field}) &&
			$hash->{$field} eq 'na'){
			delete $hash->{$field};
		}
		$hash->{$field} =~ s/_([ap])m$/$1m/;
	}
	for my $field ((
		'x15._collection_time_.24h.',
		'x31._antimalarial_therapy_initiation_at_gmc_time',
		'x11._temperature_reading_time_.24h.'
		)){
		if(defined($hash->{$field}) &&
			$hash->{$field} eq 'na'){
			delete $hash->{$field};
		}
		$hash->{$field} =~ s/_([ap])m$/$1m/;
		$hash->{$field} =~ s/^0:/12:/;
	}
}
sub makeParticipantDateKey {
  my ($self, $hash) = @_;
	my $date = $hash->{'date'};
	$date ||= $hash->{'x12._temperature_reading_date'}; 
	$date ||= $hash->{'x16._collection_date'};
	$date ||= $hash->{'x68._date_of_observation_collection'};
	$date ||= $hash->{'x30._antimalarial_therapy_initiation_at_gmc_date'};

	unless($date || $date eq 'na'){
		my $parent = $self->getParentParsedOutput()->{$hash->{participant_id}};
		$date = $parent->{'x8._date_enrolled'};
		# $date ||= $parent->{'x34._age_.at_enrollment.'};
	}
	unless($date){
		printf STDERR ("No date available: %s: %s\n",
			$self->getMetadataFile(), $hash->{participant_id});
		print STDERR Dumper $hash; die;
	}
  return join("_", $hash->{participant_id}, $date);
}
sub getPid {
  my ($self, $hash) = @_;
  if(defined($hash->{'x1._participant_id'})){
		my $pid = $hash->{'x1._participant_id'};
		if($pid !~ /^\d{7}$/){
			$pid += 1010000;
			if($pid !~ /^\d{7}$/){
				die "Cannot make pid from $pid\n";
			}
			return $pid;
		}
	}
  return $hash->{'participant_id'};
}
1;

package ClinEpiData::Load::IcemrSouthAsiaReader::HouseholdReader;
use base qw(ClinEpiData::Load::IcemrSouthAsiaReader);

sub makeParent {
	return undef;
}

sub makePrimaryKey {
  my ($self, $hash) = @_;
  if($hash->{primary_key}) {
    return $hash->{primary_key};
  }
  $self->getPid($hash);
}

sub getPrimaryKeyPrefix {
  my ($self, $hash) = @_;
  return "" if(defined($hash->{primary_key}));
  return "hh";
}

1;

package ClinEpiData::Load::IcemrSouthAsiaReader::ParticipantReader;
use base qw(ClinEpiData::Load::IcemrSouthAsiaReader);

sub cleanAndAddDerivedData {
  my ($self, $hash) = @_;
# not needed:
#  $self->SUPER::cleanAndAddDerivedData($hash);
	if(defined($hash->{'x9._time_enrolled'})){ 
		delete $hash->{'x9._time_enrolled'} if($hash->{'x9._time_enrolled'} eq 'na');
		$hash->{'x9._time_enrolled'} =~ s/^0:/12:/;
	}
	if(defined($hash->{'x8._date_enrolled'}) && $hash->{'x8._date_enrolled'} eq 'na'){ 
		delete $hash->{'x8._date_enrolled'};
	}
}

sub makeParent {
  my ($self, $hash) = @_;
  if($hash->{parent}) {
    return $hash->{parent};
  }
  my $pid = $self->getPid($hash);
  my $ppo = $self->getParentParsedOutput();
  if(defined($ppo->{"hh" . $pid})){ return $pid; }
  return "";
}

sub skipIfNoParent {
  return 1;
}

sub getParentPrefix {
  my ($self, $hash) = @_;
  return "" if(defined($hash->{parent}));
  return "hh";
}

sub makePrimaryKey {
  my ($self, $hash) = @_;
  if($hash->{primary_key}) {
    return $hash->{primary_key};
  }
  return $self->getPid($hash);
}
1;

package ClinEpiData::Load::IcemrSouthAsiaReader::ObservationReader;
use base qw(ClinEpiData::Load::IcemrSouthAsiaReader);
use strict;
use Data::Dumper;
use File::Basename;
use Date::Manip qw(Date_Init ParseDate UnixDate DateCalc);

my %remap = (
'x31._antimalarial_therapy_initiation_at_gmc_time' => 'x11._temperature_reading_time_.24h.',
'x9._time_enrolled' => 'x11._temperature_reading_time_.24h.',
'x30._antimalarial_therapy_initiation_at_gmc_date' => 'x12._temperature_reading_date',
'x8._date_enrolled' => 'x12._temperature_reading_date',
'sample_x69._time_of_observation' => 'x15._collection_time_.24h.',
'sample_x68._date_of_observation_collection' => 'x16._collection_date',
);

sub cleanAndAddDerivedData {
  my ($self, $hash) = @_;
  return if defined($hash->{primary_key});
  $self->SUPER::cleanAndAddDerivedData($hash);
	if(defined($hash->{primary_key})){ return } ## I am doing getParentParsedOutput() in SampleReader
	while(my ($a,$b) = each(%remap)){
		next unless defined $hash->{$a};
		die "$a conflicts with $b" if(defined($hash->{$b}) && $hash->{$a} ne $hash->{$b});
		my $hash->{$b} = $hash->{$a};
		delete $hash->{$a};
		printf STDERR ("remapped $a to $b: $hash->{$b}\n");
	}
}

sub makeParent {
  my ($self, $hash) = @_;
  if($hash->{parent}) {
    return $hash->{parent};
  }
	die sprintf("No parent id in %s\n", $self->getMetadataFile) unless length($hash->{'participant_id'}) > 0;
  return $hash->{'participant_id'};
}

sub makePrimaryKey {
  my ($self, $hash) = @_;
  if($hash->{primary_key}) {
    return $hash->{primary_key};
  }
 	my $parent = $self->getParentParsedOutput()->{$hash->{participant_id}};
	return undef unless $parent;
  my $mdfile = lc(fileparse($self->getMetadataFile(),qr/\.[^.]+$/));
##if($mdfile =~ /inpatient_treatment_drug/){
##  my $rx_name = # $hash->{rx_name};
##    $hash->{'x51._rx_name'} ||
##    $hash->{'x56._rx_name'} ||
##    $hash->{'x61._rx_name'} ||
##    $hash->{'x66._generic_antimalarial_name'} ||
##    $hash->{'x71._generic_antimalarial_name'}; 

##  return join("_", $hash->{participant_id}, $rx_name, $hash->{timepoint});
##}
	if($mdfile eq 'sample_collection_form'){
		my $date = $hash->{'x16._collection_date'} || $hash->{'x12._temperature_reading_date'};
		my $time = $hash->{'x15._collection_time_.24h.'} || $hash->{'x11._temperature_reading_time_.24h.'};
		$time =~ s/^0:/12:/;
		$time = UnixDate(ParseDate($time), "%H%M");
		$time ||= '0000';
  	return join("_", $hash->{participant_id}, $date, $time);
	}
	elsif($mdfile eq 'inpatient_care_chart_review'){
		my $date = $hash->{'x68._date_of_observation_collection'};
		my $time = $hash->{'x69._time_of_observation'};
		$time =~ s/^0:/12:/;
		$time = UnixDate(ParseDate($time), "%H%M");
  	return join("_", $hash->{participant_id}, $date, $time);
	}
	elsif($mdfile =~ 'diagnostics_assay'){
 	  my $date = $hash->{date};
 	  return join("_", $hash->{participant_id}, $date, '0000');
	}
	elsif($mdfile eq 'samp_coll_form_3'){
 	  my $date = $hash->{'x30._antimalarial_therapy_initiation_at_gmc_date'};
		unless($date){
			$date = $self->makeParticipantDateKey($hash);
		}
 	  return join("_", $hash->{participant_id}, $date, '0000');
	}
	else {
#	  my $parent = $self->getParentParsedOutput()->{$hash->{participant_id}};
 	  my $date = $parent->{'x8._date_enrolled'} || $parent->{'x34._age_.at_enrollment.'};
 	  unless($date){
 	  	# printf STDERR ("No date available: %s: %s\n", $mdfile, $hash->{participant_id});
 	  	# print STDERR Dumper $parent; die;
 	  }
 	  return join("_", $hash->{participant_id}, $date || 'na', '0000');
	}
 	 #	my $parent = $self->getParentParsedOutput()->{$hash->{participant_id}};
 	 #	$date = $parent->{'x8._date_enrolled'} || $parent->{'x34._age_.at_enrollment.'};
 	 #  unless($date){
 	 #  	printf STDERR ("No date available: %s: %s\n", $mdfile, $hash->{participant_id});
 	 #  	print STDERR Dumper $parent; die;
 	 #  }
}

# override read() only for inpatient_treatment_drug_[12345]
# each row generates several triples

1;

package ClinEpiData::Load::IcemrSouthAsiaReader::DrugReader;
use base qw(ClinEpiData::Load::IcemrSouthAsiaReader);
use File::Basename;
use Data::Dumper;

sub makeParent {
  my ($self, $hash) = @_;
  if($hash->{parent}) {
    return $hash->{parent};
  }
	my $id = lc($hash->{'participant_id'});
	die sprintf("No participant id in %s\n", $self->getMetadataFile) unless length($id) > 0;
	my $parentMerged = $self->getParentParsedOutput();
	my ($pid) = sort grep { /^${id}_\d\d\d\d-\d\d-\d\d/ } keys %$parentMerged;
	unless($pid){
		($pid) = sort grep { /^${id}_/ } keys %$parentMerged;
	}
	return $pid;
}

sub skipIfNoParent { return 1 }

sub makePrimaryKey {
  my ($self, $hash) = @_;
  if($hash->{primary_key}) {
    return $hash->{primary_key};
  }
  my $mdfile = lc(fileparse($self->getMetadataFile(),qr/\.[^.]+$/));
	if($mdfile =~ /inpatient_treatment_drug/){
 	  my $rx_name = # $hash->{rx_name};
 	    $hash->{'x51._rx_name'} ||
 	    $hash->{'x56._rx_name'} ||
 	    $hash->{'x61._rx_name'} ||
 	    $hash->{'x66._generic_antimalarial_name'} ||
 	    $hash->{'x71._generic_antimalarial_name'}; 

 	  return join("_", $hash->{participant_id}, $rx_name, $hash->{timepoint});
	}
	die "File $mdfile not supported\n";
}

sub cleanAndAddDerivedData {
  my ($self, $hash) = @_;
  $self->SUPER::cleanAndAddDerivedData($hash);
}

sub rowMultiplier {
	my ($self,$hash) = @_;
	my @clones;
  my $mdFile = $self->getMetadataFile();
	unless($mdFile =~ /inpatient_treatment_drug_/){ 
		return [$hash];
	}
	my @cols = sort keys %$hash;
	my @timepoints = grep(/timepoint/i, @cols); 
	my @doses = map { lc } grep(/dose/i, @cols); 
	my @methods = map { lc } grep(/method/i, @cols); 
	foreach my $i ( 0 .. $#timepoints ) {
		next if(
			($hash->{$timepoints[$i]} eq 'na') &&
			($hash->{$doses[$i]} eq 'na') &&
			($hash->{$methods[$i]} eq 'na')
		);
		my %clone = ( %$hash );
		$clone{timepoint} = $hash->{$timepoints[$i]};
		$clone{dose} = $hash->{$doses[$i]};
		$clone{method} = $hash->{$methods[$i]};
    if($hash->{participant_id} eq '1030077' && $clone{timepoint} eq '0'){
      my ($rx) = $hash->{'x51._rx_name'} || $hash->{'x56._rx_name'};
      print STDERR "DEBUG:$mdFile:$rx:timepoint 0\n";
    }
		push(@clones, \%clone);
	}
	return \@clones;
}

1;


package ClinEpiData::Load::IcemrSouthAsiaReader::SampleReader;
use base qw(ClinEpiData::Load::IcemrSouthAsiaReader);
use Date::Manip qw(Date_Init ParseDate UnixDate DateCalc);
use File::Basename;
sub cleanAndAddDerivedData {
  my ($self, $hash) = @_;
  $self->SUPER::cleanAndAddDerivedData($hash);
 	foreach my $col (qw/x68._date_of_observation_collection x69._time_of_observation/){
 		my $k = "sample_${col}";
 		$hash->{$k} = $hash->{$col};
 		delete $hash->{$col};
 	}
	if(defined($hash->{'x87._total_wbc_count'}) && $hash->{'x87._total_wbc_count'} > 0){
		$hash->{'x87._total_wbc_count'} /= 1000;
	}
}

sub makeParent {
  my ($self, $hash) = @_;
  if($hash->{parent}) {
    return $hash->{parent};
  }
	my $class = ref($self);
	my $pid = $self->makeSampleParentKey($hash);
	bless($self, $class);
	my $parentMerged = $self->getParentParsedOutput();
	if(!defined($parentMerged->{$pid})){
		my $id = $hash->{participant_id};
		($pid) = sort grep { /^${id}_\d\d\d\d-\d\d-\d\d/ } keys %$parentMerged;
		# die "NO Parent for $id" unless $pid;
	}
	return $pid;
}

sub makePrimaryKey {
  my ($self, $hash) = @_;
	return $self->makeParent($hash);
}

sub getPrimaryKeyPrefix {
	return 'S';
}

sub makeSampleParentKey{
  my ($self, $hash) = @_;
  if($hash->{primary_key}) {
    return $hash->{primary_key};
  }
  my $mdfile = lc(fileparse($self->getMetadataFile(),qr/\.[^.]+$/));
##if($mdfile =~ /inpatient_treatment_drug/){
##  my $rx_name = # $hash->{rx_name};
##    $hash->{'x51._rx_name'} ||
##    $hash->{'x56._rx_name'} ||
##    $hash->{'x61._rx_name'} ||
##    $hash->{'x66._generic_antimalarial_name'} ||
##    $hash->{'x71._generic_antimalarial_name'}; 

##  return join("_", $hash->{participant_id}, $rx_name, $hash->{timepoint});
##}
	if($mdfile eq 'sample_collection_form'){
		my $date = $hash->{'x16._collection_date'} || $hash->{'x12._temperature_reading_date'};
		my $time = $hash->{'x15._collection_time_.24h.'} || $hash->{'x11._temperature_reading_time_.24h.'};
		$time =~ s/^0:/12:/;
		$time = UnixDate(ParseDate($time), "%H%M");
		$time ||= '0000';
  	return join("_", $hash->{participant_id}, $date, $time);
	}
	elsif($mdfile eq 'inpatient_care_chart_review'){
		my $date = $hash->{'x68._date_of_observation_collection'};
		my $time = $hash->{'x69._time_of_observation'};
		$time =~ s/^0:/12:/;
		$time = UnixDate(ParseDate($time), "%H%M");
  	return join("_", $hash->{participant_id}, $date, $time);
	}
	elsif($mdfile =~ 'diagnostics_assay'){
 	  my $date = $hash->{date};
 	  return join("_", $hash->{participant_id}, $date, '0000');
	}
	elsif($mdfile eq 'samp_coll_form_3'){
 	  my $date = $hash->{'x30._antimalarial_therapy_initiation_at_gmc_date'};
		unless($date){
			$date = $self->makeParticipantDateKey($hash);
		}
 	  return join("_", $hash->{participant_id}, $date, '0000');
	}
	else {
 	  my $parent = $self->getParentParsedOutput()->{$hash->{participant_id}};
 	  my $date = $parent->{'x8._date_enrolled'} || $parent->{'x34._age_.at_enrollment.'};
 	  unless($date){
 	  	# printf STDERR ("No date available: %s: %s\n", $mdfile, $hash->{participant_id});
 	  	# print STDERR Dumper $parent; die;
 	  }
 	  return join("_", $hash->{participant_id}, $date || 'na', '0000');
	}
 	 #	my $parent = $self->getParentParsedOutput()->{$hash->{participant_id}};
 	 #	$date = $parent->{'x8._date_enrolled'} || $parent->{'x34._age_.at_enrollment.'};
 	 #  unless($date){
 	 #  	printf STDERR ("No date available: %s: %s\n", $mdfile, $hash->{participant_id});
 	 #  	print STDERR Dumper $parent; die;
 	 #  }
}

1;
