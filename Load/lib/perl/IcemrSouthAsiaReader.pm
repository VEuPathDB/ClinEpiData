package ClinEpiData::Load::IcemrSouthAsiaReader;
use base qw(ClinEpiData::Load::MetadataReader);
use Data::Dumper;

sub cleanAndAddDerivedData {
  my ($self, $hash) = @_;
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
	return undef;
}

sub makePrimaryKey {
  my ($self, $hash) = @_;
  if($hash->{primary_key}) {
    return $hash->{primary_key};
  }
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

package ClinEpiData::Load::IcemrSouthAsiaReader::ObservationReader;
use base qw(ClinEpiData::Load::IcemrSouthAsiaReader);
use strict;
use Data::Dumper;
use File::Basename;
use Date::Manip qw(Date_Init ParseDate UnixDate DateCalc);
sub cleanAndAddDerivedData {
  my ($self, $hash) = @_;
  $self->SUPER::cleanAndAddDerivedData($hash);
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
  my $mdfile = basename($self->getMetadataFile());
	if($mdfile =~ /inpatient_treatment_drug/){
 	  my $rx_name = # $hash->{rx_name};
 	    $hash->{'x51._rx_name'} ||
 	    $hash->{'x56._rx_name'} ||
 	    $hash->{'x61._rx_name'} ||
 	    $hash->{'x66._generic_antimalarial_name'} ||
 	    $hash->{'x71._generic_antimalarial_name'}; 

 	  return join("_", $hash->{participant_id}, $rx_name, $hash->{timepoint});
	}
	elsif($mdfile eq 'sample_collection_form.rawdata'){
		my $date = $hash->{'x16._collection_date'} || $hash->{'x12._temperature_reading_date'};
		my $time = $hash->{'x15._collection_time_.24h.'} || $hash->{'x11._temperature_reading_time_.24h.'};
		$time =~ s/^0:/12:/;
		$time = UnixDate(ParseDate($time), "%H%M");
		$time ||= '0000';
  	return join("_", $hash->{participant_id}, $date, $time);
	}
	elsif($mdfile eq 'inpatient_care_chart_review.rawdata'){
		my $date = $hash->{'x68._date_of_observation_collection'};
		my $time = $hash->{'x69._time_of_observation'};
		$time =~ s/^0:/12:/;
		$time = UnixDate(ParseDate($time), "%H%M");
  	return join("_", $hash->{participant_id}, $date, $time);
	}
	elsif($mdfile eq 'Diagnostics Assay.rawdata'){
 	  my $date = $hash->{date};
 	  return join("_", $hash->{participant_id}, $date, '0000');
	}
	elsif($mdfile eq 'samp_coll_form_3.rawdata'){
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
 	  	printf STDERR ("No date available: %s: %s\n", $mdfile, $hash->{participant_id});
 	  	print STDERR Dumper $parent; die;
 	  }
 	  return join("_", $hash->{participant_id}, $date, '0000');
	}
 	 #	my $parent = $self->getParentParsedOutput()->{$hash->{participant_id}};
 	 #	$date = $parent->{'x8._date_enrolled'} || $parent->{'x34._age_.at_enrollment.'};
 	 #  unless($date){
 	 #  	printf STDERR ("No date available: %s: %s\n", $mdfile, $hash->{participant_id});
 	 #  	print STDERR Dumper $parent; die;
 	 #  }
}

# override read() only for inpatient_treatment_drug_[12345].rawdata
# each row generates several triples
sub read {
  my ($self) = @_;

  my $metadataFile = $self->getMetadataFile();
  my $fileBasename = basename $metadataFile;
	unless($fileBasename =~ /inpatient_treatment_drug_/){ 
		return $self->SUPER::read();
	}

  my $colExcludes = $self->getColExcludes();
  my $rowExcludes = $self->getRowExcludes();


  open(FILE, $metadataFile) or die "Cannot open file $metadataFile for reading: $!";

  my $header = <FILE>;
  $header =~s/\n|\r//g;

  my $delimiter = $self->getDelimiter($header);

  my @headers = $self->splitLine($delimiter, $header);

  my $headersAr = $self->adjustHeaderArray(\@headers);

  $headersAr = $self->clean($headersAr);

	my @timepoints = map { lc } grep(/timepoint/i, @$headersAr); 
	my @doses = map { lc } grep(/dose/i, @$headersAr); 
	my @methods = map { lc } grep(/method/i, @$headersAr); 

  my $parsedOutput = {};

  while(<FILE>) {
    $_ =~ s/\n|\r//g;

    my @values = $self->splitLine($delimiter, $_);
    my $valuesAr = $self->clean(\@values);

		my $countTimepoints = 0;
		foreach my $timeIndex ( 0 .. $#timepoints ){
   	  my %hash; ## new hash (row) each loop
   	  for(my $i = 0; $i < scalar @$headersAr; $i++) {
   	    my $key = lc($headersAr->[$i]);
   	    my $value = lc($valuesAr->[$i]);

   	    # TODO: move this to PRISM class clean method
   	    next if($value eq '[skipped]');

   	    $hash{$key} = $value if(defined $value);
   	  }

			my $timepoint = $hash{ $timepoints[$timeIndex] } || "";
			next if $timepoint eq 'na';
			my $dose = $hash{ $doses[$timeIndex] } || "";
			my $method = $hash{ $methods[$timeIndex] } || "";
			## clobber all timepoint, dose, and method values except these
			## delete($hash{$_}) for @timepoints,@doses,@methods;
			$hash{'timepoint'} = $timepoint;
			$hash{'dose'} = $dose;
			$hash{'method'} = $method;
			next unless($timepoint);
			$countTimepoints++;

   	  my $primaryKey = $self->makePrimaryKey(\%hash);
   	  my $parent = $self->makeParent(\%hash);

   	  next if($self->skipIfNoParent() && !$parent);

   	  my $parentPrefix = $self->getParentPrefix(\%hash) || "";
   	  my $parentWithPrefix = $parentPrefix . $parent;

   	  $hash{'__PARENT__'} = $parentWithPrefix unless($parentPrefix && $parentWithPrefix eq $parentPrefix);

   	  next unless($primaryKey); # skip rows that do not have a primary key
   	  if(defined($rowExcludes->{$primaryKey}) && ($rowExcludes->{$primaryKey} ne "") && ($rowExcludes->{$primaryKey} eq $fileBasename) || ($rowExcludes->{$primaryKey} eq '__ALL__')){
	 	  	next;
	 	  }

   	  ## $primaryKey = $self->getPrimaryKeyPrefix(\%hash) . $primaryKey;

   	  $self->cleanAndAddDerivedData(\%hash);

   	  foreach my $key (keys %hash) {
   	    next if($colExcludes->{$fileBasename}->{$key} || $colExcludes->{'__ALL__'}->{$key});
   	    next unless defined $hash{$key}; # skip undef values
   	    next if($hash{$key} eq '');

   	    next if($self->seen($parsedOutput->{$primaryKey}->{$key}, $hash{$key}));

   	    push @{$parsedOutput->{$primaryKey}->{$key}}, $hash{$key};
   	  }
			## end of timepoints
			my @tpkeys = qw/primary_key parent timepoint dose method/;
			# printf STDERR ("DEBUG: %s\n", join("\t", map { $hash{$_} || "" } @tpkeys ));
			# printf STDERR ("DEBUG: %d timepoints found for id\n", $countTimepoints, $hash{participant_id});
    }
  }

  close FILE;

  my $rv = {};

  foreach my $primaryKey (keys %$parsedOutput) {

    foreach my $key (keys %{$parsedOutput->{$primaryKey}}) {

      my @values = @{$parsedOutput->{$primaryKey}->{$key}};

      for(my $i = 0; $i < scalar @values; $i++) {
        my $value = $values[$i];

        my $newKey = $i == 0 ? $key : "${key}_$i";
        $rv->{$primaryKey}->{$newKey} = $values[$i];
      }
    }
  }


  $self->setParsedOutput($rv);
}

1;

package ClinEpiData::Load::IcemrSouthAsiaReader::SampleReader;
use base qw(ClinEpiData::Load::IcemrSouthAsiaReader);
use Date::Manip qw(Date_Init ParseDate UnixDate DateCalc);
sub cleanAndAddDerivedData {
  my ($self, $hash) = @_;
  $self->SUPER::cleanAndAddDerivedData($hash);
}

sub makeParent {
  my ($self, $hash) = @_;
  if($hash->{parent}) {
    return $hash->{parent};
  }
	my $class = ref($self);
	bless($self, 'ClinEpiData::Load::IcemrSouthAsiaReader::ObservationReader');
	my $parent = $self->makePrimaryKey($hash);
	bless($self, $class);
	return $parent;
}
sub makePrimaryKey {
  my ($self, $hash) = @_;
	return $self->makeParent($hash);
}

sub getPrimaryKeyPrefix {
	return 'S_';
}

1;
