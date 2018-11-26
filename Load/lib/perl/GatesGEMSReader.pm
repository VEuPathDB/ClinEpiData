package ClinEpiData::Load::GatesGEMSReader;
use base qw(ClinEpiData::Load::MetadataReaderCSV);


sub formatdate{
    my ($self,$date) = @_;
    $date =~ s/\//-/g;
    return $date;
}

sub clean {
  my ($self, $ar) = @_;

  my $clean = $self->SUPER::clean($ar);

  for(my $i = 0; $i < scalar @$clean; $i++) {

    my $v = $clean->[$i];

    my $lcv = lc($v);

    if($lcv eq 'na' || $lcv eq 'a' || $lcv eq 'f' || $lcv eq 't' || $lcv eq 'u' || $lcv eq 'n' || $lcv eq 'r' || $lcv eq 'l') {
      $clean->[$i] = undef;
    }
  }
  return $clean;
}

sub cleanAndAddDerivedData {
  my ($self, $hash) = @_;
}

1;

package ClinEpiData::Load::GatesGEMSReader::HouseholdReader;
use base qw(ClinEpiData::Load::GatesGEMSReader);
use strict;

sub makeParent {
  my ($self, $hash) = @_;
  return undef;
}

sub makePrimaryKey {
  my ($self, $hash) = @_;
  return $hash->{childid};
}

sub getPrimaryKeyPrefix {
    return "HH"; 
}

1;

=pop
HouseholdObservations.txt is merged by(rbind) HHObservationEnrolldate and HHObservationFollowup (PKs are different). Each file has all HH variables.  
=cut
package ClinEpiData::Load::GatesGEMSReader::HouseholdObservationEnrolldateReader;
use base qw(ClinEpiData::Load::GatesGEMSReader);


sub makeParent {
  my ($self, $hash) = @_;
  return $hash->{childid};
}

sub getParentPrefix {                                                                                                                  my ($self, $hash) = @_;                                                                                                            return "HH";                                                                                                                   }

sub makePrimaryKey {                                                                                                                
    my ($self, $hash) = @_;                                                                                                           
    my $suffix = $self->getSuffix();                                                                                               
    return $hash->{childid} .  "_" . $suffix;                                                                                      }     

sub getSuffix{                                                                                                                      
    return "enrollment";                                                                                                    
}

sub getPrimaryKeyPrefix {
    return "HH";
}


sub cleanAndAddDerivedData{
    my ($self,$hash)=@_;
    $hash->{hhobservationprotocol}="enrollment";


    if ($hash->{parent}){
	$hash->{parent} =~ s/^([a-z])/\u$1/;
    }

    

}

1;


package ClinEpiData::Load::GatesGEMSReader::HouseholdObservationFollowdateReader;
use base qw(ClinEpiData::Load::GatesGEMSReader);


sub makeParent {
  my ($self, $hash) = @_;
  return $hash->{childid};
}

sub getParentPrefix {                                                                                                                  my ($self, $hash) = @_;                                                                                                            return "HH";                                                                                                                   }

sub makePrimaryKey {                                                                                                               
																       my ($self, $hash) = @_;                                                                                                        
																       my $suffix = $self->getSuffix();
																       return $hash->{childid} .  "_" . $suffix;                                                                                    
}

sub getSuffix{                                                                                       
																       return "followup";
}

sub getPrimaryKeyPrefix {
    return "HH";
}


sub cleanAndAddDerivedData{
    my ($self,$hash)=@_;
    $hash->{hhobservationprotocol}="60 day follow-up";

    
    if ($hash->{parent}){
        $hash->{parent} =~ s/^([a-z])/\u$1/;
    }
}

1;




package ClinEpiData::Load::GatesGEMSReader::OutputReader;
use base qw(ClinEpiData::Load::OutputFileReader);


1;


package ClinEpiData::Load::GatesGEMSReader::ParticipantReader;
use base qw(ClinEpiData::Load::GatesGEMSReader);

sub makeParent {
  my ($self, $hash) = @_;

  return $hash->{childid};
}

sub makePrimaryKey {
  my ($self, $hash) = @_;

  return $hash->{childid};
}


sub getParentPrefix {
  my ($self, $hash) = @_;

  return "HH";
}

1;

package ClinEpiData::Load::GatesGEMSReader::EnrollmentObservationReader;
use base qw(ClinEpiData::Load::GatesGEMSReader);

use Date::Manip qw(Date_Init ParseDate UnixDate DateCalc Delta_Format);

use Data::Dumper;


sub makeParent {
  my ($self, $hash) = @_;
  
  return $hash->{childid};
}

sub makePrimaryKey {
  my ($self, $hash) = @_;
  my $date;
  if ($hash->{enrolldate}){
      $date=$hash->{enrolldate};
  }
  else {
      
      die 'Could not find the enrollment date';
           
  }
  $date= $self->formatdate($date);
  return $hash->{childid} . "_" . $date;
}

#we are appending to col_exclude based on the header
sub adjustHeaderArray { 
  my ($self, $ha) = @_;
  my $colExcludes = $self->getColExcludes();
  #$colExcludes->{'__ALL__'}->{$key}

  my @find = grep (/_find_/i,@$ha);
  my @last = grep (/_last_/i,@$ha);
  my @out = grep (/_out_/i,@$ha);
  my @rehyd = grep (/_rehyd_/i,@$ha);
  my @newcolExcludes=(@find,@last,@out,@rehyd);
  #print Dumper \@newcolExcludes;
  
  #exit;

  foreach my $newcol (@newcolExcludes){
      $newcol=lc($newcol);
      $colExcludes->{'__ALL__'}->{$newcol}=1;
  }
  #print Dumper $colExcludes;
  return $ha;
}

sub cleanAndAddDerivedData{
    my ($self,$hash)=@_;
    
    $hash->{observationprotocol}="enrollment";
    
    Date_Init("DateFormat=US");

   # my $type = $hash->{type};

    if ($hash->{type} eq  'case'){


	my $f3_date = $hash->{f3_date};
	my $f3_childbirth = $hash->{f3_childbirth};
	
 	my $parsed_f3_date = ParseDate($f3_date);
	my $parsed_f3_childbirth = ParseDate($f3_childbirth);
	

	my $deltaCase = DateCalc($f3_childbirth, $f3_date);


	my $hoursCase = Delta_Format($deltaCase,"%hv");  


	$hash->{enrollment_age_days} = int($hoursCase/24 + 0.5);
	
    }


    if ($hash->{type} eq 'control'){
	
	my $f6_date=$hash->{f6_date};                                                                                                      my $f6_birth_date=$hash->{f6_birth_date};                                                                                    
	my $parsed_f6_date = ParseDate($f6_date);                                                                                          my $parsed_f6_birth_date = ParseDate($f6_birth_date);                                                                              
	my $deltaControl = DateCalc($f6_birth_date, $f6_date); 
	
	my $hoursControl = Delta_Format($deltaControl,"%hv");                                                                           
	
	$hash->{enrollment_age_days} = int($hoursControl/24 + 0.5);     
    }

}



1;




package ClinEpiData::Load::GatesGEMSReader::FollowupObservationReader;
use base qw(ClinEpiData::Load::GatesGEMSReader);

sub makeParent {
  ## returns a Participant ID
  my ($self, $hash) = @_;
  
  return $hash->{childid};
}

sub makePrimaryKey {
  my ($self, $hash) = @_;
  my  $date=$hash->{f5_date};
 
   $date= $self->formatdate($date); 
  return $date ? $hash->{childid} . "_" . $date : $hash->{childid};
}


sub cleanAndAddDerivedData{
    my ($self,$hash)=@_;
    $hash->{observationprotocol}="60 day follow-up";

}

1;




package ClinEpiData::Load::GatesGEMSReader::MedicalObservationReader;
use base qw(ClinEpiData::Load::GatesGEMSReader);

sub getDateColumn{
    return "enrolldate";
}
sub getSuffix{}

sub makeParent {
  ## returns a Participant ID 
  my ($self, $hash) = @_;
  my $dateColumn = $self->getDateColumn();
  my $date = $self->formatdate($hash->{$dateColumn});
  return $hash->{childid} . "_" . $date;
}

sub makePrimaryKey {
  my ($self, $hash) = @_;
  my $dateColumn = $self->getDateColumn();
  my  $date=$hash->{$dateColumn};
  return undef unless $date;
  my $suffix = $self->getSuffix();
  $date= $self->formatdate($date);
  return $hash->{childid} . "_" . $date .  "_" . $suffix;
}

1;


package ClinEpiData::Load::GatesGEMSReader::MedicalFindObservationReader;
use base qw(ClinEpiData::Load::GatesGEMSReader::MedicalObservationReader);

sub getSuffix{
    return "find";
}


sub cleanAndAddDerivedData{
    my ($self,$hash)=@_;
    $hash->{observationprotocol}="Enrollment, Outcome 4 hours after rehydration";

}

1;



package ClinEpiData::Load::GatesGEMSReader::MedicalLASTObservationReader;
use base qw(ClinEpiData::Load::GatesGEMSReader::MedicalObservationReader);

sub getSuffix{
    return "last";
}


sub cleanAndAddDerivedData{
    my ($self,$hash)=@_;
    $hash->{observationprotocol}="Enrollment, Last Outcome";

}

1;


package ClinEpiData::Load::GatesGEMSReader::MedicalOUTObservationReader;
use base qw(ClinEpiData::Load::GatesGEMSReader::MedicalObservationReader);

sub getSuffix{
    return "out";
}


sub cleanAndAddDerivedData{
    my ($self,$hash)=@_;
    $hash->{observationprotocol}="Enrollment, Outcome leaving hospital/health center";

}

1;


package ClinEpiData::Load::GatesGEMSReader::MedicalREHYDObservationReader;
use base qw(ClinEpiData::Load::GatesGEMSReader::MedicalObservationReader);

sub getSuffix{
    return "rehyd";
}


sub cleanAndAddDerivedData{
    my ($self,$hash)=@_;
    $hash->{observationprotocol}="Enrollment, Outcome if additional rehydration needed";

}

1;



package ClinEpiData::Load::GatesGEMSReader::SampleReader;
use base qw(ClinEpiData::Load::GatesGEMSReader);
use File::Basename;
use Data::Dumper;

sub getDateColumn{
    return "enrolldate";
}

sub makeParent {
    ## returns a Participant ID + ENROLLDATE
    my ($self, $hash) = @_;
    my $file = basename $self->getMetadataFile();
    return undef if (($file eq "TAC.csv") || ($file eq "microbiome.csv"));
    my $dateColumn_sample = $self->getDateColumn();
    my $date = $self->formatdate($hash->{$dateColumn_sample});
    return $hash->{childid} . "_" . $date;
}

sub makePrimaryKey {
    my ($self, $hash) = @_;
    my $file = basename $self->getMetadataFile();
    #print $file . "\n";
    #exit;
    
    if ($file eq "GEMS1_Case_control_Study_data.csv"){
	return $hash->{f11_specimen_id};
    }elsif ($file eq "TAC.csv"){
	return $hash->{sid};
    }elsif ($file eq "microbiome.csv"){
	return undef;
    }else{
	die "could not find sid and f11_specimen_id (or lab_specimen_id) and sample_id";
    }
    
}



sub readAncillaryInputFile {
    my ($self, $file) = @_;
    
    my @sampleID;
    my @Value;
    my %hash;

    open(FILE, $file) or die "Cannot open file $file for reading:$!";
    my $dummy=<FILE>;
    while (<FILE>){
        chomp;
        my ($sampleID,$Value) = split ",";
        push @sampleID,$sampleID;
       # push @Value,$Value;
    }
  
    $hash{'Sample_ID'} = \@sampleID;
    #$hash{'microbiome'} = \@Value;
    
#    foreach (@{$hash{'Sample_ID'}}) {
#	print $_ . "\n";
#   }
 
    return \%hash;
}



sub cleanAndAddDerivedData {
    my ($self, $hash) = @_;
    my @total_specimen_id;
    my @total_sid;
    my $f11_specimen_id;
    my $sid; 
    
    my $file =  basename $self->getMetadataFile();    
    my $ancillaryData =  $self->getAncillaryData();
    

    if ($file eq "GEMS1_Case_control_Study_data.csv"){
	$f11_specimen_id =  $hash->{f11_specimen_id};

	foreach (@{$ancillaryData->{'Sample_ID'}}){
	    if ($_ == $f11_specimen_id){
		$hash->{microbiome} = 1;
	    }
	
	    
	}
    }elsif($file eq "TAC.csv"){
	$sid =  $hash->{sid};
	foreach(@{$ancillaryData->{'Sample_ID'}}){
	    if ($_ == $sid){
		$hash->{microbiome} = 1;
	    }
	}
    }else{
	die "could not find sid and f11_specimen_id (or lab_specimen_id) and sample_id";  
    }
    
    $hash->{microbiome} = 0 unless defined($hash->{microbiome});
}

1;



=pod



sub adjustHeaderArray { 
    my ($self, $ha) = @_;
    my $colExcludes = $self->getColExcludes();
    my $file = basename $self->getMetadataFile();
    if ($file eq "GEMS1_Case_control_Study_data.csv"){
	
	$colExcludes->{'__ALL__'}->{aepec}=1;
	$colExcludes->{'__ALL__'}->{eaec}=1;
	$colExcludes->{'__ALL__'}->{stec}=1;
	$colExcludes->{'__ALL__'}->{tepec}=1;
    }
    
    return $ha;
}


1;


    if(exists($hash->{f11_specimen_id})) {
	return $hash->{f11_specimen_id};
    }else {
	my $child=$hash->{childid};
	#print STDERR "childid=$child\n";	
        die "Could not find the specimen_id for participant:  $child";
	
    }
}

1;

=cut







####################### GEMS1A ################################################################################
###############################################################################################################
###############################################################################################################

package ClinEpiData::Load::GatesGEMSReader::HouseholdReader::GEMS1aHouseholdReader;
use base qw(ClinEpiData::Load::GatesGEMSReader::HouseholdReader);
1;

package ClinEpiData::Load::GatesGEMSReader::HouseholdObservationEnrolldateReader::GEMS1aHouseholdObservationEnrolldateReader;
use base qw(ClinEpiData::Load::GatesGEMSReader::HouseholdObservationEnrolldateReader);

1;


package ClinEpiData::Load::GatesGEMSReader::HouseholdObservationFollowdateReader::GEMS1aHouseholdObservationFollowdateReader;
use base qw(ClinEpiData::Load::GatesGEMSReader::HouseholdObservationFollowdateReader);

1;



package ClinEpiData::Load::GatesGEMSReader::OutputReader::GEMS1aOutputReader;
use base qw(ClinEpiData::Load::GatesGEMSReader::OutputReader);
1;

package ClinEpiData::Load::GatesGEMSReader::ParticipantReader::GEMS1aParticipantReader;
use base qw(ClinEpiData::Load::GatesGEMSReader::ParticipantReader);
use Data::Dumper;


sub cleanAndAddDerivedData {
    my ($self, $hash) = @_;

    if ($hash->{f4b_date_death} eq "10/11/2012"){
	$hash->{f5_date_death} = $hash->{f4b_date_death};
    }
    
    if ($hash->{f4b_date_death} eq "10/27/2012"){
        $hash->{f5_date_death} = $hash->{f4b_date_death};
    }
 }



1;



package ClinEpiData::Load::GatesGEMSReader::EnrollmentObservationReader::GEMS1aEnrollmentObservationReader;
use base qw(ClinEpiData::Load::GatesGEMSReader::EnrollmentObservationReader);
use Date::Manip qw(Date_Init ParseDate UnixDate DateCalc Delta_Format);
1;

package ClinEpiData::Load::GatesGEMSReader::FollowupObservationReader::GEMS1aFollowupObservationReader;
use base qw(ClinEpiData::Load::GatesGEMSReader::FollowupObservationReader);
1;





package ClinEpiData::Load::GatesGEMSReader::GEMS1aMedicalFindObservationReader;
use base qw(ClinEpiData::Load::GatesGEMSReader::MedicalObservationReader);

sub getSuffix{
    return "find";
}


sub cleanAndAddDerivedData{
    my ($self,$hash)=@_;
    $hash->{observationprotocol}="Enrollment, Outcome 4 hours after rehydration";

}

1;

package ClinEpiData::Load::GatesGEMSReader::GEMS1aMedicalLastObservationReader;
use base qw(ClinEpiData::Load::GatesGEMSReader::MedicalObservationReader);

sub getSuffix{
    return "last";
}


sub cleanAndAddDerivedData{
    my ($self,$hash)=@_;
    $hash->{observationprotocol}="Enrollment, Last Outcome";

}

1;

package ClinEpiData::Load::GatesGEMSReader::GEMS1aMedicalOutObservationReader;
use base qw(ClinEpiData::Load::GatesGEMSReader::MedicalObservationReader);

sub getSuffix{
    return "out";
}


sub cleanAndAddDerivedData{
    my ($self,$hash)=@_;
    $hash->{observationprotocol}="Enrollment, Outcome leaving hospital/health center";

}

1;

package ClinEpiData::Load::GatesGEMSReader::GEMS1aMedicalRehydObservationReader;
use base qw(ClinEpiData::Load::GatesGEMSReader::MedicalObservationReader);

sub getSuffix{
    return "rehyd";
}


sub cleanAndAddDerivedData{
    my ($self,$hash)=@_;
    $hash->{observationprotocol}="Enrollment, Outcome if additional rehydration needed";

}

1;





package ClinEpiData::Load::GatesGEMSReader::GEMS1aSampleReader;
use base qw(ClinEpiData::Load::GatesGEMSReader);
use File::Basename;
use Data::Dumper;

sub getDateColumn{
    return "enrolldate";
}

sub makeParent {
    ## returns a Participant ID + ENROLLDATE
    my ($self, $hash) = @_;
    my $dateColumn_sample = $self->getDateColumn();
    my $date = $self->formatdate($hash->{$dateColumn_sample});
    return $hash->{childid} . "_" . $date;
}

sub makePrimaryKey {
    my ($self, $hash) = @_;
    my $file = basename $self->getMetadataFile();
    #print $file . "\n";
    #exit;
    
    if ($file eq "gems1a_case_control_study_data.csv"){
	return $hash->{f11_specimen_id};
    }else{
	die "could not find sid and f11_specimen_id (or lab_specimen_id)";
    }
    
}
