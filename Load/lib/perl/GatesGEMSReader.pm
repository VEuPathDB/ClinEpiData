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

use Data::Dumper;

sub makeParent {
  ## returns a Participant ID
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
    if(exists($hash->{f11_specimen_id})) {
	return $hash->{f11_specimen_id};
    }else {
	my $child=$hash->{childid};
	#print STDERR "childid=$child\n";	
        die "Could not find the specimen_id for participant:  $child";
	
    }
}

1;

































