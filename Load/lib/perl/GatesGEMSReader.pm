package ClinEpiData::Load::GatesGEMSReader;
use base qw(ClinEpiData::Load::MetadataReaderCSV);


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

sub makeParent {
  ## returns a Participant ID
  my ($self, $hash) = @_;
  
  return $hash->{childid};
}

sub makePrimaryKey {
  my ($self, $hash) = @_;
  my $date;
  if ($hash->{f4a_date}){
      $date=$hash->{f4a_date};
  }
  elsif ($hash->{f4b_date}){
      $date=$hash->{f4b_date};
  }
  elsif ($hash->{f7_date}){
      $date=$hash->{f7_date};
  }
  else {
      
      die 'Could not find the enrollment date';
           
  }
  $date=~s/\//-/g;
  return $hash->{childid} . "_" . $date;
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
  my $date;
  if ($hash->{f5_date}){
      $date=$hash->{f5_date};
  }
  
  else {
      
      die 'Could not find the followup date';
           
  }
  $date=~s/\//-/g;
  return $hash->{childid} . "_" . $date;
}
1;

package ClinEpiData::Load::GatesGEMSReader::SampleReader;
use base qw(ClinEpiData::Load::GatesGEMSReader);

sub makeParent {
  ## returns a Participant ID
  my ($self, $hash) = @_;
  if($hash->{"parent"}) {
    return $hash->{"parent"};
  }
	return $hash->{caseid};
}

sub makePrimaryKey {
  my ($self, $hash) = @_;
  if($hash->{"primary_key"}) {
    return $hash->{"primary_key"};
  }
	return $hash->{lab_specimen_id};
}

1;
