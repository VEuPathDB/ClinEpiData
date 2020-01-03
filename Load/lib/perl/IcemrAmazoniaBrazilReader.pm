package ClinEpiData::Load::IcemrAmazoniaBrazilReader;
use base qw(ClinEpiData::Load::MetadataReaderCSV);


sub formatdate{
    my ($self,$date) = @_;
    $date =~ s/\//-/g;
    return $date;
}


sub cleanAndAddDerivedData {
  my ($self, $hash) = @_;
}

1;

package ClinEpiData::Load::IcemrAmazoniaBrazilReader::HouseholdReader;
use base qw(ClinEpiData::Load::IcemrAmazoniaBrazilReader);
use strict;

sub makeParent {
  my ($self, $hash) = @_;
  return undef;
}

sub makePrimaryKey {
  my ($self, $hash) = @_;
  return $hash->{dom};
}

sub getPrimaryKeyPrefix {
    return "HH"; 
}

sub cleanAndAddDerivedData{
    my ($self,$hash)=@_;
    $hash->{hhobservationprotocol}="Enrollment";

}

1;



package ClinEpiData::Load::IcemrAmazoniaBrazilReader::HouseholdObservationReader;
use base qw(ClinEpiData::Load::IcemrAmazoniaBrazilReader);


sub makeParent {
  my ($self, $hash) = @_;
  return $hash->{childid};
}

sub getParentPrefix {                                                                                                            

    my ($self, $hash) = @_;                                                                                                      
    return "HH";                                                                                                                 
}

sub makePrimaryKey {                                                                                                             
    my ($self, $hash) = @_;                                                                                                      
    my $suffix = $self->getSuffix();                                                                                             
    return $hash->{childid} .  "_" . $suffix;                                                                                     
}
sub getSuffix{                                                                                                                    
    return "household_followup";                                                                                                  
}


sub cleanAndAddDerivedData{
    my ($self,$hash)=@_;
    $hash->{hhobservationprotocol}="60 day follow-up";


}

1;


