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
  #my $metadataFilename=$self->getMetadataFileLCB();

  return $hash->{dom};
  #return $hash->{ $metadataFilename . "::dom" };
}

sub getPrimaryKeyPrefix {
    return "HH"; 
}


=head
sub cleanAndAddDerivedData{
    my ($self,$hash)=@_;
    $hash->{hhobservationprotocol}="Enrollment";

}
=cut
1;



package ClinEpiData::Load::IcemrAmazoniaBrazilReader::HouseholdObservationReader;
use base qw(ClinEpiData::Load::IcemrAmazoniaBrazilReader);


sub makeParent {
  my ($self, $hash) = @_;
  return $hash->{dom};
}

sub getParentPrefix {                                                                                                            

    my ($self, $hash) = @_;                                                                                                      
    return "HH";                                                                                                                 
}

sub makePrimaryKey {                                                                                                             
    my ($self, $hash) = @_;                                                                                                      
    #my $suffix = $self->getSuffix();                                                                                             
    return $hash->{dom} .  "_" . $hash->{wave};                                                                                    }

#sub cleanAndAddDerivedData{                                                                                                       
 #   my ($self,$hash)=@_; 
 #   if ($hash->{dom} eq "61b" || $hash->{dom} eq "35a" ) { for my $key ( keys %$hash ) { $hash->{$key} =~ s/^na$//; } } 
#}     


=head
sub getSuffix{                                                                                                                    
    my ($self, $hash) = @_;                                                                                                      

    return $hash->{wave};                                                                                                
}
=cut

=head
sub cleanAndAddDerivedData{
    my ($self,$hash)=@_;
    if ($hash->{wave} == 1){$hash->{wave}="enrollment";}
    else $hash->{wave}="followup";


}
=cut
1;


package ClinEpiData::Load::IcemrAmazoniaBrazilReader::OutputReader;
use base qw(ClinEpiData::Load::OutputFileReader);

1;

package ClinEpiData::Load::IcemrAmazoniaBrazilReader::ParticipantReader;
use base qw(ClinEpiData::Load::IcemrAmazoniaBrazilReader);
use File::Basename;
use Data::Dumper;


sub makeParent {
    my ($self, $hash) = @_;
    return $hash->{dominicial};
}

sub makePrimaryKey {
    my ($self, $hash) = @_;
    return $hash->{rg};
}


sub getParentPrefix {
    my ($self, $hash) = @_;
    return "HH";
}

1;


package ClinEpiData::Load::IcemrAmazoniaBrazilReader::ObservationReader;
use base qw(ClinEpiData::Load::IcemrAmazoniaBrazilReader);
use File::Basename;
use Data::Dumper;

sub makeParent {
  my ($self, $hash) = @_;
  return $hash->{rg};
}

sub makePrimaryKey {                                                                                                             
    my ($self, $hash) = @_;                                                                                                      
    return $hash->{rg} .  "_" . $hash->{wave};  
}

sub getPrimaryKeyPrefix {
    return "O_";
}


sub cleanAndAddDerivedData {
    my ($self, $hash) = @_;

    my $file =  basename $self->getMetadataFile();

    if ($file eq "ICEMR1_Brazil_observation_05jul18z_st.csv" ){

	$hash->{wave_obs} = $hash->{wave};
	$hash->{wave} = undef;
    }
}

1;


package ClinEpiData::Load::IcemrAmazoniaBrazilReader::SampleReader;
use base qw(ClinEpiData::Load::IcemrAmazoniaBrazilReader);
use File::Basename;
use Data::Dumper;

sub makeParent {
    my ($self, $hash) = @_;
    return $hash->{rg} .  "_" . $hash->{wave};
}

sub getParentPrefix {
    return "O_";
}

sub makePrimaryKey {
    my ($self, $hash) = @_;
    return $hash->{rg} .  "_" . $hash->{wave};
}

sub getPrimaryKeyPrefix {
    return "S_";
}

1;
