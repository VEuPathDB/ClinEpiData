package ClinEpiData::Load::GatesGEMSHuasReader;
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

################ Household Reader
package ClinEpiData::Load::GatesGEMSHuasReader::HouseholdReader;
use base qw(ClinEpiData::Load::GatesGEMSHuasReader);
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
############## Output Reader
package ClinEpiData::Load::GatesGEMSHuasReader::OutputReader;
use base qw(ClinEpiData::Load::OutputFileReader);

1;

################ Participant Reader 
package ClinEpiData::Load::GatesGEMSHuasReader::ParticipantReader;
use base qw(ClinEpiData::Load::GatesGEMSHuasReader);
use File::Basename;
use Data::Dumper;

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
    return $hash->{childid};
}


sub cleanAndAddDerivedData {
    my ($self, $hash) = @_;
    
    my $file =  basename $self->getMetadataFile();
    
    if ($file eq "gems1_huas_data.csv"){
	$hash->{study_arm}=uc("HUAS");

    }elsif($file eq "gems1_huas_lite_data_6_sites.csv"){
	$hash->{study_arm}="HUAS lite";

    }elsif($file eq "gems1_huas_lite_data_kenya.csv"){
	$hash->{study_arm}="HUAS lite - Kenya";
	
    }else{
	die "cannot find these files."
    }
}


1;


################ Observation Reader 
package ClinEpiData::Load::GatesGEMSHuasReader::ObservationReader;
use base qw(ClinEpiData::Load::GatesGEMSHuasReader);
use File::Basename;
use Data::Dumper;


sub makeParent {
  ## returns a Participant ID                                                                                                       
    my ($self, $hash) = @_;

    return $hash->{childid};
}


sub makePrimaryKey {
    my ($self, $hash) = @_;  
    return $hash->{childid};  
}

sub getPrimaryKeyPrefix {
    return "O";
}



=head
sub makePrimaryKey {
    my ($self, $hash) = @_;
    
    my $file =  basename $self->getMetadataFile();
    my $date;
    if ($file eq "gems1_huas_data.csv" || $file eq "gems1_huas_lite_data_kenya.csv"){
	$date = $hash->{visitdate};  
    }elsif($file eq "gems1_huas_lite_data_6_sites.csv"){
        $date =  $hash->{visit_date};
    }else{
	die "could not find visitdate or visiot_date in these parent files";
    }
    $date= $self->formatdate($date);
    return $date ? $hash->{childid} . "_" . $date : $hash->{childid};

}
=cut
sub cleanAndAddDerivedData {
    my ($self, $hash) = @_;
    my $file =  basename $self->getMetadataFile();
    
    if(defined($hash->{noseek_politic})){
	if($file eq "gems1_huas_data.csv"){

	    $hash->{huas_noseek_politic} = $hash->{noseek_politic}; 
	}

	if($file eq "gems1_huas_lite_data_kenya.csv" || $file eq "gems1_huas_lite_data_6_sites.csv"){
	
	    $hash->{kenya6sites_noseek_politic} = $hash->{noseek_politic};
	}
    }
}

1;



####################### GEMS1A HUAS ################################################################################
###############################################################################################################                    ############################################################################################################### 
################ Household Reader
package ClinEpiData::Load::GatesGEMSHuasReader::GEMS1aHuasHouseholdReader;
use base qw(ClinEpiData::Load::GatesGEMSHuasReader);
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



################ Participant Reader 
package ClinEpiData::Load::GatesGEMSHuasReader::GEMS1aHuasParticipantReader;
use base qw(ClinEpiData::Load::GatesGEMSHuasReader);

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
    return $hash->{childid};
}

1;


################ Observation Reader 
package ClinEpiData::Load::GatesGEMSHuasReader::GEMS1aHuasObservationReader;
use base qw(ClinEpiData::Load::GatesGEMSHuasReader);
use File::Basename;
use Data::Dumper;


sub makeParent {
  ## returns a Participant ID                                                                                                       
    my ($self, $hash) = @_;

    return $hash->{childid};
}

sub makePrimaryKey {
    my ($self, $hash) = @_;

    my $file =  basename $self->getMetadataFile();

    my $date;

    if ($file eq "gems1a_huas_lite_data_6_sites.csv"){
	
	$date = $hash->{visit_date};  
    }elsif($file eq "gems1a_huas_lite_data_kenya.csv"){
	
        $date =  $hash->{visitdate};
    }else{
	die "could not find visitdate or visiot_date in these parent files";
    }

    $date= $self->formatdate($date);
    return $date ? $hash->{childid} . "_" . $date : $hash->{childid};
}

1;

