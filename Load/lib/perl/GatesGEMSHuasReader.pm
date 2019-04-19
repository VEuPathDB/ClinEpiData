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

	$hash->{kenya_consent} = $hash->{consent};
	$hash->{consent} = undef;
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


sub cleanAndAddDerivedData {
    my ($self, $hash) = @_;
    my $file =  basename $self->getMetadataFile();
    
    if(defined($hash->{noseek_politic})){
	if($file eq "gems1_huas_data.csv"){

	    $hash->{huas_noseek_politic} = $hash->{noseek_politic}; 
	    $hash->{noseek_politic} = undef;

	}

	if($file eq "gems1_huas_lite_data_kenya.csv" || $file eq "gems1_huas_lite_data_6_sites.csv"){
	
	    $hash->{kenya6sites_noseek_politic} = $hash->{noseek_politic};
	    $hash->{noseek_politic} = undef;
	    
	}
	if ($file eq "gems1_huas_lite_data_kenya.csv"){
	    $hash->{vomit_kenya} = $hash->{vomit};
	    $hash->{vomit} = undef;

	    $hash->{seekcare_kenya} = $hash->{seekcare};
	    $hash->{seekcare} = undef;

	    $hash->{admit_kenya} = $hash->{admit};
	    $hash->{admit} = undef;

	    $hash->{offer_drink_kenya} = $hash->{offer_drink};
	    $hash->{offer_drink} = undef;

	    $hash->{offer_eat_kenya} = $hash->{offer_eat};
	    $hash->{offer_eat} = undef;

	}
	if ($file eq "gems1_huas_lite_data_6_sites.csv"){
            $hash->{vomit_6site} = $hash->{vomit};
	    $hash->{vomit} = undef;

	    $hash->{seekcare_6site} = $hash->{seekcare};
	    $hash->{seekcare} = undef; 

	    $hash->{admit_6site} = $hash->{admit};
	    $hash->{admit} = undef; 

	    $hash->{offer_drink_6site} = $hash->{offer_drink};
	    $hash->{offer_drink} = undef;

	    $hash->{offer_eat_6site} = $hash->{offer_eat};
	    $hash->{offer_eat} = undef;

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
    
    if ($file eq "gems1a_huas_lite_data_6_sites.csv"){
	$hash->{study_arm}="HUAS Lite";

    }elsif($file eq "gems1a_huas_lite_data_kenya.csv"){
	$hash->{study_arm}="HUAS Lite - Kenya";


	$hash->{kenya_consent} = $hash->{consent};
	$hash->{consent} = undef;

    }else{
	die "cannot find these files."
    }
}


1;


################ Observation Reader 
package ClinEpiData::Load::GatesGEMSHuasReader::GEMS1aHuasObservationReader;
use base qw(ClinEpiData::Load::GatesGEMSHuasReader);
use File::Basename;
use Data::Dumper;

sub makeParent {

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



sub cleanAndAddDerivedData {
    my ($self, $hash) = @_;
    
    my $file =  basename $self->getMetadataFile();
    
    if ($file eq "gems1a_huas_lite_data_6_sites.csv"){
	$hash->{study_arm}="HUAS Lite";
	
	$hash->{diarrhea_6site} = $hash->{diarrhea};
        $hash->{diarrhea} = undef;

	$hash->{offer_eat_6site} = $hash->{offer_eat};
        $hash->{offer_eat} = undef;

	$hash->{offer_drink_6site} = $hash->{offer_drink};
        $hash->{offer_drink} = undef;

	$hash->{vomit_6site} = $hash->{vomit};
        $hash->{vomit} = undef;

	$hash->{seekcare_6site} = $hash->{seekcare};
        $hash->{seekcare} = undef;

	$hash->{admit_6site} = $hash->{admit};
        $hash->{admit} = undef;


    }elsif($file eq "gems1a_huas_lite_data_kenya.csv"){
	$hash->{study_arm}="HUAS Lite - Kenya";


	$hash->{kenya_consent} = $hash->{consent};
	$hash->{consent} = undef;

	$hash->{diarrhea_kenya} = $hash->{diarrhea};
        $hash->{diarrhea} = undef;

	$hash->{offer_eat_kenya} = $hash->{offer_eat};
        $hash->{offer_eat} = undef;

	$hash->{offer_drink_kenya} = $hash->{offer_drink};
        $hash->{offer_drink} = undef;

	$hash->{vomit_kenya} = $hash->{vomit};
        $hash->{vomit} = undef;

	$hash->{seekcare_kenya} = $hash->{seekcare};
        $hash->{seekcare} = undef;

	$hash->{admit_kenya} = $hash->{admit};
        $hash->{admit} = undef;

    }else{
	die "cannot find these files."
    }
}






1;




