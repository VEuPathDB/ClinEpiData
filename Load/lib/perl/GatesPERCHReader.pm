package ClinEpiData::Load::GatesPERCHReader;
use base qw(ClinEpiData::Load::MetadataReader);


sub formatdate{
    my ($self,$date) = @_;
    $date =~ s/\//-/g;
    return $date;
}

=head
sub cleanAndAddDerivedData {
    my ($self, $hash) = @_;

        for my $field ((
                'seaettm','clalatm','spfpftm', 'cam1abtm'
		       )){
	    if(defined($hash->{$field}) &&
	       $hash->{$field} eq 'na'){
		delete $hash->{$field};
	    }
	    $hash->{$field} =~ s/^0:/12:/;
        }
}

1;
=cut

package ClinEpiData::Load::GatesPERCHReader::HouseholdReader;
use base qw(ClinEpiData::Load::GatesPERCHReader);
use File::Basename;
use Data::Dumper;
use strict;

sub makeParent {
    my ($self, $hash) = @_;
    return undef;
}

sub makePrimaryKey {
    my ($self, $hash) = @_;
    if(defined $hash->{patid}){
	return $hash->{patid};
    }else{
	return undef;
    }
}

sub getPrimaryKeyPrefix {
    my ($self, $hash) = @_;
    return "HH";
}

1;



package ClinEpiData::Load::GatesPERCHReader::OutputReader;
use base qw(ClinEpiData::Load::OutputFileReader);

1;




package ClinEpiData::Load::GatesPERCHReader::ParticipantReader;
use base qw(ClinEpiData::Load::GatesPERCHReader);
use File::Basename;
use Data::Dumper;


sub makeParent {
    my ($self, $hash) = @_;
    return $hash->{patid};
}

sub makePrimaryKey {
    my ($self, $hash) = @_;
    return $hash->{patid};
}


sub getParentPrefix {
    my ($self, $hash) = @_;
    return "HH";
}

sub cleanAndAddDerivedData {
    my ($self, $hash) = @_;
    my $file =  basename $self->getMetadataFile();

    if ($file eq "cdc_R.txt" || $file eq "csa_R.txt" || $file eq "csf_R.txt" || $file eq "cfu.txt" || $file eq "lrt.txt")
    {
	$hash->{enrldate} = undef;
	
    }

    if(defined($hash->{enrldate})){
            $hash->{enrldate_par} = $hash->{enrldate};
            $hash->{enrldate} = undef;
    }
  
    if(defined($hash->{cmrdthdt})){
	my $date = $hash->{cmrdthdt};
	return undef unless $date;
    }



 }


1;



package ClinEpiData::Load::GatesPERCHReader::ObservationReader;
use base qw(ClinEpiData::Load::GatesPERCHReader);
use Date::Manip qw(Date_Init ParseDate UnixDate DateCalc);
use File::Basename;
use Data::Dumper;


sub makeParent {
    my ($self, $hash) = @_;
    return $hash->{patid};
}

sub makePrimaryKey {
    my ($self, $hash) = @_;
    return $hash->{patid};
}

sub getPrimaryKeyPrefix {
    return "O_";
}



sub adjustHeaderArray {
    my ($self, $ha) = @_;
    my $colExcludes = $self->getColExcludes();

    my @visit24hr = grep (/24$/i,@$ha);
    my @visit48hr = grep (/48$/i,@$ha);
    my @visit30days = grep (/^csf|30d$/i,@$ha);
    my @newcolExcludes=(@visit24hr,@visit48hr,@visit30days);
    

    foreach my $newcol (@newcolExcludes){
	$newcol=lc($newcol);
	$colExcludes->{'__ALL__'}->{$newcol}=1;
    }

    return $ha;
}

sub cleanAndAddDerivedData {
    my ($self, $hash) = @_;

    $hash->{observation_type}="1) Enrollment";


    my $file =  basename $self->getMetadataFile();

    if ($file eq "cdc_R.txt" || $file eq "csa_R.txt" || $file eq "csf_R.txt" || $file eq "cfu.txt" || $file eq "lrt.txt"){
	$hash->{enrldate} = undef;
	$hash->{cdcdisdt} = undef;
	$hash->{csahr} = undef;
    }

    if ($file eq "_lab_rev_de.txt"){
	$hash->{_prabxur_3_lab_rev} = $hash->{_prabxur_3};
	$hash->{_prabxur_3} = undef;
    }


    if(defined($hash->{enrldate})){

            $hash->{enrldate_obs} = $hash->{enrldate};
            $hash->{enrldate} = undef;
    }

=head
    if(defined ($hash->{'cam1abtm'})){
	my $time = $hash->{'cam1abtm'};
	$time =~ s/^0:/12:/;
	$time = UnixDate(ParseDate($time), "%H%M");
	$time ||= '0000';
    }
=cut
    if ($file eq "_clin_rev_de.txt"){
	$hash->{_muc} = ($hash->{_muc})/10;
	$hash->{chxotasp} = ucfirst $hash->{chxotasp};
	$hash->{chxotbsp} = ucfirst $hash->{chxotbsp};
    }

    if ($file eq "cdc_R.txt"){
	$hash->{cdcodbsp} = ucfirst $hash->{cdcodbsp};
	$hash->{cdcoebsp} = ucfirst $hash->{cdcoebsp};
	$hash->{cdcofbsp} = ucfirst $hash->{cdcofbsp};
    }
    if ($file eq "csa_R.txt"){
	$hash->{csadx1sp} = ucfirst $hash->{csadx1sp};
	$hash->{csadx2sp} = ucfirst $hash->{csadx2sp};
	$hash->{csadx3sp} = ucfirst $hash->{csadx3sp};
    }

}

1;




package ClinEpiData::Load::GatesPERCHReader::SubObservationVisit24hrReader;
use base qw(ClinEpiData::Load::GatesPERCHReader);
use File::Basename;
use Data::Dumper;


sub makeParent {
    my ($self, $hash) = @_;
    return $hash->{patid};
}
=head -- here we changed its PARENT to Participant instead of Observation !!!!!!!!!!!!!!!!!!!!!!!
sub getParentPrefix {
    my ($self, $hash) = @_;
    return "O_";
}
=cut
sub makePrimaryKey {
    my ($self, $hash) = @_;
    return $hash->{patid};
}

sub getPrimaryKeyPrefix {
    return "visit24h_";
}

sub cleanAndAddDerivedData {
    my ($self, $hash) = @_;

    $hash->{observation_type}="2) 24 hour follow-up";

    my $file =  basename $self->getMetadataFile();

    if ($file eq "cdc_R.txt" || $file eq "csa_R.txt" || $file eq "csf_R.txt"|| $file eq "cfu_24h.txt" || $file eq "cfu_48h.txt"|| $file eq "lrt.txt"){
	$hash->{csffudt} = undef;
    }
 
    if($file eq "cfu_24h.txt"){
	$hash->{cfumvent_24h} = $hash->{cfumvent};
	$hash->{cfumvent} = undef;
	
    }
}

1;

package ClinEpiData::Load::GatesPERCHReader::SubObservationVisit48hrReader;
use base qw(ClinEpiData::Load::GatesPERCHReader);
use File::Basename;

sub makeParent {
    my ($self, $hash) = @_;
    return $hash->{patid};
}
=head
sub getParentPrefix {
    my ($self, $hash) = @_;
    return "O_";
}
=cut
sub makePrimaryKey {
    my ($self, $hash) = @_;
    return $hash->{patid};
}

sub getPrimaryKeyPrefix {
    return "visit48h_";
}

sub cleanAndAddDerivedData {
    my ($self, $hash) = @_;

    $hash->{observation_type}="3) 48 hour follow-up";

    my $file =  basename $self->getMetadataFile();
    
    if($file eq "cfu_48h.txt"){
	$hash->{cfumvent_48h} = $hash->{cfumvent};
	$hash->{cfumvent} = undef;
	
    }
    
}



1;


package ClinEpiData::Load::GatesPERCHReader::SubObservationVisit30dayReader;
use base qw(ClinEpiData::Load::GatesPERCHReader);
use File::Basename;
use Data::Dumper;


sub makeParent {
    my ($self, $hash) = @_;
    return $hash->{patid};
}
=head
sub getParentPrefix {
    my ($self, $hash) = @_;
    return "O_";
}
=cut
sub makePrimaryKey {
    my ($self, $hash) = @_;
    return $hash->{patid};
}

sub getPrimaryKeyPrefix {
    return "visit30d_";
}

sub cleanAndAddDerivedData {
    my ($self, $hash) = @_;

    $hash->{observation_type}="4) 30 day follow-up";


    my $file =  basename $self->getMetadataFile();

    if ($file eq "cdc_R.txt" || $file eq "csa_R.txt" || $file eq "csf_R.txt"|| $file eq "cfu.txt" || $file eq "lrt.txt"){
	$hash->{csffudt} = undef;
	$hash->{csfvitst} = undef;
    }

    if ($file eq "csf_R.txt"){
	$hash->{csfarmci} = ($hash->{csfarmci})/10;
    }


}

1;



package ClinEpiData::Load::GatesPERCHReader::SampleReader;
use base qw(ClinEpiData::Load::GatesPERCHReader);
use Date::Manip qw(Date_Init ParseDate UnixDate DateCalc);
use File::Basename;
use Data::Dumper;

sub makeParent {
    my ($self, $hash) = @_;
    return $hash->{patid};
}

sub getParentPrefix {
    return "O_";
}

sub makePrimaryKey {
    my ($self, $hash) = @_;
    return $hash->{patid};
}

sub getPrimaryKeyPrefix {
    return "S_";
}

sub cleanAndAddDerivedData {
    my ($self, $hash) = @_;
    my $file =  basename $self->getMetadataFile();

    if ($file eq "_clin_rev_de.txt"){
	$hash->{_prabxur_3_clin_rev} = $hash->{_prabxur_3};
	$hash->{_prabxur_3} = undef;
    }
    if ($file eq "_lab_rev_de.txt"){
	$hash->{_prabxur_3_lab_rev} = $hash->{_prabxur_3};
	$hash->{_prabxur_3} = undef;
    }

    if ($file eq "_pneu_st.txt"){
	$hash->{is_nt_pneu_st} = $hash->{is_nt};
	$hash->{is_nt} = undef;

	$hash->{is_unknown_pneu_st} = $hash->{is_unknown};
	$hash->{is_unknown} = undef;
    }
    if ($file eq "_hinf_st.txt"){
	
	$hash->{is_unknown_hinf_st} = $hash->{is_unknown};
	$hash->{is_unknown} = undef;
	
	if( ($hash->{is_nt} ==9) && ($hash->{is_hinf} ==9)){
	    $hash->{is_hinf_nt_all} = 9;
	    
	}elsif(($hash->{is_nt} == 1) || ($hash->{is_hinf} == 1)){
	    $hash->{is_hinf_nt_all} = 1;
	    
	    #$hash->{is_nt_hinf_st} = $hash->{is_nt};
	    #$hash->{is_nt} = undef;
	    
	    #$hash->{is_nt_hinf_st} = undef;
	    #$hash->{is_hinf} = undef;
	}elsif(($hash->{is_nt} eq '') && ($hash->{is_hinf} eq '') ){
	    $hash->{is_hinf_nt_all} = '';
	}else{$hash->{is_hinf_nt_all} = 0;}


	if( ($hash->{is_b} ==9) && ($hash->{is_hinb} ==9)){
            $hash->{is_bb_all} = 9;

	}elsif(($hash->{is_b} == 1) || ($hash->{is_hinb} == 1)){
            $hash->{is_bb_all} = 1;

	}elsif(($hash->{is_b} eq '') && ($hash->{is_hinb} eq '') ){
            $hash->{is_bb_all} = '';
        }else{$hash->{is_bb_all} = 0;}

	$hash->{is_nt_hinf_st} = $hash->{is_nt};
	#$hash->{is_nt} = undef;
	#$hash->{is_nt_hinf_st} = undef;
	#$hash->{is_b} = undef;
	#$hash->{is_hinb} = undef;

	    
    }

############## we may don't need the following in PERCH
=head
    if(defined ($hash->{'seaettm'} || $hash->{'clalatm'} || $hash->{'spfpftm'})){

    my $time = $hash->{'seaettm'} || $hash->{'clalatm'} || $hash->{'spfpftm'};
    $time =~ s/^0:/12:/;
    $time = UnixDate(ParseDate($time), "%H%M");
    $time ||= '0000';

    }


=cut
}



1;






