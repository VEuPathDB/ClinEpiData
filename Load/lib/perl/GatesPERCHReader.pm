package ClinEpiData::Load::GatesPERCHReader;
use base qw(ClinEpiData::Load::MetadataReader);


sub formatdate{
    my ($self,$date) = @_;
    $date =~ s/\//-/g;
    return $date;
}


sub cleanAndAddDerivedData {
    my ($self, $hash) = @_;

        for my $field ((
                'seaettm','clalatm','spfpftm'
		       )){
	    if(defined($hash->{$field}) &&
	       $hash->{$field} eq 'na'){
		delete $hash->{$field};
	    }
	    $hash->{$field} =~ s/^0:/12:/;
        }
}

1;


package ClinEpiData::Load::GatesPERCHReader::HouseholdReader;
use base qw(ClinEpiData::Load::GatesPERCHReader);
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


=head
sub makePrimaryKey {
    my ($self, $hash) = @_;  

    if($hash->{enrldate}){
	$hash->{enrldate_obs} = $hash->{enrldate};
	$hash->{enrldate} = undef;
    }

    my $date = $hash->{enrldate_obs};

    $date= $self->formatdate($date);
    return $hash->{patid} . "_" . $date;

}
=cut
sub adjustHeaderArray {
    my ($self, $ha) = @_;
    my $colExcludes = $self->getColExcludes();

    my @visit24hr = grep (/24$/i,@$ha);
    my @visit48hr = grep (/48$/i,@$ha);
    my @visit30days = grep (/^csf|30d$/i,@$ha);
    my @newcolExcludes=(@visit24hr,@visit48hr,@visit30days);
    
#print Dumper \@newcolExcludes;                                                                                                    #exit;                                                                                                                            

    foreach my $newcol (@newcolExcludes){
	$newcol=lc($newcol);
	$colExcludes->{'__ALL__'}->{$newcol}=1;
    }
#print Dumper $colExcludes;                                                                                                       
    return $ha;
}

sub cleanAndAddDerivedData {
    my ($self, $hash) = @_;

    if(defined($hash->{enrldate})){

            $hash->{enrldate_obs} = $hash->{enrldate};
            $hash->{enrldate} = undef;
    }

    if(defined ($hash->{'cam1abtm'})){

	my $time = $hash->{'cam1abtm'};
	$time =~ s/^0:/12:/;
	$time = UnixDate(ParseDate($time), "%H%M");
	$time ||= '0000';

    }

}

1;




package ClinEpiData::Load::GatesPERCHReader::SubObservationVisit24hrReader;
use base qw(ClinEpiData::Load::GatesPERCHReader);

sub makeParent {
    my ($self, $hash) = @_;
    return $hash->{patid};
}
sub getParentPrefix {
    my ($self, $hash) = @_;
    return "O_";
}

sub makePrimaryKey {
    my ($self, $hash) = @_;
    return $hash->{patid};
}

sub getPrimaryKeyPrefix {
    return "visit24h_";
}

=head
sub makePrimaryKey {
    my ($self, $hash) = @_;  

    my $date= $hash->{cfuvisdt24};
    $date= $self->formatdate($date);
    return $date ? $hash->{patid} . "_" . $date . "_" . "24hr" : $hash->{patid} . "_" . "24hr";

}
=cut
sub cleanAndAddDerivedData {
    my ($self, $hash) = @_;

    if(defined($hash->{enrldate})){

            $hash->{enrldate_obs} = $hash->{enrldate};
            $hash->{enrldate} = undef;
    }
}



1;
=head
sub makeParent {
    my ($self, $hash) = @_;  

    if(defined($hash->{enrldate})){
	$hash->{enrldate_obs} = $hash->{enrldate};
	$hash->{enrldate} = undef;
    }

    my  $date=$hash->{enrldate_obs};

    $date= $self->formatdate($date);
    return $date ? $hash->{patid} . "_" . $date : $hash->{patid};

}

sub makePrimaryKey {
    my ($self, $hash) = @_;  

    if(defined($hash->{cfuvisdt24})){
	$hash->{cfuvisdt24} = $hash->{cfuvisdt24};
    }

    my $date= $hash->{cfuvisdt24};
    $date= $self->formatdate($date);
    return $date ? $hash->{patid} . "_" . $date . "_" . "24hr" : $hash->{patid} . "_" . "24hr";

}
=cut

=head
sub makePrimaryKey {
    my ($self, $hash) = @_;

    my $date;
    if (defined $hash->{cfuvisdt24}){
        $date=$hash->{cfuvisdt24};
    }
    else {
        die 'Could not find the visit24hr date';
    }

    $date= $self->formatdate($date);
    return $hash->{patid} . "_" . $date . "_" . "24hr";
}

sub adjustHeaderArray {
    my ($self, $ha) = @_;
    my $colExcludes = $self->getColExcludes();

    my @visit48hr = grep (/48$/i,@$ha);
    my @visit30days = grep (/^csf|30d$/i,@$ha);
    my @newcolExcludes=(@visit48hr,@visit30days);
    
    print Dumper \@newcolExcludes;                                                                                                    exit;                                                                                                                            

    foreach my $newcol (@newcolExcludes){
	$newcol=lc($newcol);
	$colExcludes->{'__ALL__'}->{$newcol}=1;
    }
    return $ha;
}

=cut




package ClinEpiData::Load::GatesPERCHReader::SubObservationVisit48hrReader;
use base qw(ClinEpiData::Load::GatesPERCHReader);

sub makeParent {
    my ($self, $hash) = @_;
    return $hash->{patid};
}
sub getParentPrefix {
    my ($self, $hash) = @_;
    return "O_";
}

sub makePrimaryKey {
    my ($self, $hash) = @_;
    return $hash->{patid};
}

sub getPrimaryKeyPrefix {
    return "visit48h_";
}

sub cleanAndAddDerivedData {
    my ($self, $hash) = @_;

    if(defined($hash->{enrldate})){

            $hash->{enrldate_obs} = $hash->{enrldate};
            $hash->{enrldate} = undef;
    }
}



1;


package ClinEpiData::Load::GatesPERCHReader::SubObservationVisit30dayReader;
use base qw(ClinEpiData::Load::GatesPERCHReader);

sub makeParent {
    my ($self, $hash) = @_;
    return $hash->{patid};
}
sub getParentPrefix {
    my ($self, $hash) = @_;
    return "O_";
}

sub makePrimaryKey {
    my ($self, $hash) = @_;
    return $hash->{patid};
}

sub getPrimaryKeyPrefix {
    return "visit30d_";
}

sub cleanAndAddDerivedData {
    my ($self, $hash) = @_;

    if(defined($hash->{enrldate})){

            $hash->{enrldate_obs} = $hash->{enrldate};
            $hash->{enrldate} = undef;
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
	    
	   # $hash->{is_nt_hinf_st} = $hash->{is_nt};
	   # $hash->{is_nt} = undef;
	    
	    #$hash->{is_nt_hinf_st} = undef;
	    #$hash->{is_hinf} = undef;
	    
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
    if(defined ($hash->{'seaettm'} || $hash->{'clalatm'} || $hash->{'spfpftm'})){

    my $time = $hash->{'seaettm'} || $hash->{'clalatm'} || $hash->{'spfpftm'};
    $time =~ s/^0:/12:/;
    $time = UnixDate(ParseDate($time), "%H%M");
    $time ||= '0000';

    }


}



1;






