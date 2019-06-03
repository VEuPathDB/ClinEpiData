package ClinEpiData::Load::GatesPERCHReader;
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
        if($file eq "_clin_rev_de.txt"){

            $hash->{enrldate_par} = $hash->{enrldate};
            $hash->{enrldate} = undef;

        }
    }
}


1;



package ClinEpiData::Load::GatesPERCHReader::ObservationReader;
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

sub getPrimaryKeyPrefix {
    return "O";
}


sub cleanAndAddDerivedData {
    my ($self, $hash) = @_;
    my $file =  basename $self->getMetadataFile();

    if(defined($hash->{enrldate})){
        if($file eq "_clin_rev_de.txt"){

            $hash->{enrldate_obs} = $hash->{enrldate};
            $hash->{enrldate} = undef;

        }
    }
}


1;
