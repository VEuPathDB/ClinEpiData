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
    my ($self, $hash) = @_;
    return "HH";
}

sub cleanAndAddDerivedData {
    my ($self, $hash) = @_;
    
    $hash->{ramal2} = ucfirst($hash->{ramal2})

}

1;

package ClinEpiData::Load::IcemrAmazoniaBrazilReader::HouseholdObservationReader;
use base qw(ClinEpiData::Load::IcemrAmazoniaBrazilReader);
use File::Basename;
use Data::Dumper;


sub makeParent {
  my ($self, $hash) = @_;

  return $hash->{dom} ;

 }

sub getParentPrefix {
    my ($self, $hash) = @_;
    return "HH";
}

sub makePrimaryKey {                                                                                                             
    my ($self, $hash) = @_;
    return $hash->{dom} .  "_" . $hash->{wave};                                                                               
}

sub getPrimaryKeyPrefix {
    my ($self, $hash) = @_;
    return "HO";
}



sub cleanAndAddDerivedData {
    my ($self, $hash) = @_;
    
    $hash->{cob} = ucfirst($hash->{cob})

}

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

sub getParentPrefix {
    my ($self, $hash) = @_;
    return "HH";
}

sub makePrimaryKey {
    my ($self, $hash) = @_;
    return $hash->{rg};
}


sub cleanAndAddDerivedData {
    my ($self, $hash) = @_;
    
    $hash->{localnas} = ucfirst($hash->{localnas});
    $hash->{occupa} = ucfirst($hash->{occupa});
    $hash->{occupb} = ucfirst($hash->{occupb});
    $hash->{lococcupa} = ucfirst($hash->{lococcupa});
    $hash->{lococcupb} = ucfirst($hash->{lococcupb});
    $hash->{rempa} = ucfirst($hash->{rempa});
    $hash->{quelrema} = ucfirst($hash->{quelrema});
    $hash->{rempb} = ucfirst($hash->{rempb});
    $hash->{quelremb} = ucfirst($hash->{quelremb});
    $hash->{motinta} = ucfirst($hash->{motinta});
    $hash->{motintb} = ucfirst($hash->{motintb});
    $hash->{motintc} = ucfirst($hash->{motintc});
    $hash->{cidainta} = ucfirst($hash->{cidainta});
    $hash->{cidaintb} = ucfirst($hash->{cidaintb});
    $hash->{cidaintc} = ucfirst($hash->{cidaintc});
    $hash->{estado1an} = ucfirst($hash->{estado1an});
    $hash->{estado2an} = ucfirst($hash->{estado2an});
    $hash->{estado3an} = ucfirst($hash->{estado3an});
    $hash->{estado4an} = ucfirst($hash->{estado4an});
    $hash->{estado5an} = ucfirst($hash->{estado5an});
    $hash->{ocupb} = ucfirst($hash->{ocupb});
    $hash->{ocupa} = ucfirst($hash->{ocupa});
    $hash->{lococupa} = ucfirst($hash->{lococupa});
    $hash->{lococupb} = ucfirst($hash->{lococupb});
    $hash->{qualrema} = ucfirst($hash->{qualrema});
    $hash->{qualremb} = ucfirst($hash->{qualremb});

    $hash->{comdor_par} = $hash->{comdor};
    $hash->{comdor} = undef;

    #$hash->{idade_en} = int $hash->{idade_en};
    if($hash->{idade_en} eq 'na'){$hash->{idade_en} = $hash->{idade_en}}
    else{$hash->{idade_en} = int $hash->{idade_en}};
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

    if ($file eq "Observations.csv" ){

	$hash->{wave_obs} = $hash->{wave};
	$hash->{wave} = undef;
    }

    $hash->{outr30d} = ucfirst($hash->{outr30d});
    $hash->{outr7d} = ucfirst($hash->{outr7d});
    $hash->{ordsin} = ucfirst($hash->{ordsin});
    $hash->{remcasei} = ucfirst($hash->{remcasei});
    $hash->{qrem} = ucfirst($hash->{qrem});
    $hash->{qualmed} = ucfirst($hash->{qualmed});
    $hash->{pq_itn} = ucfirst($hash->{pq_itn});
    $hash->{desloc30d} = ucfirst($hash->{desloc30d});
    $hash->{desloc6d} = ucfirst($hash->{desloc6d});
    $hash->{localout} = ucfirst($hash->{localout});
    $hash->{qualmeto} = ucfirst($hash->{qualmeto});


    if($hash->{dtmal1c} eq "4/5/2010"){$hash->{dtmal1c} = "2010-04-05"};
    if($hash->{dtmal1c} eq "4/25/2010"){$hash->{dtmal1c} = "2010-04-25"};
    if($hash->{dtmal1c} eq "4/1/2010"){$hash->{dtmal1c} = "2010-04-01"};

    
    if($hash->{idadeno} eq 'na'){$hash->{idadeno} = $hash->{idadeno}}
    else{$hash->{idadeno} = int $hash->{idadeno}};


    if($hash->{dtentrev} eq "41010"){$hash->{dtentrev} = "04/10/2010"};

    if($hash->{inisint}){

	if($hash->{inisint} =~ m/(\d\d).(\d\d).(\d\d)/){
	    $hash->{inisint} = join('/',$2,$1,join('','20',$3)), "\n";
	}
    }

    if($hash->{sintno}){

	if($hash->{sintno} =~ m/(\d\d).(\d\d).(\d\d)/){
	    $hash->{sintno} = join('/',$2,$1,join('','20',$3)), "\n";
	}
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

sub cleanAndAddDerivedData {
    my ($self, $hash) = @_;

    $hash->{gametoci} = ucfirst($hash->{gametoci});


}

1;
