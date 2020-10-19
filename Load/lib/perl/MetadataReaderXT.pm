package ClinEpiData::Load::MetadataReaderXT;
use base qw(ClinEpiData::Load::MetadataReader);
# XT = Extended: use valueMap.txt as ancillary input file 
# apply value mapping and/or replace variables with IRI (Source ID)
use Data::Dumper;
use strict;
use warnings;

sub readAncillaryInputFile {
# use for 1. value mapping, and 2. IRI mapping
  my ($self, $ancFile) = @_;
  open(FH, "<$ancFile") or die "Cannot read $ancFile:$!\n";
  my $anc = {};
  while(my $row = <FH>){
    chomp $row;
    $row =~ s/(\r|\l)$//g;
    my($col, $iri, $val, $mapVal) = split(/\t/,$row);
    if(defined($val) && defined($mapVal)){
      $anc->{var}->{lc($col)}->{lc($val)} =$mapVal;
      $anc->{var2}->{lc($iri)}->{lc($val)} = $mapVal;
      if($val eq ':::append:::'){ # add a static value to every row in a metadatafile
        # where $col is the metadatafile, $iri is the variable
        $anc->{append}->{$col}->{$iri}=$mapVal;
      }
    }
    $anc->{iri}->{lc($col)}=lc($iri);
  }
  close(FH);
  return $anc;
}

sub applyMappedValues {
  my ($self,$hash) = @_;
  my $anc = $self->getAncillaryData();
  my $type = 'var';
  if($self->getConfig('applyMappedIRI')){
    $type = 'var2';
  }
  while(my ($k, $v) = each %$hash){
    $k = lc($k);
    #next unless(defined($v));
    $v = lc($v);
    $v =~ s/^\s*|\s*$//g;
    if(defined($anc->{$type}->{$k})) {
      if(defined($anc->{$type}->{$k}->{$v})){
        if(uc($anc->{$type}->{$k}->{$v}) eq ':::UNDEF:::'){
          delete($hash->{$k});
        }
        else {
          $hash->{$k} = $anc->{$type}->{$k}->{$v};
          #printf STDERR ("newval %s => %s\n", $v, $hash->{$k});
        }
        last;
      }
      elsif(defined($anc->{$type}->{$k}->{':::regex:::'})){
        my $oper = sprintf("\$v =~ %s", $anc->{$type}->{$k}->{':::regex:::'});
        eval $oper;
        # printf STDERR ("newval %s => %s\n", $v, $hash->{$k});
        $hash->{$k} =$v;
      }
      elsif(defined($anc->{$type}->{$k}->{':::function:::'})){
        my @functions = split(/,/,$anc->{$type}->{$k}->{':::function:::'});
        foreach my $func (@functions){
          my $newVal = $self->$func($v);
          if(defined($newVal)){ $hash->{$k} = $newVal }
          #printf STDERR ("%s: %s => %s\n", $func, $v, $hash->{$k});
        }
        # printf STDERR ("newval %s\n", $oper);
      }
    }
    else {
    }
  }
  my $mdfile = $self->getMetadataFileLCB();
  if(defined($anc->{append}->{$mdfile})){
    while(my ($k, $v) = each %{$anc->{append}->{$mdfile}}){
      $hash->{$k} = $v;
    }
  }
}

sub applyMappedIRI {
  my ($self,$hash,$keep) = @_;
  my $anc = $self->getAncillaryData();
  while(my ($col, $val) = each %$hash){
    my $iri = $anc->{iri}->{$col};
    if($iri){
      $hash->{$iri} = $hash->{$col};
      delete($hash->{$col}) unless $keep;
      #printf STDERR ("rename %s => %s\n", $col, $iri);
    }
  }
}

sub formatTimeHHMM {
  my($self,$val) = @_;
  my($hr,$min,$half) = ($val =~ m/^(\d{1,2}).?(\d\d).?(am|pm)?$/i);
  # $min = $min / 60; # for decimal
  if(defined($half) && ($half eq 'pm') && ($hr < 12)){
    $hr = $hr + 12;
  }
  return sprintf("%02d%02d", $hr, $min);
}
1;
