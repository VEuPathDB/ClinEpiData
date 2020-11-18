package ClinEpiData::Load::Utilities::File;
require Exporter;
our @ISA = qw/Exporter/;
our @EXPORT_OK = qw/csv2tab diff/;
use Text::CSV;
use Scalar::Util qw/looks_like_number/;
use open ':std', ':encoding(UTF-8)';

sub csv2tab {
  my ($file, $out, $delim) = @_;
  $delim ||= ",";
  my $csv = Text::CSV->new({binary => 1, sep_char => $delim, quote_char => '"' }) or die "Cannot use CSV: " . Text::CSV->error_diag ();  
  open(my $ifh, "<$file") or die "$@\n";
  open(my $ofh, ">$out") or die "$@\n";
  while (my $row = $csv->getline( $ifh )) {
  	$lines++;
  	#die if grep { /\t/ } @$row;
  	my @out;
  	foreach my $val (@$row){
      $val =~ s/^\x{FEFF}//;
  		$val =~ s/[\n\r\l]/ /g;
  		$val =~ s/\t/ /g;
  		push(@out, $val);
  	}
  	printf $ofh ("%s\n", join("\t", @$row));
  }
  close($ifh);
  close($ofh);
}

sub diff {
  my ($fileA, $fileB, $delim) = @_;
  my ($varsA,$A) = readFileGetVarsAndDataHashes($fileA, $delim);
  my ($varsB,$B) = readFileGetVarsAndDataHashes($fileB, $delim);
  my %discrep;
  my %info;
  foreach my $var (keys %$varsA){
    $info{removedVar}->{$var} = 1 unless $varsB->{$var};
  }
  foreach my $var (keys %$varsB){
    $info{addedVar}->{$var} = 1 unless $varsA->{$var};
  }
  
  while(my ($id,$row) = each %$A){
    unless(defined($B->{$id})){
      errmsg("ID missing: $id");
      $discrep{"ID missing: $id"} = 1;
      $info{removedID} //= 0;
      $info{removedID}++;
      next;
    }
    while(my ($k,$v) = each %$row){
      next if ($info{addedVar}->{$k} || $info{removedVar}->{$k});
      if(!defined($B->{$id}->{$k})){
        errmsg("value missing $id:$k");
        $discrep{"value missing: $k"}=1;
        $info{removedValue} //= 0;
        $info{removedValue}++;
        next;
      }
      if($v eq 'NA'){$v = ''}
      my $val = $B->{$id}->{$k};
      if($val eq 'NA'){
        $val='';
      }
      if($val ne $v){
        if(looks_like_number($val)
          && looks_like_number($v)
          && $val == $v ){
          next;
        }
        errmsg("value mismatch $id:$k $v != $B->{$id}->{$k}");
        $info{changedValue} //= 0;
        $info{changedValue}++;
      }
    }
  }
  while(my ($id,$row) = each %$B){
    unless(defined($A->{$id})){
      errmsg("ID added: $id");
      $discrep{"ID added: $id"} = 1;
      $info{addedID} //= 0;
      $info{addedID}++;
      next;
    }
  }
  print msg($_) for keys %discrep;
  print msg("No discrepencies found") unless keys %discrep;
  
  #### Report ####
  if($info{removedVar}){
    printf ("Variable(s) removed: %s\n", join(", ", sort keys %{$info{removedVar}}));
  }
  if($info{addedVar}){
    printf ("Variable(s) added: %s\n", join(", ", sort keys %{$info{addedVar}}));
  }
  if($info{removedID}){
    printf ("%d row(s) deleted\n", $info{removedID});
  }
  if($info{addedID}){
    printf ("%d row(s) added\n", $info{addedID});
  }
  if($info{removedValue}){
    printf ("%d value(s) deleted\n", $info{removedValue});
  }
  if($info{changedValue}){
    printf ("%d value(s) changed\n", $info{changedValue});
  }
  
}

sub readFileGetVarsAndDataHashes {
  my ($file,$delim) = @_;
  $delim ||= "\t";
  msg("reading $file");
  my $csv = Text::CSV->new({binary => 1, quote_char => '"', sep_char => $delim});
  open(my $fh, "<$file") or die "Cannot read $file:$!\n";
  my $hr = $csv->getline($fh);
  my $k = $hr->[0];
  $csv->column_names($hr);
  my %vars;
  $vars{$_} = 1 for @$hr; 
  my %data;
  while(my $r = $csv->getline_hr($fh)){
    $data{ $r->{$k} } = $r;
  }
  close($fh);
  return (\%vars,\%data);
}

sub msg { printf STDERR ("LOG: %s\n", $_) for @_ }

sub errmsg { printf STDERR ("ERROR: %s\n", $_) for @_ }

1;
