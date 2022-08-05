package ClinEpiData::Load::Utilities::File;
use strict;
use warnings;
require Exporter;
our @ISA = qw/Exporter/;
our @EXPORT_OK = qw/csv2tab csv2array diff tabWriter nonumvalues csv2cfg getHeaders getPrefixedHeaders getValidValues/;
use Text::CSV_XS;
use Config::Std;
use Scalar::Util qw/looks_like_number/;
use open ':std', ':encoding(UTF-8)';
use Data::Dumper;
use File::Basename;

sub getCsvObject {
  my ($options) = @_;
  $options ||= {};
  $options->{sep_char} ||= "\t";
  my $csv = Text::CSV_XS->new($options) or die "Cannot use CSV: " . Text::CSV->error_diag ();
  return $csv;
}

sub tabWriter{
  return getCsvObject();
}

sub nonumvalues {
  my($rows,$col) = @_;
  my %values;
  foreach my $row(@$rows){
    next unless($row->[$col] =~ /.+/);
    next if(looks_like_number($row->[$col]));
    next if($row->[$col] =~ /NA/i );
    next if($row->[$col] =~ /^\s*$/ );
    $values{$row->[$col]} = 1;
  }
  my @nonums = keys %values;
  printf STDERR ("%s: %s\n", $col, join(",", @nonums));
  return \@nonums;
}


sub getHeaders {
  my ($file, $delim) = @_;
  $delim ||= ",";
  my $csv = getCsvObject({ sep_char => $delim});  
  open(my $ifh, "<$file") or die "$@\n";
  my $row = $csv->getline( $ifh );
  close($ifh);
  return $row; 
}

sub getPrefixedHeaders {
  my ($file, $delim) = @_;
  my $h = getHeaders($file, $delim);
  my $prefix = lc(fileparse($file, qr/\.[^\.]+$/));
  my @row = map { join("::", $prefix, $_) } @$h;
  return \@row; 
}


sub getValidValues {
  my ($file, $delim) = @_;
  $delim ||= detectDelimiter($file);
  my $csv = getCsvObject({ sep_char => $delim});  
  open(my $fh, "<$file") or die "$@\n";
  my %valid;
  $csv->column_names($csv->getline($fh));
  while( my $row = $csv->getline_hr($fh)){
    while( my ($col,$val) = each %$row ){
      next if $val eq '';
      $valid{lc($col)}->{lc($val)}++;
    }
  }
  close($fh);
  return \%valid
}

sub csv2array {
  my ($file, $delim) = @_;
  $delim ||= ",";
  my $csv = getCsvObject({ sep_char => $delim});  
  open(my $ifh, "<$file") or die "$@\n";
  my @rows;
  while (my $row = $csv->getline( $ifh )) {
  	#die if grep { /\t/ } @$row;
  	my @data;
  	foreach my $val (@$row){
      $val =~ s/\x{FEFF}//;
    	$val =~ s/[\n\r\l]/ /g;
    	$val =~ s/\t/ /g;
  		push(@data, $val);
  	}
  	push(@rows, \@data);
  }
  close($ifh);
  $csv = undef;
  return \@rows;
}

sub csv2tab {
  my ($file, $out, $delim, $quote_char, $autoclean) = @_;
#  use open ':std', ':encoding(UTF-8)';
  $delim ||= detectDelimiter($file);
  my $csv = getCsvObject({ sep_char => $delim });  
  open(my $ifh, "<$file") or die "$@\n";
  open(my $ofh, ">:encoding(utf-8)", "$out") or die "$@\n";
  my $count = 0;
  if($autoclean){
    my $row = $csv->getline( $ifh );
  	my @data;
  	foreach my $val (@$row){
      my $orig = $val;
      $val =~ s/[^A-Za-z0-9_.]/_/g;
  		push(@data, $val);
      if($val ne $orig){
        printf STDERR ("Column converted from $orig to $val\n");
      }
  	}
  	printf $ofh ("%s\n", join("\t", @data));
    $count++;
  } 
  while (my $row = $csv->getline( $ifh )) {
  	#die if grep { /\t/ } @$row;
  	my @data;
  	foreach my $val (@$row){
      $val =~ s/\x{FEFF}//;
    	$val =~ s/[\n\r\l]/ /g;
    	$val =~ s/\t/ /g;
      if($autoclean){
        $val =~ s/"//g;
      }
  		push(@data, $val);
  	}
  	printf $ofh ("%s\n", join("\t", @data));
  }
  close($ifh);
  close($ofh);
}

sub detectDelimiter {
  my ($file) = @_;
  my $headers = getHeaders($file); # assumes CSV
  if($headers->[0] =~ /\t/){ return "\t" }
  else { return "," }
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
      $v =~ s/^\s*|\s*$//g;
      if($v =~ /^N[\/]?A$/i){$v = ''}
      my $val = $B->{$id}->{$k};
      $val =~ s/^\s*|\s*$//g;
      if($val =~ /^N[\/]?A$/i){$val = ''}
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
  my $csv = Text::CSV_XS->new({binary => 1, quote_char => '"', sep_char => $delim});
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

sub csv2cfg {
  my ($infile,$outfile) = @_;
  my ($vars,$data) = readFileGetVarsAndDataHashes($infile,",");
  read_config($outfile,my %config);
  while(my ($k,$v) = each %$data){
    $config{$k} = $v;
  }
  write_config(%config);
}

sub forceArray {
  my ($node) = @_;
  unless(ref($node)){
    # a scalar
    return [$node];
  }
  # must be a ref
  if(ref($node) eq 'HASH'){
    while(my ($k,$v) = each %$node){
      $node->{$k} = forceArray($v);
    }
    return $node;
  }
  elsif(ref($node) eq 'ARRAY') {
    my @arr;
    foreach my $v (@$node){
      push(@arr, forceArray($v));
    }
    return \@arr;
  }
}



sub msg { printf STDERR ("LOG: %s\n", $_) for @_ }

sub errmsg { printf STDERR ("ERROR: %s\n", $_) for @_ }

1;
