#!/usr/bin/env perl
use strict;
use warnings;
use File::Basename;
use Text::CSV_XS;
use Getopt::Long;
use Unicode::Normalize qw/normalize/;
use Data::Dumper;
use utf8;
my ($delim, $tab, $utf, $form);
GetOptions( 'd=s' => \$delim, 't!' => \$tab, 'u!' => \$utf, 'n=s' => \$form );
if($utf) { use open ':std', ':encoding(UTF-16)' }
else { use open ':std', ':encoding(UTF-8)' }
my ($file, $outdir) = @ARGV;
$delim //= ",";
if($tab){ $delim = "\t" }
my $indir = ".";
unless ( $outdir &&  -d $outdir ){ die "Must specify an output dir" }
my @files;
if(-d $file){
  $indir = $file;
  opendir(DH, $indir);
  @files = map { -T "$indir/$_" ? $_ : undef } grep { !/^\./ } readdir(DH);
  closedir(DH);
  # printf STDERR ("FILES:\n%s\n\t", join("\n\t", @files));
}
else{
  $indir = dirname($file);
  @files = (basename($file));
}
foreach $file (@files){
  next unless $file;
  my $csv = Text::CSV_XS->new({binary => 1, sep_char => $delim, quote_char => '"' }) or die "Cannot use CSV: " . Text::CSV_XS->error_diag ();  
  printf STDERR ("[Reading file %s/%s]\n", $indir, $file);
  my $lines = 0;
  open(my $fh, "<$indir/$file") or die "$@\n";
  open(OF, "> $outdir/$file") or die "Cannot write $outdir/$file: $!\n";
  my @headers;
  while(my $line = <$fh>){
    chomp $line;
    $line =~ s/[\r\l\n]/; /g;
  #while (my $row = $csv->getline( $fh )) {
  	$lines++;
  	#die if grep { /\t/ } @$row;
    my @row;
  	my @out;
    my $retry = 0;
    my $colnum = 0;
 RETRY:
    my @check = split(/$delim/, $line);
    while(scalar @headers > scalar @check){ # first iter, headers empty
      my $append = <$fh>;
      $line .= $append;
      chomp $line;
      $line =~ s/[\r\l\n]/; /g;
      @check = split(/$delim/, $line);
    }
    my $status = $csv->parse($line);
    if($retry || ! $status){
        print STDERR ("Text::CSV failed, falling back to base Perl at $lines\n");
      ## Try to parse with base Perl
      my @values = split(/$delim/, $line);
      if($retry){ ## last line contained newline, splice first value to $row[-1]
        print STDERR ("RETRY at $lines\n");
        $row[-1] = join("; ", $row[-1], shift @values);
        $retry = 0;
      }
      for(; $colnum < @headers; $colnum++){
        push(@row,$values[$colnum]);
        #print STDERR ("\t$h = $v\n");
      }
      if(scalar @row < scalar @headers){
        $line = <$fh>;
        $retry = 1;
        goto RETRY;
      }
      if(scalar @row > scalar @headers){
        die "STATUS = $status at line $lines: $line";
      }
    }
    else {
      @row = $csv->fields();
    }
    #print STDERR Dumper \@row; exit;
  	foreach my $val (@row){
      $val =~ s/^\x{FEFF}//;
  		$val =~ s/[\n\r\l]/ /g;
  		$val =~ s/[\n\r\l]/ /g;
  		$val =~ s/\t/ /g;
      if($form){ $val = normalize($form, $val) }
  		push(@out, $val);
  	}
    unless(@headers){ @headers = @out; }
  	printf OF ("%s\n", join("\t", @out));
  }
  close(OF);
  printf STDERR ("\n[Read %d lines]\n", $lines);
}
printf STDERR ("[Done, %d files]\n", scalar @files);

