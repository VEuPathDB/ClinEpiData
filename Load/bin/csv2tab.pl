#!/usr/bin/env perl
use strict;
use warnings;
use File::Basename;
use Text::CSV_XS;
use Getopt::Long;
use Unicode::Normalize qw/normalize/;
# use Data::Dumper;
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
  # printf STDERR ("Reading file %s/%s\n", $indir, Dumper($file));
  my $lines = 0;
  open(my $fh, "<$indir/$file") or die "$@\n";
  open(OF, "> $outdir/$file") or die "Cannot write $outdir/$file: $!\n";
  while (my $row = $csv->getline( $fh )) {
  	$lines++;
  	#die if grep { /\t/ } @$row;
  	my @out;
  	foreach my $val (@$row){
      $val =~ s/^\x{FEFF}//;
  		$val =~ s/[\n\r\l]/ /g;
  		$val =~ s/\t/ /g;
      if($form){ $val = normalize($form, $val) }
  		push(@out, $val);
  	}
  	printf OF ("%s\n", join("\t", @$row));
  }
  close(OF);
  printf STDERR ("Read %d lines\n", $lines);
}
printf STDERR ("Done, %d files\n", scalar @files);

