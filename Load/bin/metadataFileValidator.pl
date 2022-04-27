#!/usr/bin/perl
#
# ISASimple metadata file validator
#
# Input args: list of files
# Exit code: 0 success, 1+ number of failurs
# STDERR lists PASS/FAIL + failure messages
#
# Validator tests for:
# - Supported file format / extension
# - File exists
# - Can be parsed by Perl package Text::CSV
# - Minimum number of columns
#   - failure may indicate parse error, e.g. extention:delimiter mismatch
# - Minimum number of rows
#

use strict;
use warnings;
# use lib "$ENV{GUS_HOME}/lib/perl";
use File::Basename;
use Text::CSV;
use Data::Dumper;

my %FORMAT = (
  '.TXT' => "\t",
  '.TSV' => "\t",
  '.CSV' => ",",
);

# args: list of files
# or, get list from stdin
#
my @files = @ARGV;
unless(@files){
  while(<>){
    chomp;
    push(@files, $_);
  }
}

unless(@files){
  printf STDERR ("No files to validate\n");
  exit 0;
}

my $exitcode = 0;

my $results = { FAIL => {}, PASS => {}};

foreach my $filepath (@files){
  my ($path, $name, $ext) = fileparse($filepath, qr/\.[^\.]*$/);
  my $filename = basename($filepath);
  unless( $ext && $FORMAT{ uc($ext) } ){
    $results->{FAIL}->{$filename} = "Unsupported file extension $ext";
    next;
  }
  my $delim = $FORMAT{uc($ext)};
  unless( -e $filepath ){
    $results->{FAIL}->{$filename} = "File not found";
    next;
  }
  unless( -f $filepath ){
    $results->{FAIL}->{$filename} = "File not a plain file (may be link, pipe, directory, etc.)";
    next;
  }

  if(-B $filepath){
    $results->{FAIL}->{$filename} = "Binary file; only text is supported";
    next;
  }
  my $ifh;
  unless( open($ifh, "<$filepath")){
    $results->{FAIL}->{$filename} = "Cannot read file: $@";
    next;
  }
  my $csv = Text::CSV->new({binary => 1, sep_char => $delim, quote_char => '"' }) or die "Cannot use CSV: " . Text::CSV->error_diag ();  
  my $headers = $csv->getline( $ifh );
  unless($headers) {
    $results->{FAIL}->{$filename} = "unreadable, possibly not a plain text file";
    close($ifh);
    next;
  }

  if(@$headers < 2 ){ ## check headers
    $results->{FAIL}->{$filename} = "Only one column header found; check file format and delimiter";
    close($ifh);
    next;
  }
  my $lines = 0;
  eval {
    while($csv->getline( $ifh )) { $lines++ }
  };
  unless($lines){ ## Text::CSV threw an error
    $results->{FAIL}->{$filename} = "Empty (no data rows)";
  }
  else {
    $results->{PASS}->{$filename} = "OK";
  }
  close($ifh);
}

while( my ($filename, $message) = each %{$results->{FAIL}}){
  printf STDERR ("%s\t%s\tFAIL\n", $filename, $message);
  $exitcode++;
}
while( my ($filename, $message) = each %{$results->{PASS}}){
  printf STDERR ("%s\t%s\tPASS\n", $filename, $message)
}

exit ($exitcode > 0);
