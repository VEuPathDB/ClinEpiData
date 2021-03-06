#!/usr/bin/env perl

use strict;
use warnings;

use Getopt::Long;
use File::Basename;

unless(@ARGV){
	printf("Usage:\n\t%s -f inputFile -i identifierColumnHeader [-t] [-v]\n\t-i|--idcol header of the unique identifier column\n\t-t|--tab for tab-delimited input (default is CSV)\n\t-v|--inverse print column headers that are not invariable (good for finding columns to exclude)\n", basename($0));
	exit;
}


my ($tab, $file, @idcols, $inverse);
GetOptions(
	'f|file=s' => \$file,
	'i|idcol=s' => \@idcols,
	't|tab' => \$tab,
	'v|inverse' => \$inverse,
);

# field separator regex
my $fsrx = ',';
$fsrx = '\t' if($tab);

# read column headers
open(FH, "<$file") or die "Cannot read $file:$!\n";
my $headrow = <FH>;
$headrow =~ s/\n|\r//g;
my (@fields) = map { s/^"|"$//g; lc } split($fsrx, $headrow);

my %data;
my %bin;
my %nobin;
my $count;
my $prev = 0;
my $n = 0;
while(my $line = <FH>){
  $n++;
  chomp $line;
  my (@values) = split($fsrx, $line);
  my %row;
  @row{@fields} = @values;
	my $id = join("_", map { $row{$_} } @idcols);
  foreach my $field (@fields){
		next if grep /^$field$/, @idcols;
    $data{$field}{$id} ||= $row{$field};
    if($data{$field}{$id} eq $row{$field}){
      $bin{$field} = 1 unless $nobin{$field};
    }
    else {
      delete $bin{$field};
      $nobin{$field} = 1;
    }
  }
  $count = scalar keys %bin;
  printf STDERR ("%d: %d lines, %d invariable fields\n", $n, $count, scalar keys %bin) unless $count == $prev ;
  $prev = $count;
}
if($inverse){
	printf("%s\n", join("\n", sort keys %nobin));
}
else{
	printf("%s\n", join("\n", sort keys %bin));
}


  
  
  
