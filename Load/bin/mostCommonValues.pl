#!/usr/bin/env perl
use strict;
use warnings;
use Getopt::Long;

my ($file, @keys, @cols);
GetOptions(
	'f|file=s' => \$file,
	'k|key=s' => \@keys, # columns to use for primary key
	'c|col=s' => \@cols, # columns of data to merge
);
my $merge = {}; 
open (FH, "<$file") or die "$!\n";
my $line = <FH>;
chomp $line;
my @index = split(/\t/, $line);
while($line = <FH>){
	chomp $line;
	my @row = split(/\t/, $line);
	my %data;
	@data{@index} = @row;
	my $key = join(":::", @data{@keys});
	$merge->{$key} ||= {};
	foreach my $col (@cols){
		next if $data{$col} eq "";
		$merge->{$key}->{$col} ||= { _total => 0, $data{$col} => 0 };
		$merge->{$key}->{$col}->{$data{$col}}++;
		$merge->{$key}->{$col}->{_total} = scalar keys %{$merge->{$key}->{$col}} ;
	}
}
close(FH);
while(my ($key,$data) = each %$merge){
	foreach my $col(@cols){
	#	next unless $merge->{$key}->{$col}->{_total} > 1;
		delete $merge->{$key}->{$col}->{_total};
		my $scores = $merge->{$key}->{$col};
		my ($topvalue) = sort { $scores->{$b} <=> $scores->{$a} } keys %$scores ;
		$merge->{$key}->{$col} = $topvalue;
	}
}
printf("%s\n", join("\t", @keys, @cols));
while(my ($key,$data) = each %$merge){
	my @keyvals = split(/:::/, $key);
	my @row = map { $data->{$_} } @cols;
	printf("%s\n", join("\t", @keyvals, @row));
}
		
