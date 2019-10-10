#!/usr/bin/env perl
($file,$str) = @ARGV;
$str ||= "USER_ERROR";
open(FH, "<$file");
$r = <FH>;
close FH;
chomp $r;
@k = split(/\t/, $r);
%t;

$cmd = sprintf('grep %s %s|', $str, $file);
print STDERR "opening:\n$cmd\n";
open(CH, $cmd);

while(<CH>){
	chomp;
	@a = split /\t/;
	for($i=0; $i <= $#a; $i++){
		$t{$k[$i]} = 1 if $a[$i] =~ /$str/;
	}
}
print STDERR "done\n";
close(CH);

map { print "$_\n" if $t{$_}} @k;

