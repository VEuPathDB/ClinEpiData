#!/usr/bin/env perl
use strict;
use warnings;

use lib $ENV{GUS_HOME} . "/lib/perl";

use ApiCommonData::Load::OwlReader;
use Env qw/PROJECT_HOME SPARQLPATH/;
use File::Basename;
use Getopt::Long;
use Data::Dumper;
use CBIL::Util::PropertySet;

my ($dataset, @filters, @files, $inverse, $propFile, $varDelim);
GetOptions(
  'o|owl=s' => \$dataset,
  'f|filter=s' => \@filters,
  'i|input=s' => \@files,
  'v|inverse' => \$inverse,
  'p|propFile=s' => \$propFile,
  'd|varDelim=s' => \$varDelim # a delimeter used in the owl file as in <column><Delim><file>
);
# $varDelim = '::';
if(defined($propFile) && -e $propFile) {
  my @_p;
  my $p = CBIL::Util::PropertySet->new($propFile, \@_p, 1);
  push(@files, split(/,/,$p->{props}->{metadataFile}));
  $dataset ||= $p->{props}->{ontologyOwlFile};
  push(@filters, lc($p->{props}->{type})) unless @filters;
}

unless($dataset){
  printf(join("\n\n",
    "Usage:\n\t%s -o|owl [owl] -f|filter [[filter]] -i|input [data file] [ -i [data file ] ] [-v|inverse]",
    "Owl file must exist:\$PROJECT_HOME/ApiCommonData/Load/ontology/release/production/[owl].owl",
    "Run without [[filter]] to see a list of options for this dataset",
    "Run without -i[[data files ...]] to print only columns that are mapped in this dataset",
    "Run with -v to get only columns in [[filter]]\n"
  ), basename($0));
  exit;
}

unless (@files) { $inverse = 1; } 

my %columns; ## all columns in data files
my %index; ## col => file
foreach my $file (@files){
  unless(-f $file){
    print STDERR "File does not exist: $file\n";
    next;
  }
  open(FH, "<$file") or die "Cannot open $file:$!\n";
  my $head = <FH>;
  close(FH);
  my ($baseName) = fileparse(lc($file), qr/\.[^.]+$/);
  chomp $head;
  $head =~ s/\r$//;
  my $delim = ",";
  $delim = "\t" if($head =~ /\t/);
  for my $col (split(/$delim/, $head)){
    $col = pp($col);
    if($varDelim){ $col = join($varDelim, $baseName, $col); }
    $columns{$col} = 1;
    $index{$col} ||= {};
    $index{$col}->{$baseName} = 1;
 printf STDERR ("READ: $col FROM $baseName\n");
  } 
}

printf STDERR ("%d files, %d columns\n", scalar @files, scalar keys %columns);

my $owlFile = $dataset;
unless( -e $owlFile ){
  $owlFile = "$PROJECT_HOME/ApiCommonData/Load/ontology/release/production/$dataset.owl";
}

my $owl = ApiCommonData::Load::OwlReader->new($owlFile);

my @entities;
my %map;
my %saved;
my %terms;

my %filterOptions;
my $it = $owl->execute('top_level_entities');
while (my $row = $it->next) {
  my $label = pp($row->{label}->as_sparql);
  my $entity = $row->{entity}->as_sparql;
  $filterOptions{$label} = $entity;
}

unless(@filters){
  printf STDERR ("Choose one of these top-level entities:\n\t%s\n", join("\n\t", sort keys %filterOptions));
  exit;
} 

foreach my $filter (@filters){
  die "Entity not found for $filter\nAvailable:\n\t" . join("\n\t", keys %map) . "\n" unless defined $filterOptions{$filter};
  my $filterEntity = $filterOptions{$filter};

  my $itr = $owl->execute('all_subclasses', { ENTITY => $filterEntity });
  #my @keys = $itr->binding_names;
  #printf ("%s\n", join("\t", @keys)) if @keys;
  while (my $row = $itr->next) {
    my $col = pp($row->{col}->as_sparql);
    my @cols;
    if($col =~ /,/){
      @cols = split (/\s*,\s*/,$col);
    }
    else {
      @cols = ($col);
    }
    foreach $col (@cols){
      unless($varDelim){ $col =~ s/^.*::// }
      my $dataset = defined($row->{dataset}) ? pp($row->{dataset}->as_sparql) : [];
      if(ref($dataset) eq 'ARRAY'){
        $dataset = join("\t", @$dataset);
      }
      if(defined($terms{$col})){
        if($terms{$col} eq '1' && $dataset){
          $terms{$col} = $dataset;
        }
        elsif($dataset){
          $terms{$col} .= $dataset;
        }
      }
      elsif($dataset){
        $terms{$col} = $dataset;
      }
      else {
        $terms{$col} = 1;
      }
      if(defined($columns{$col})){
        delete($columns{$col});
        delete($index{$col});
        $saved{$col} = 1;
      }
    }
  }

  printf STDERR ("on branch %s(%s) there are %d terms\n", $filter, $filterEntity, scalar keys %terms);
  printf STDERR ("%d columns remain\n", scalar keys %columns);
}


print STDERR ("-------------------\n");

if($inverse){
  foreach my $k (sort keys %saved){
    if($terms{$k} eq '1'){
      print "$k\n";
    }
    else {
      print "$k\t$terms{$k}\n";
    }
  }
  if( !  @files ){
    foreach my $k (sort keys %terms){
      if($terms{$k} eq '1'){
        print "$k\n";
      }
      else {
        print "$k\t$terms{$k}\n";
      }
    }
  }
}
else{
  foreach my $var (sort keys %index){
    printf("%s\n", $var);
   #foreach my $file( keys %{$index{$var}} ){
   #  printf("%s\t%s\n", $var, $file);
   #}
  }
}

exit;


sub pp {
  my ($val) = @_;
  $val =~ s/^"|"$//g;
  return lc($val);
}

1;
