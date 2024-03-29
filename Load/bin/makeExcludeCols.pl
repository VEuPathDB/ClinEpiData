#!/usr/bin/env perl
use strict;
use warnings;

use lib $ENV{GUS_HOME} . "/lib/perl";

use ApiCommonData::Load::OwlReader;
use Env qw/PROJECT_HOME SPARQLPATH GUS_HOME/;
use File::Basename;
use Getopt::Long qw/:config no_ignore_case/ ;
use Data::Dumper;
use Config::Std; # read_config()

my ($dataset, @filters, @files, $inverse, $propFile, $varDelim, $timeVarying, $noPrefix, @filterOwlAttributes);
GetOptions(
  'o|owl=s' => \$dataset,
  'f|filter=s' => \@filters,
  "a|otherAttr=s" => \@filterOwlAttributes,
  'i|input=s' => \@files,
  'v|inverse' => \$inverse,
  'p|propFile=s' => \$propFile,
  'd|varDelim=s' => \$varDelim, # a delimeter used in the owl file as in <column><Delim><file>
  'n|noPrefix!' => \$noPrefix
);
# defaults
if(defined($propFile) && -e $propFile) {
  read_config($propFile, my %config);
  my $p = $config{''};
  if(ref($p->{metadataFile}) eq 'ARRAY'){
    push(@files, @{$p->{metadataFile}});
  }
  else {
    push(@files, split(/,/,$p->{metadataFile}));
  }
  $dataset ||= $p->{ontologyOwlFile};
  my $type  = $p->{category} || $p->{type};
  unless(@filterOwlAttributes){
    if(ref($p->{filterOwlAttribute}) eq 'ARRAY'){
      @filterOwlAttributes = @{$p->{filterOwlAttribute}};
    }
    else {
      @filterOwlAttributes = ($p->{filterOwlAttribute});
    }
  }
  if($p->{filter}){ push(@filters, $p->{filter}) }
  if($p->{otherAttr}){ push(@filterOwlAttributes, $p->{otherAttr}) }
  push(@filters, lc($type)) unless @filters;
  if($p->{noFilePrefix}){ $noPrefix = 1 }
}
unless($noPrefix){ $varDelim ||= '::' } # always use file prefix w/ '::', unless explicitly disabled
my $filterOwlAttrHash = {};
foreach my $attr (@filterOwlAttributes){
  my($k,$v) = split(/\s*[=:]\s*/, $attr);
  $filterOwlAttrHash->{$k} = $v;
        printf STDERR ("DEBUG: setting %s:%s\n", $k, $v); 
}

unless($dataset){
  printf(join("\n\n",
    "Usage:\n\t%s -o|owl [owl] -f|filter [[filter]] -i|input [data file] [ -i [data file ] ] [-v|inverse]",
    "Owl file must exist:\$GUS_HOME/ontology/release/production/[owl].owl",
    "Run without [[filter]] to see a list of options for this dataset",
    "Run without -i[[data files ...]] to print only columns that are mapped in this dataset",
    "Run with -v to get only columns in [[filter]]\n"
  ), basename($0));
  exit;
}

unless (@files) { $inverse = 1; } 

my @filesInDirs;
foreach my $mdfile ( @files ){
  if( -d $mdfile ){
    opendir(DH, $mdfile) or die "Cannot read directory $mdfile: $!";
    my @files = map { join("/", $mdfile, $_) } grep { ! /^\./ } readdir(DH);
    closedir(DH);
    push(@filesInDirs, @files);
  }
  else{ push(@filesInDirs,$mdfile) };
}

@files = @filesInDirs;

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
  $owlFile = "$GUS_HOME/ontology/release/production/$dataset.owl";
}

my $owl = ApiCommonData::Load::OwlReader->new($owlFile);

my %entities;
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
  printf STDERR ("No top-level category provided (-f option), using all:\n\t%s\n", join("\n\t", map {sprintf("%s\t%s",$_, $filterOptions{$_})} sort keys %filterOptions));
  @filters = keys %filterOptions;
} 

## Use other .owl attributes as keep flags
my ($propertyNames, $propertySubclasses, $propertyOrder, $otherAttrs) = $owl->getLabelsAndParentsHashes($owlFile);

foreach my $filter (@filters){
  die "Entity not found for $filter\nAvailable:\n\t" . join("\n\t", keys %map) . "\n" unless defined $filterOptions{$filter};
  my $filterEntity = $filterOptions{$filter};

  my $itr = $owl->execute('all_subclasses', { ENTITY => $filterEntity });
  #my @keys = $itr->binding_names;
  #printf ("%s\n", join("\t", @keys)) if @keys;
  while (my $row = $itr->next) {
    my $rawEntity = $row->{iri}->as_hash()->{literal};
    my $entity = lc($rawEntity);
    my $keep = 1;
    while(my ($attrName,$keepMatch) = each %$filterOwlAttrHash){
      next unless($otherAttrs->{$rawEntity}->{$attrName});
      # the attribute is set, so we must check it
      unless($otherAttrs->{$rawEntity}->{$attrName} =~ /$keepMatch/i){
        # the attribute does not match, so exclude these columns
        $keep = 0;
      }
    }
    next unless $keep; # if $keep, it means we omit it from output (output is columns to exclude)
      
# Now each column mapped to this entity will be deleted from the list of things to exclude
    unless(defined($row->{col})){
      printf STDERR ("WARNING: no column defined for %s\n", $entity);
      next;
    }
    my $col = pp($row->{col}->as_sparql);
    $entities{$entity} = 1;
    my @cols;
    if($col =~ /,/){
      @cols = split (/\s*,\s*/,$col);
    }
    else {
      @cols = ($col);
    }
    #push(@cols, $entity);
    foreach $col (@cols){
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
    my $baseVar = $var;
    $baseVar =~ s/^.*:://;
    next if $entities{$baseVar};
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
