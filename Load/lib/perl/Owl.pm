package ClinEpiData::Load::Owl;
## TODO
## move to another package tree where it belongs (???)
use Digest::MD5;
use RDF::Trine;
use RDF::Query;
use File::Basename;
use Env /PROJECT_HOME SPARQLPATH/;


sub new {
  my ($class, $owlFile) = @_;
  my $self = bless {}, $class;
	$self->{config} = {
		file => $owlFile,
		dbfile => "$owlFile.sqlite",
		md5file => "$owlFile.md5"
	};
	$self->loadOwl();
	unless ($SPARQLPATH){
		$SPARQLPATH = "$PROJECT_HOME/ApiCommonData/Load/ontology/release/SPARQL";
	}
	$self->loadQueries();
	return $self;
}

sub loadQueries{
	my ($self) = @_;
	opendir(DH, $SPARQLPATH) or die "Cannot open directory $SPARQLPATH: $!\n";
	my @files = grep { ! /^\./ } readdir(DH);
	my %queries;
	foreach my $file (@files){
		my $name = basename($file, '.rq');
		printf STDERR ("SPARQL: loading query $name\n");
		open(FH, "<$SPARQLPATH/$file") or die "Cannot read $file: $!\n";
		my @lines = <FH>;
		close(FH);
		my $sparql = join("", @lines);
		$queries{$name} = $sparql;
	}
	$self->{config}->{queries} = \%queries;
}

sub loadOwl {
	my($self) = @_;
	my $dbfile = $self->{config}->{dbfile};
	my $owlFile = $self->{config}->{file};
	my $exists = -e $dbfile;
	my $name = basename($owlFile);
	if($exists && ! $self->fileIsCurrent($owlFile)){
		print STDERR "OWL DB out of date, rebuilding\n";
		$exists = 0;
		unlink($dbfile);
		$self->writeMD5($owlFile);
	}
	else{
		print STDERR "OWL DB is up to date\n";
	}
	my $model = RDF::Trine::Model->new(
	    RDF::Trine::Store::DBI->new(
	        $name,
	        "dbi:SQLite:dbname=$dbfile",
	        '',  # no username
	        '',  # no password
	    ),
	);
	unless( $exists ) { ## assumes existing dbfile is loaded
		my $parser = RDF::Trine::Parser->new('rdfxml');
		$parser->parse_file_into_model(undef, $owlFile, $model);
		print STDERR ("OWL DB ready\n");
	}
	$self->{config}->{model} = $model;
}
	


sub getLabelsAndParentsHashes {
  my ($self, $owlFile) = @_;

	my $it = $self->execute('get_entity_parent_column_label');
  my $propertyNames = {};
  my $propertySubclasses = {};
	while (my $row = $it->next) {
		my $sourceid = $self->getSourceIdFromIRI($row->{entity}->as_hash()->{iri});
		my $parentid = $self->getSourceIdFromIRI($row->{parent}->as_hash()->{iri});
		$parentid =~ s/^.*#(.+)$/$1/; ## handle owl#Thing
		$propertySubclasses->{$parentid} ||= [];
		push(@{$propertySubclasses->{$parentid}}, $sourceid);
		my $col = $row->{column} ? $row->{column}->as_hash()->{literal} : "";
		my $label = $row->{label} ? $row->{label}->as_hash()->{literal} : "";
		$propertyNames->{$sourceid} ||= $label; ## do not overwrite first label, use label that appears first in the OWL
	}
  return($propertyNames, $propertySubclasses);
}

sub execute {
	my ($self, $queryname) = @_;
	die("$queryname cannot be loaded!\n") unless(defined($self->{config}->{queries}->{$queryname}));
	my $sparql = $self->{config}->{queries}->{$queryname};
	my $query = RDF::Query->new($sparql);
	return $query->execute( $self->{config}->{model} );
}


sub getSourceIdFromIRI {
	my($self,$iri) = @_;
	if($iri =~ /^.*:\/\//){ $iri =~ s/\/\///; } # cut protocol://
	my @addr = split(/\//, $iri);
	if(2 > @addr){ return $iri; } # nothing to split
	elsif(3 > @addr) { return pop(@addr); } # pattern is protocol://domain/Thing#what
	else{ 
		## http://domain-name/sub/PREFIX_0000 or
		## http://domain-name/sub/PREFIX/0000
		my ($domain, $sd, @id) = @addr;
		return join("_", @id);
	}
}

sub writeMD5 {
	my ($self, $file) = @_;
	my $md5file = $self->{config}->{md5file};
	my $ctx = Digest::MD5->new;
	open(my $fh, $file);
	$ctx->addfile($fh);
	my $md5 = $ctx->hexdigest();
	close($fh);
	open(FH, ">$md5file") or die "Cannot write $md5file:$!\n";
	print FH "$md5\n";
	close(FH);
}

sub fileIsCurrent {
	my ($self, $file) = @_;
	my $md5file = $self->{config}->{md5file};
	unless (-e $md5file){
		return 0;
	}
	open(FH, "<$md5file") or die "Cannot read $md5file:$!\n";
	my $oldmd5 = <FH>; 
	chomp $oldmd5;
	my $ctx = Digest::MD5->new;
	open(my $fh, $file);
	$ctx->addfile($fh);
	my $md5 = $ctx->hexdigest();
	close($fh);
	if($md5 ne $oldmd5){
		return 0;
	}
	return 1;
}

1;
