package ClinEpiData::Load::OntologyDAGNode;
use strict;
use warnings;
use parent 'Tree::DAG_Node';

use Data::Dumper;

sub format_node {
  my ($self, $options, $node) = @_;

  my $name = $node->{name};
  my $displayName = $node->{attributes}->{displayName};
  my $isLeaf = $node->{attributes}->{isLeaf};

  my $altQualifiers = $node->{attributes}->{alternativeQualifiers};

  my $altQualifiersString = join(",", @$altQualifiers) if $altQualifiers;

  if($isLeaf) {
    return $displayName;
  }
  
  if($altQualifiers) {
    $name = "$name $altQualifiersString";
  }

  if($displayName) {
    return "$displayName ($name)";
  }

  return $name;
}


sub node2string {
  my ($self, $options, $node, $vert_dashes) = @_;

  my $keep = $node->{attributes}->{keep};
  unless($keep || $options->{keep_all}) {
    return undef;
  }

  return $self->SUPER::node2string($options, $node, $vert_dashes);
}


sub transformToHashRef {
  my ($self, $force) = @_;

  return unless $self->{attributes}->{keep} || $force;

  my $name = $self->{name};
  my $displayName = $self->{attributes}->{displayName};
  my $order = $self->{attributes}->{order};

  $displayName = $name unless($displayName);

  my $hashref = {id => $name, display => $displayName, order => $order};

  foreach my $daughter (sort { (($a->{attributes}->{order}||99) <=> ($b->{attributes}->{order}||99))||($a->{attributes}->{displayName} cmp $b->{attributes}->{displayName}) } $self->daughters()) {
    my $child = $daughter->transformToHashRef($force);
    push @{$hashref->{children}}, $child if($child);
  }

  return $hashref;
}

sub getNonFilteredAlternativeQualifiers {
  my ($self, $print) = @_;

  return [] if $self->{attributes}->{filter};
  return [] if $self->{attributes}->{isLeaf};
  my $altQualifiers = $self->{attributes}->{alternativeQualifiers};
  if($altQualifiers){
    if($print){
      my $altQualifiersString = join("\n", @$altQualifiers);
      print STDERR "$altQualifiersString\n";
    }
  }

  foreach my $daughter ($self->daughters()) {
    my $more = $daughter->getNonFilteredAlternativeQualifiers();
    push(@$altQualifiers, @$more) if $more;
  }

  return $altQualifiers;
}

sub getNoMatchAttribAlternativeQualifiers {
  my ($self, $filterOwlAttributes, $print) = @_;

  # now scan ONLY filtered (kept)
  # return undef unless $self->{attributes}->{filter};
  return [] if $self->{attributes}->{isLeaf};

  my $keep = 0;
  foreach my $attrName (keys %$filterOwlAttributes){
    $keep++ if($self->{attributes}->{$attrName}); # was matched, keep
  }
  my $altQualifiers = [];
  unless($keep){ # no attributes matched
    $altQualifiers = $self->{attributes}->{alternativeQualifiers};
    if($altQualifiers){
      if($print){
        my $altQualifiersString = join("\n", @$altQualifiers);
        print STDERR "$altQualifiersString\n";
      }
    }
  }

  foreach my $daughter ($self->daughters()) {
    my $more = $daughter->getNoMatchAttribAlternativeQualifiers($filterOwlAttributes);
    push(@$altQualifiers, @$more) if $more;
  }
  return $altQualifiers;
}
1;
