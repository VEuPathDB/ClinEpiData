#!/usr/bin/env Rscript
args = commandArgs(trailingOnly=TRUE);

if(length(args) < 4) {
  stop("usage:  getRecordAttributes.R inputFile outputFile isCommaDelimited primaryKey.1 [primaryKey.n].n", call.=FALSE)
} 

inputFile = args[1];
outputFile = args[2];
isCommaDelimited = args[3];
primaryKeyColumns = args[4:length(args)];

lengthUnique = function(x) {return(length(unique(x)))}

del = "\t";
if(isCommaDelimited == 1) {
  del = ",";
}

data = read.table(inputFile, sep=del, header=TRUE);

l = list();
for(i in 1:length(primaryKeyColumns)) {
 index = which(colnames(data) == primaryKeyColumns[i]);
  l[[i]] = data[, index];
}


output=NULL;
outputColumnNames = vector();
for(i in 1:ncol(data)) {
  res = aggregate(data[, i], l, lengthUnique);
  if(sum(res$x != 1) == 0) {
    output = cbind(output, as.vector(data[, i]));
    outputColumnNames[length(outputColumnNames)+1] = colnames(data)[i];
  }
}


write.table(unique(output), outputFile, row.names=FALSE, col.names=outputColumnNames, quote=TRUE, sep=del);


