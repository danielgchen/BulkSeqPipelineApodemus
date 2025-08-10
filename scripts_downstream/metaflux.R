library(METAFlux)

# derive arguments
args <- commandArgs(trailingOnly = TRUE)
input_file <- args[1]
medium <- args[2]
output_file <- args[3]

# data is genes by cells, normalized counts
bulk_test_example <- read.csv(input_file, row.names=1)
# medium file for human derived samples
data(list=medium)

# calculate mras for human sample data
scores <- calculate_reaction_score(bulk_test_example)
# calculate flux for human sample data
flux <- compute_flux(mras=scores, medium=get(medium)) 
# flux scores cubic root normalization
cbrt <- function(x) {
  sign(x) * abs(x)^(1/3)
}
flux <- cbrt(flux)
# write the flux
write.csv(flux, output_file)