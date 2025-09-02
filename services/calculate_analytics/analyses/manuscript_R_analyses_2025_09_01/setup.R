# Setup script for Manuscript R Analyses
# This script installs and verifies all required R packages

cat("=== R Package Setup for Manuscript Analyses ===\n")
cat("Installing required packages...\n")

# Set CRAN mirror to avoid "trying to use CRAN without setting a mirror" error
options(repos = c(CRAN = "https://cran.rstudio.com/"))

# List of required packages in dependency order
required_packages <- c("tidyverse", "lmerTest", "broom.mixed", "knitr", "ggpubr", "emmeans")

# Function to install packages with better error handling
install_if_missing <- function(package) {
  if (!require(package, character.only = TRUE)) {
    cat(paste("Installing", package, "...\n"))
    tryCatch({
      install.packages(package, dependencies = TRUE, repos = "https://cran.rstudio.com/", 
                      type = "binary", INSTALL_opts = "--no-lock")
    }, error = function(e) {
      cat(paste("Error installing", package, ":", e$message, "\n"))
      cat(paste("Trying to install", package, "from source...\n"))
      tryCatch({
        install.packages(package, dependencies = TRUE, repos = "https://cran.rstudio.com/", 
                        type = "source", INSTALL_opts = "--no-lock")
      }, error = function(e2) {
        cat(paste("Failed to install", package, ":", e2$message, "\n"))
        return(FALSE)
      })
    })
  } else {
    cat(paste(package, "is already installed.\n"))
  }
}

# Install packages
for (pkg in required_packages) {
  install_if_missing(pkg)
}

cat("\n=== Verifying Package Installation ===\n")

# Load and verify each package
packages_loaded <- c()
for (pkg in required_packages) {
  tryCatch({
    library(pkg, character.only = TRUE)
    cat(paste("✓", pkg, "loaded successfully\n"))
    packages_loaded <- c(packages_loaded, pkg)
  }, error = function(e) {
    cat(paste("✗ Failed to load", pkg, ":", e$message, "\n"))
  })
}

# Summary
cat("\n=== Setup Summary ===\n")
cat(paste("Successfully loaded", length(packages_loaded), "out of", length(required_packages), "packages\n"))

if (length(packages_loaded) == length(required_packages)) {
  cat("✓ All packages are ready! You can now run feed_analyses_updated.R\n")
} else {
  cat("✗ Some packages failed to load. Please check the error messages above.\n")
  cat("You may need to install packages manually or check your R installation.\n")
}

cat("\nTo run the analysis, use:\n")
cat("  source('feed_analyses_updated.R')\n")
cat("  or\n")
cat("  Rscript feed_analyses_updated.R\n")