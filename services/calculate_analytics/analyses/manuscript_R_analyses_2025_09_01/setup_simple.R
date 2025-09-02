# Simple Setup script for Manuscript R Analyses
# This script installs packages one by one with better error handling

cat("=== Simple R Package Setup ===\n")

# Set CRAN mirror
options(repos = c(CRAN = "https://cran.rstudio.com/"))

# Install packages one by one in dependency order
packages_to_install <- c(
  "tidyverse",
  "lmerTest", 
  "broom.mixed",
  "knitr",
  "ggpubr",
  "emmeans"
)

for (pkg in packages_to_install) {
  cat(paste("Installing", pkg, "...\n"))
  tryCatch({
    install.packages(pkg, dependencies = TRUE, type = "binary")
    cat(paste("✓", pkg, "installed successfully\n"))
  }, error = function(e) {
    cat(paste("✗ Error installing", pkg, ":", e$message, "\n"))
    cat(paste("Trying source installation for", pkg, "...\n"))
    tryCatch({
      install.packages(pkg, dependencies = TRUE, type = "source")
      cat(paste("✓", pkg, "installed from source\n"))
    }, error = function(e2) {
      cat(paste("✗ Failed to install", pkg, ":", e2$message, "\n"))
    })
  })
}

# Verify installation
cat("\n=== Verifying Installation ===\n")
for (pkg in packages_to_install) {
  if (require(pkg, character.only = TRUE, quietly = TRUE)) {
    cat(paste("✓", pkg, "loaded successfully\n"))
  } else {
    cat(paste("✗", pkg, "failed to load\n"))
  }
}

cat("\nSetup complete!\n")
