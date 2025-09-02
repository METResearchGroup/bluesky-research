# Load Packages Script
# Run this first to ensure all packages are properly loaded

cat("=== Loading R Packages ===\n")

# Set CRAN mirror
options(repos = c(CRAN = "https://cran.rstudio.com/"))

# Install CMake if not present
if (!system("which cmake", ignore.stdout = TRUE) == 0) {
  cat("Installing CMake...\n")
  system("brew install cmake")
}

# Install and load packages
packages <- c("tidyverse", "lmerTest", "broom.mixed", "knitr", "ggpubr", "emmeans")

for (pkg in packages) {
  cat(paste("Processing", pkg, "...\n"))
  
  # Install if not present
  if (!require(pkg, character.only = TRUE, quietly = TRUE)) {
    cat(paste("Installing", pkg, "...\n"))
    install.packages(pkg, dependencies = TRUE, type = "source")
  }
  
  # Load the package
  library(pkg, character.only = TRUE)
  cat(paste("✓", pkg, "loaded\n"))
}

# Verify key functions
cat("\n=== Verifying Functions ===\n")
key_functions <- c("read_csv", "%>%", "ggplot", "ggsave", "mutate", "filter", "sym")

for (func in key_functions) {
  if (exists(func)) {
    cat(paste("✓", func, "available\n"))
  } else {
    cat(paste("✗", func, "NOT available\n"))
  }
}

cat("\n=== Package Loading Complete ===\n")
cat("You can now run your analysis script.\n")
