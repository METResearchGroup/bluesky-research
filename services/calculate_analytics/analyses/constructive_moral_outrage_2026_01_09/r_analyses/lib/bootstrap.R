get_script_dir <- function() {
  # Prefer RStudio when available, otherwise fall back to --file= invocation, else getwd()
  if (requireNamespace("rstudioapi", quietly = TRUE) && rstudioapi::isAvailable()) {
    p <- rstudioapi::getSourceEditorContext()$path
    if (!is.null(p) && nzchar(p)) return(dirname(p))
  }

  cmd_args <- commandArgs(trailingOnly = FALSE)
  file_arg <- grep("^--file=", cmd_args, value = TRUE)
  if (length(file_arg) > 0) {
    return(dirname(normalizePath(sub("^--file=", "", file_arg[1]))))
  }

  getwd()
}

set_working_dir_to_script <- function() {
  setwd(get_script_dir())
  invisible(getwd())
}

require_packages <- function(pkgs) {
  stopifnot(is.character(pkgs), length(pkgs) > 0)
  missing <- pkgs[!vapply(pkgs, requireNamespace, logical(1), quietly = TRUE)]
  if (length(missing) > 0) {
    stop(
      paste0(
        "Missing required R packages: ",
        paste(missing, collapse = ", "),
        "\nInstall them, then re-run."
      ),
      call. = FALSE
    )
  }
  invisible(TRUE)
}


