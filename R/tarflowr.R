#' Run a parallelized, reproducible workflow using targets.
#'
#' This function serves as a high-level interface to the 'targets' and 'crew'
#' packages. It programmatically generates a `_targets.R` pipeline file based on
#' user-provided inputs, runs the pipeline, and returns the final result. It is
#' designed to abstract the complexity of setting up a 'targets'-based workflow
#' for common "map-reduce" or "split-apply-combine" tasks.
#'
#' @details The function works by creating a self-contained project in the
#'   `project_dir`. It serializes the user's `work_units` and functions
#'   (`process_func`, `combine_func`) into this directory. It then generates a
#'   `_targets.R` script that orchestrates the following steps:
#'
#'   1. Load the `work_units`
#'   2. Map the `process_func` over each element of the `work_units` using
#'   specified crew controller
#'   3. Combine the results of the processing step using `combine_func`
#'   (optional)
#'   4. Execute the pipeline with `targets::tar_make()`
#'   5. Load the final result into original R session
#'
#' @param work_units list or vector. Each element represents a single unit of
#'   work to be processed.
#' @param process_func function. Takes one element of `work_units` as its first
#'   argument and returns the "processed" result.
#' @param combine_func function. Takes a list of all processed results from
#'   `process_func` and combines them into a single, final object. If `NULL`,
#'   the final result will be the list of all processed results.
#' @param project_dir character. Path to the directory where the tarflowr
#'   project will be created. It will be created if it does not exist.
#' @param result_target_name character. Name of the last target in the pipeline,
#'   which contains the result of evaluating `combine_func` on the list of work
#'   units. If `NULL` (default) the final target name is based on the project
#'   directory name, with the suffix `"_result"`.
#' @param packages character. Vector of R package names that are required by the
#'   `process_func` and `combine_func`. These will be passed as the option
#'   `packages` to [targets::tar_option_set()] to be loaded on each worker.
#' @param metadata list. Named list of elements to write to
#'   `_tarflowr_meta.yaml` file on successful run.
#' @param workers integer. Number of local parallel workers to use via the
#'   `crew` package. This is only used when the default `crew_controller` is
#'   `NULL`.
#' @param crew_controller crew_class_controller. Custom `crew` controller.
#'   Default `NULL` uses [crew::crew_controller_local()] with the specified
#'   number of `workers` (parallel processes). [targets::tar_make()] is called
#'   with the R option `"targets.controller` set.
#' @param seed integer. Random number seed. Passed to
#'   [targets::tar_option_set()] via argument `seed`.
#' @param error character. Error behavior. Either `"stop"` (default) or
#'   `"continue"`. Passed to [targets::tar_option_set()] via argument `error`.
#' @param force logical. If `FALSE` (default), the hash of the input arguments
#'   will be checked to determine if the work units or _targets.R file are
#'   updated. If `TRUE` new work units and targets scripts will be written.
#' @param callr_function function. Passed to [targets::tar_make()]. Default
#'   [callr::r()] uses a new R session. Use [callr::r_bg()] to run in background
#'   and suppress targets pipeline output.
#' @param callr_arguments list. Arguments passed to [targets::tar_make()].
#'
#' @return The final combined result of the workflow, as returned by
#'   `combine_func`, or the list of all processed results when `combine_func` is
#'   `NULL`.
#'
#' @author Andrew G. Brown
#' @export
#'
#' @examples
#' \dontrun{
#'
#' td <- file.path(tempdir(), "_my_first_project")
#'
#' # define the work: a list of numbers
#' my_work <- as.list(1:10)
#'
#' # define the processing function to work on one item
#' square_a_number <- function(x) {
#'   Sys.sleep(1)
#'   return(x^2)
#' }
#'
#' # define the combine function for the list of results
#' sum_all_results <- function(results_list) {
#'   sum(unlist(results_list))
#' }
#'
#' # run the workflow
#' final_sum <- tarflowr_run(
#'   work_units = my_work,
#'   process_func = square_a_number,
#'   combine_func = sum_all_results,
#'   project_dir = td,
#'   workers = 4
#' )
#'
#' # final target value is 385
#' print(final_sum)
#'
#' # now inspect "_my_first_project" folder to see the
#' # generated _targets.R file and the _targets/ cache
#'
#' # rerun and get the result instantly from the cache:
#' cached_sum <- tarflowr_run(
#'   work_units = my_work,
#'   process_func = square_a_number,
#'   combine_func = sum_all_results,
#'   project_dir = td,
#'   workers = 4
#' )
#' print(cached_sum)
#'
#' }
tarflowr_run <- function(work_units,
                         process_func,
                         combine_func = NULL,
                         project_dir,
                         result_target_name = NULL,
                         packages = c(),
                         metadata = list(),
                         workers = 1L,
                         crew_controller = NULL,
                         seed = NULL,
                         error = "stop",
                         force = FALSE,
                         callr_function = callr::r, # callr::r_bg,
                         callr_arguments = list(stdout = "|", stderr = "2>&1")) {

    required_pkgs <- c("targets", "crew", "rlang")
    for (pkg in required_pkgs) {
        if (!requireNamespace(pkg, quietly = TRUE)) {
            stop(paste("Package '", pkg, "' is required."), call. = FALSE)
        }
    }

    if (is.null(crew_controller)) {
        crew_controller <- crew::crew_controller_local(workers = workers)
    }

    if (!is.list(work_units) && !is.vector(work_units)) {
        stop("`work_units` must be a list or vector.", call. = FALSE)
    }

    if (!dir.exists(project_dir)) {
        cli::cli_alert_info("Creating project directory: {.path {project_dir}}")
        dir.create(project_dir, recursive = TRUE)
    }
    project_dir <- normalizePath(project_dir)

    if (is.null(result_target_name)) {
        result_target_name <- paste0(make.names(gsub("^[^:alpha:]", "", basename(project_dir))), "_result")
    }

    targets_store_path <- file.path(project_dir, "_targets")
    inputs_dir <- file.path(project_dir, "_tarflowr_inputs")
    dir.create(inputs_dir, showWarnings = FALSE)
    signature_path <- file.path(inputs_dir, "project_signature")

    current_signature <- digest::digest(list(work_units, process_func, combine_func, packages))

    old_signature <- if (file.exists(signature_path)) readLines(signature_path) else ""
    needs_update <- force || (current_signature != old_signature)

    if (needs_update) {
        # generate work_units.rds
        work_units_path <- file.path(inputs_dir, "work_units.rds")
        if (is.null(names(work_units))) {
            names(work_units) <- paste0("unit", as.character(seq_along(work_units)))
        }
        saveRDS(work_units, file = work_units_path)

        # generate user_functions.R
        user_funcs_path <- file.path(inputs_dir, "user_functions.R")
        con <- file(user_funcs_path, "w")

        process_func_name <- "process_func"
        process_func_clean <- process_func
        environment(process_func_clean) <- .GlobalEnv

        cat(process_func_name, "<- ", file = con)
        dput(process_func_clean, file = con)

        combine_func_name <- NULL
        if (!is.null(combine_func)) {
            combine_func_name <- "combine_func"

            combine_func_clean <- combine_func
            environment(combine_func_clean) <- .GlobalEnv

            cat("\n\n", file = con) # Add space between functions
            cat(combine_func_name, "<- ", file = con)
            dput(combine_func_clean, file = con)
        }

        close(con)

        # generate _targets.R
        packages <- packages[nzchar(packages)]

        script_lines <- c(
            "library(targets)",
            "library(crew)",
            # "library(tarchetypes)",
            ifelse(length(packages) > 0, paste0("library(", packages, ", warn.conflicts=FALSE)", collapse = "\n"), ""),
            paste0("source(file.path('", project_dir, "', '_tarflowr_inputs', 'user_functions.R'))"),
            "",
            "tar_option_set(", paste0(
              "  error = '", error, "',"),
            ifelse(is.null(seed), "  # seed = 123,", paste0("  seed = ", seed, ",")),
              "  packages = c(", if (length(packages) > 0)
              paste0("'", paste(packages, collapse = "', '"), "'"), "))",
            "",
            "list("
        )

        script_lines <- c(
            script_lines,
            paste0("  tar_target(user_functions_file, '_tarflowr_inputs/user_functions.R', format = 'file'),"),
            paste0("  tar_target(work_units_file, '_tarflowr_inputs/work_units.rds', format = 'file'),"),
                   "  tar_target(user_functions, source(user_functions_file), cue = tar_cue(mode = 'always')),",
                   "  tar_target(project_work_units, readRDS(work_units_file)),",
                   "  tar_target(work_seq, seq_along(project_work_units)),"
        )

        punit <- paste0("  tar_target(processed_unit, ", process_func_name, "(project_work_units[[work_seq]]), pattern = map(work_seq)),")
        if (!is.null(combine_func_name)) {
            script_lines <- c(script_lines,
                              punit,
                              paste0("  tar_target(", result_target_name, ", ", combine_func_name, "(list(processed_unit)))")
            )
        } else {
            script_lines <- c(script_lines,
                              gsub(
                                  "tar_target(processed_unit, ",
                                  paste0("tar_target(", result_target_name, ", "),
                                  gsub(",$", "", punit),
                                  fixed = TRUE
                              ))
        }

        script_lines <- c(script_lines, ")")

        targets_script_path <- file.path(project_dir, "_targets.R")
        writeLines(paste(script_lines, collapse = "\n"), con = targets_script_path)
    }

    cli::cli_h1("Starting tarflowr workflow")
    cli::cli_li("Project directory: {.path {project_dir}}")
    cli::cli_li(" Number of work units: {length(work_units)}")
    cli::cli_li(" Parallel workers: {workers}")

    is_interactive <- interactive()
    cli::cli_alert_info("Executing {.pkg targets} pipeline...")


    tryCatch({

        withr::with_options(list("targets.controller" = crew_controller),
                            withr::with_dir(project_dir, {
                                targets::tar_make(
                                    callr_function = callr_function,
                                    callr_arguments = c(list(wd = project_dir), callr_arguments)
                                )
                            }))
    }, error = function(e) {
        stop("The 'targets' pipeline failed. Check the logs in '", project_dir, "'.\nOriginal error: ", e$message, call. = FALSE)
    })

    # write metadata on successful run
    .write_tarflowr_metadata(
        project_dir,
        packages_to_log = c(required_pkgs, packages),
        user_meta = c(list(username = Sys.info()[["user"]]),
                      metadata)
    )

    if (!dir.exists(targets_store_path)) {
        targets_store_path <- "_targets"
    }

    targets::tar_read_raw(result_target_name, store = targets_store_path)
}


#' Load a target from a tarflowr project into an environment.
#'
#' A simple wrapper around `targets::tar_load()`. This function loads the value
#' of a target from a completed or running tarflowr pipeline into the specified
#' R environment.
#'
#' @param name The unquoted name of the target to load.
#' @param project_dir The path to the tarflowr project directory containing
#'   _targets/objects folder to load from.
#' @param envir The environment to load the target into. Defaults to the calling
#' environment ([parent.frame()])
#'
#' @return The object `name` is loaded into the specified environment `envir` as
#'   a side-effect.
#' @author Andrew G. Brown
#' @export
#' @examples
#' \dontrun{
#'
#' td <- file.path(tempdir(), "_my_project")
#'
#' res <- tarflowr_run(
#'   work_units = list(a = 1, b = 2),
#'   process_func = function(x) x * 10,
#'   project_dir = td
#' )
#'
#' # load target object into current environment as side-effect
#' tarflowr_load(work_seq, project_dir = td)
#'
#' work_seq
#' }
tarflowr_load <- function(name, project_dir, envir = parent.frame()) {

    store_path <- file.path(normalizePath(project_dir, mustWork = TRUE), "_targets")
    if (!dir.exists(store_path)) {
        stop("Could not find a `_targets` store in '", project_dir, "'.", call. = FALSE)
    }

    tarname <- rlang::as_name(rlang::enquo(name))
    targets::tar_load_raw(
        tarname,
        store = store_path,
        envir = envir
    )
}


#' Read a target from a tarflowr project and return its value.
#'
#' A simple wrapper around `targets::tar_read()`. This function reads the value
#' of a target from a completed or running tarflowr pipeline and returns it as
#' an object. This is generally preferred over `tarflowr_load()` for
#' programmatic access.
#'
#' @param name The unquoted name of the target to read.
#' @param project_dir The path to the tarflowr project directory.
#'
#' @return The value of the target.
#'
#' @author Andrew G. Brown
#' @export
#' @examples
#' \dontrun{
#'
#' td <- file.path(tempdir(), "_my_project")
#'
#' tarflowr_run(
#'   work_units = list(a = 1, b = 2),
#'   process_func = function(x) x * 10,
#'   combine_func = function(x) sum(unlist(x)),
#'   project_dir = td
#' )
#'
#' # inspect the processed work units
#' punits <- tarflowr_read(processed_unit, project_dir = td)
#' punits
#'
#' # inspect the final result
#' res <- tarflowr_read(my_project_result, project_dir = td)
#' res
#' }
tarflowr_read <- function(name, project_dir) {

    store_path <- file.path(normalizePath(project_dir, mustWork = TRUE), "_targets")
    if (!dir.exists(store_path)) {
        stop("Could not find a `_targets` store in '", project_dir, "'.", call. = FALSE)
    }

    tarname <- rlang::as_name(rlang::enquo(name))
    targets::tar_read_raw(tarname, store = store_path)
}

#' Write or update a project's metadata file.
#'
#' This internal helper function manages a `_tarflowr_meta.yaml` file within
#' the project's input directory. It records key information for reproducibility,
#' including package versions and timestamps.
#'
#' @details
#' On the first run within a project, it creates the file and records the
#' `created_datetime`. On subsequent runs, it reads the existing file,
#' preserves the `created_datetime`, and updates the `last_run_datetime` and
#' package versions. This provides a clear audit trail for the project.
#'
#' This function is not exported and is intended to be called from within
#' `tarflowr_run()`.
#'
#' @param project_dir The absolute path to the tarflowr project directory.
#' @param packages_to_log A character vector of R package names whose versions
#'   should be recorded.
#' @param user_meta (Optional) A named list of any custom metadata the user
#'   wishes to include in the file.
#'
#' @return Invisibly returns the path to the metadata file.
#' @noRd
#' @importFrom stats setNames
.write_tarflowr_metadata <- function(project_dir, packages_to_log = c(), user_meta = list()) {

    if (!requireNamespace("yaml", quietly = TRUE)) {
        stop("Package 'yaml' is required to write metadata. Please install it.", call. = FALSE)
    }

    meta_dir <- file.path(project_dir, "_tarflowr_inputs")
    if (!dir.exists(meta_dir)) {
        dir.create(meta_dir, recursive = TRUE, showWarnings = FALSE)
    }
    meta_path <- file.path(meta_dir, "_tarflowr_meta.yaml")

    get_pkg_version <- function(pkg) {
        tryCatch(
            as.character(utils::packageVersion(pkg)),
            error = function(e) "Not Found"
        )
    }

    pkg_versions <- setNames(
        lapply(packages_to_log, get_pkg_version),
        packages_to_log
    )

    current_time_iso <- format(Sys.time(), format = "%Y-%m-%dT%H:%M:%S%z")

    if (file.exists(meta_path)) {
        existing_meta <- yaml::read_yaml(meta_path)

        created_time <- existing_meta$created_datetime %||% current_time_iso

        final_meta <- list(
            project_name = basename(project_dir),
            created_datetime = created_time,
            last_run_datetime = current_time_iso,
            r_version = R.version.string,
            tarflowr_version = get_pkg_version("tarflowr"),
            package_versions = pkg_versions,
            user_metadata = user_meta %||% existing_meta$user_metadata %||% list()
        )

    } else {
        final_meta <- list(
            project_name = basename(project_dir),
            created_datetime = current_time_iso,
            last_run_datetime = current_time_iso,
            r_version = R.version.string,
            tarflowr_version = get_pkg_version("tarflowr"),
            package_versions = pkg_versions,
            user_metadata = user_meta
        )
    }

    yaml::write_yaml(final_meta, file = meta_path)

    return(invisible(meta_path))
}

#' Restore a project's R environment using renv.
#'
#' This function reads the recorded package versions from a tarflowr project's
#' metadata file (`_tarflowr_meta.yaml`) and uses the `renv` package to
#' install those specific versions, recreating the original R environment.
#'
#' @details
#' This function is designed to make tarflowr projects highly reproducible. It
#' provides a simple way for another user (or your future self) to ensure they
#' are running the code with the same dependencies as the original author.
#'
#' It will prompt the user to initialize an `renv` project if one is not
#' already active in the current directory. It also checks if the current R
#' version matches the version recorded in the metadata and will issue a
#' warning if they differ.
#'
#' @param project_dir The path to the tarflowr project directory.
#'
#' @author Andrew G. Brown
#' @export
#' @examples
#' \dontrun{
#' # Assuming a project has already been run in "./_my_project"
#'
#' # This will attempt to install the exact package versions
#' # recorded in the project's metadata file.
#' tarflowr_restore_env(project_dir = "_my_project")
#' }
tarflowr_restore_env <- function(project_dir) {

    required_pkgs <- c("renv", "yaml", "cli")
    for (pkg in required_pkgs) {
        if (!requireNamespace(pkg, quietly = TRUE)) {
            stop(
                "Package '", pkg, "' is required for this function. ",
                "Please install it.",
                call. = FALSE
            )
        }
    }

    meta_path <- file.path(project_dir, "_tarflowr_inputs", "_tarflowr_meta.yaml")

    if (!file.exists(meta_path)) {
        stop(
            "Could not find a `_tarflowr_meta.yaml` file in '", project_dir, "'.\n",
            "Please run the `tarflowr_run()` function first to generate metadata.",
            call. = FALSE
        )
    }

    cli::cli_h1("Restoring tarflowr environment")
    cli::cli_alert_info("Reading metadata from {.path {meta_path}}")

    meta <- yaml::read_yaml(meta_path)

    if (!is.null(meta$r_version) && !grepl(meta$r_version, R.version.string, fixed = TRUE)) {
        cli::cli_alert_warning(
            "R version mismatch: Metadata specifies {.val {meta$r_version}}, but you are running {.val {R.version.string}}."
        )
    }

    pkg_versions <- meta$package_versions

    if (is.null(pkg_versions) || length(pkg_versions) == 0) {
        cli::cli_alert_success("No package versions recorded in metadata. Nothing to do.")
        return(invisible(NULL))
    }

    packages_to_install <- paste0(
        names(pkg_versions), "@", unlist(pkg_versions)
    )

    cli::cli_alert_info("Found {length(packages_to_install)} package(s) to restore:")
    cli::cli_bullets(packages_to_install)

    # Confirm with the user before proceeding
    cli::cli_alert("This will use {.pkg renv} to install these specific package versions.")
    if (!utils::askYesNo("Do you want to continue?", default = TRUE)) {
        cli::cli_alert_danger("Restore operation cancelled by user.")
        return(invisible(NULL))
    }

    cli::cli_alert_info("Starting environment restoration with {.pkg renv}...")

    renv::install(packages = packages_to_install)

    cli::cli_alert_success("Environment restoration complete.")
}
